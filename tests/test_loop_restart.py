#!/usr/bin/env python3
"""T14：发送任务认领一条回复后异常退出，进程不重启，随后又收到 READY。

场景（全假消息、临时库、离线）：
- 种子消息 FAKE-T-001 入队待发；fake API 第一发挂起，模拟发送在途。
- 取消发送循环任务（模拟任务异常退出：CancelledError 穿过 _send_one，
  行保持 claimed，进程不重启）。
- 同一进程内再 await bridge.on_ready() 一次。
- 预期：发送循环能恢复（新任务在跑，且只有一个）；遗留的 claimed 行
  被恢复为 needs_check 并告警 1 次，不自动重发，入站仍 queued（不标 done）。
- 再入队 FAKE-T-002：重启后的循环能正常发出（sent、入站 done），
  证明循环恢复后仍能工作；FAKE-T-001 仍是 needs_check，不被重发。
"""
import asyncio
import importlib.util
import os
import shutil
import sys
import tempfile

import botpy

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
import store  # noqa: E402


def load_daemon(workdir):
    """把 daemon.py + store.py 拷进临时目录，从副本加载（不碰原文件）。"""
    os.makedirs(os.path.join(workdir, "hidden_files"), exist_ok=True)
    for name in ("store.py", "daemon.py"):
        shutil.copyfile(os.path.join(ROOT, name),
                        os.path.join(workdir, name))
    sys.path.insert(0, workdir)
    spec = importlib.util.spec_from_file_location(
        "daemon_under_test", os.path.join(workdir, "daemon.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["daemon_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeApi:
    """模拟 BotAPI：第一发挂起（等测试取消任务），之后的调用立即成功。"""

    def __init__(self):
        self.calls = []  # (msg_id, msg_seq) 实际发出的调用
        self.started = asyncio.Event()  # 第一发已进入发送（挂起中）

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        self.calls.append((msg_id, msg_seq))
        if len(self.calls) == 1:
            self.started.set()
            await asyncio.Event().wait()  # 永远挂起，等测试取消任务
        return {"id": f"FAKE-RECEIPT-{len(self.calls)}"}

    async def post_group_message(self, group_openid, content, msg_type=0,
                                 msg_id=None, msg_seq=None):
        return await self.post_c2c_message(
            group_openid, content, msg_type, msg_id, msg_seq)


def send_loop_tasks():
    """当前事件循环里正在跑的 send_loop 任务（按协程名识别）。"""
    found = []
    for task in asyncio.all_tasks():
        coro = task.get_coro()
        frame = getattr(coro, "cr_frame", None)
        if frame is not None and frame.f_code.co_name == "send_loop":
            found.append(task)
    return found


async def wait_until(predicate, timeout=12.0, step=0.05):
    """轮询等 predicate() 为真，超时抛 AssertionError。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if predicate():
            return
        if loop.time() >= deadline:
            raise AssertionError(f"等待超时（{timeout}s）：{predicate}")
        await asyncio.sleep(step)


async def main():
    tmp = tempfile.mkdtemp(prefix="qq-t14-")
    hidden = os.path.join(tmp, "hidden_files")
    db = os.path.join(hidden, "bridge.db")
    os.environ["QQBOT_DB_PATH"] = db
    mod = load_daemon(tmp)

    conn = store.connect(db)
    try:
        assert store.record_inbound(conn, "FAKE-T-001", "c2c", "fake-openid-1", "消息一")
        status, _ = store.enqueue_reply(conn, "FAKE-T-001", "c2c", "fake-openid-1", "回复一")
        assert status == "enqueued", status

        fake = FakeApi()
        bridge = mod.Bridge(
            intents=botpy.Intents(public_messages=True),
            is_sandbox=True, ext_handlers=False)
        bridge.api = fake

        await bridge.on_ready()  # 第一次 READY：启动发送循环
        # 等第一发进入发送（挂起中）：循环先 reconcile，再等 5 秒才认领
        await asyncio.wait_for(fake.started.wait(), timeout=10)
        assert store.get_outbox(conn, "FAKE-T-001")["status"] == "claimed"

        # 模拟发送任务异常退出：取消循环任务（CancelledError 穿过 _send_one）
        (old_loop,) = send_loop_tasks()
        old_loop.cancel()
        try:
            await old_loop
        except asyncio.CancelledError:
            pass
        assert old_loop.done() and old_loop.cancelled()
        print("PASS T14-A 发送任务异常退出：循环任务已取消，进程未重启")
        # 退出时行保持 claimed，尚未处理
        assert store.get_outbox(conn, "FAKE-T-001")["status"] == "claimed"
        assert store.get_inbound(conn, "FAKE-T-001")["status"] == "queued"

        # 同一进程内再收到 READY（不重启进程）
        await bridge.on_ready()
        await asyncio.sleep(0.5)  # 让重启的循环跑完开头的 reconcile

        # B. 发送循环能恢复，且只有一个
        loops = send_loop_tasks()
        assert len(loops) == 1, f"send_loop 任务数={len(loops)}，应为 1"
        assert loops[0] is not old_loop
        print("PASS T14-B 再 READY 后发送循环恢复，且只有一个新任务")

        # C. 遗留的 claimed 行被恢复为 needs_check 并告警，不重发、不标 done
        assert store.get_outbox(conn, "FAKE-T-001")["status"] == "needs_check"
        assert store.get_inbound(conn, "FAKE-T-001")["status"] == "queued"
        alert_path = os.path.join(hidden, "needs-check.log")
        lines = open(alert_path, encoding="utf-8").read().splitlines() if os.path.exists(alert_path) else []
        t1_lines = [ln for ln in lines if "FAKE-T-001" in ln]
        assert len(t1_lines) == 1, f"FAKE-T-001 告警 {len(t1_lines)} 行，应为 1"
        assert [c for c in fake.calls if c[0] == "FAKE-T-001"] == [("FAKE-T-001", 1)]
        print("PASS T14-C 遗留 claimed 行转 needs_check + 告警 1 次，不重发，入站仍 queued")

        # D. 重启后的循环能正常发出新消息
        assert store.record_inbound(conn, "FAKE-T-002", "c2c", "fake-openid-2", "消息二")
        status, _ = store.enqueue_reply(conn, "FAKE-T-002", "c2c", "fake-openid-2", "回复二")
        assert status == "enqueued", status
        await wait_until(
            lambda: store.get_outbox(conn, "FAKE-T-002")["status"] == "sent")
        assert fake.calls == [("FAKE-T-001", 1), ("FAKE-T-002", 1)], fake.calls
        assert store.get_outbox(conn, "FAKE-T-002")["status"] == "sent"
        assert store.get_inbound(conn, "FAKE-T-002")["status"] == "done"
        assert store.get_outbox(conn, "FAKE-T-001")["status"] == "needs_check"
        print("PASS T14-D 重启后的循环正常发出新消息；旧行仍是 needs_check，未被重发")

        print("结果：T14 通过（任务异常退出后 READY 可恢复循环，遗留 claimed 转 needs_check）")
    finally:
        for task in send_loop_tasks():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            bridge.db.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
