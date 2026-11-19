#!/usr/bin/env python3
"""T13：同一进程触发两次 READY，只有一个发送循环；旧进程遗留记录
只在启动时恢复一次；只有出站确实 claimed→sent，才在同一事务里把
入站标成 done。

场景（全假消息、临时库、离线）：
- 库里预置：旧进程遗留的 claimed 行（FAKE-OLD-001，入站 queued），
  以及一条新入队待发消息（FAKE-NEW-001）。
- fake API 第一发挂起（等放行事件），模拟发送在途。
- 同一进程内 await bridge.on_ready() 两次（模拟网关重连又 READY）。
- 断言：只有一个 send_loop 任务；遗留行只恢复一次（needs-check.log
  只有旧行的一行告警）；在途的新行仍是 claimed，未被第二次 READY
  的 reconcile 误伤。
- 放行回执：fake 返回成功 id。断言：fake 只调 1 次；新行 sent、
  入站 done；mark_sent 对 needs_check 行返回 False 且不碰入站；
  mark_sent 对 claimed 行返回 True 且同一事务内出站 sent、入站 done。
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
    """模拟 BotAPI：第一发挂起等放行，放行后返回成功回执。"""

    def __init__(self):
        self.calls = []  # (msg_id, msg_seq) 实际发出的调用
        self.started = asyncio.Event()   # 已进入发送（挂起中）
        self.release = asyncio.Event()   # 放行回执

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        self.calls.append((msg_id, msg_seq))
        self.started.set()
        await self.release.wait()
        return {"id": "FAKE-RECEIPT-1"}

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


async def wait_until(predicate, timeout=10.0, step=0.05):
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
    tmp = tempfile.mkdtemp(prefix="qq-t13-")
    hidden = os.path.join(tmp, "hidden_files")
    db = os.path.join(hidden, "bridge.db")
    os.environ["QQBOT_DB_PATH"] = db
    mod = load_daemon(tmp)

    conn = store.connect(db)
    try:
        # 预置：旧进程遗留的 claimed 行（入站 queued，出站 claimed seq=1）
        assert store.record_inbound(conn, "FAKE-OLD-001", "c2c", "fake-openid-old", "旧消息")
        status, _ = store.enqueue_reply(conn, "FAKE-OLD-001", "c2c", "fake-openid-old", "旧回复")
        assert status == "enqueued", status
        claimed = store.claim_next(conn)
        assert claimed is not None and claimed["msg_id"] == "FAKE-OLD-001"
        assert claimed["seq"] == 1
        # 预置：一条新入队待发消息（入站 queued，出站 pending）
        assert store.record_inbound(conn, "FAKE-NEW-001", "c2c", "fake-openid-new", "新消息")
        status, _ = store.enqueue_reply(conn, "FAKE-NEW-001", "c2c", "fake-openid-new", "新回复")
        assert status == "enqueued", status

        fake = FakeApi()
        bridge = mod.Bridge(
            intents=botpy.Intents(public_messages=True),
            is_sandbox=True, ext_handlers=False)
        bridge.api = fake

        await bridge.on_ready()  # 第一次 READY：启动发送循环
        # 等第一发进入发送（挂起中）：循环先 reconcile，再等 5 秒才认领
        await asyncio.wait_for(fake.started.wait(), timeout=10)
        await bridge.on_ready()  # 第二次 READY（同一进程，模拟网关重连）
        await asyncio.sleep(0.5)  # 让第二个循环（若有）有机会跑 reconcile

        # A. 只有一个发送循环
        loops = send_loop_tasks()
        assert len(loops) == 1, f"send_loop 任务数={len(loops)}，应为 1"
        print("PASS T13-A 两次 READY 后只有一个 send_loop 任务")

        # B. 旧进程遗留记录只在启动时恢复一次
        alert_path = os.path.join(hidden, "needs-check.log")
        lines = open(alert_path, encoding="utf-8").read().splitlines() if os.path.exists(alert_path) else []
        old_lines = [ln for ln in lines if "FAKE-OLD-001" in ln]
        new_lines = [ln for ln in lines if "FAKE-NEW-001" in ln]
        assert len(old_lines) == 1, f"旧行告警 {len(old_lines)} 行，应为 1"
        assert not new_lines, f"新行不应有告警，实际 {new_lines}"
        assert store.get_outbox(conn, "FAKE-OLD-001")["status"] == "needs_check"
        print("PASS T13-B 遗留 claimed 行只恢复一次（needs_check 告警 1 行）")

        # C. 在途的新行仍是 claimed，未被第二次 READY 的 reconcile 误伤
        assert store.get_outbox(conn, "FAKE-NEW-001")["status"] == "claimed"
        print("PASS T13-C 在途发送仍是 claimed，未被第二次 READY 误恢复")

        # 放行成功回执
        fake.release.set()
        await wait_until(
            lambda: store.get_outbox(conn, "FAKE-NEW-001")["status"] == "sent")

        # D. 只发一次；出站 sent、入站 done
        assert fake.calls == [("FAKE-NEW-001", 1)], fake.calls
        assert store.get_outbox(conn, "FAKE-NEW-001")["status"] == "sent"
        assert store.get_inbound(conn, "FAKE-NEW-001")["status"] == "done"
        print("PASS T13-D 放行后只发 1 次，出站 sent、入站 done")

        # E. mark_sent 对 needs_check 行：不转移、不碰入站，返回 False
        old_row = store.get_outbox(conn, "FAKE-OLD-001")
        moved = store.mark_sent(conn, old_row["id"], "FAKE-OLD-001")
        assert moved is False, moved
        assert store.get_outbox(conn, "FAKE-OLD-001")["status"] == "needs_check"
        assert store.get_inbound(conn, "FAKE-OLD-001")["status"] == "queued"
        print("PASS T13-E mark_sent 对 needs_check 行返回 False，入站仍 queued")

        # F. mark_sent 对 claimed 行：转移并返回 True，同一事务内出站 sent、入站 done
        assert store.record_inbound(conn, "FAKE-NEW-002", "c2c", "fake-openid-2", "又一条")
        status, _ = store.enqueue_reply(conn, "FAKE-NEW-002", "c2c", "fake-openid-2", "回")
        assert status == "enqueued", status
        row2 = store.claim_next(conn)
        assert row2 is not None and row2["msg_id"] == "FAKE-NEW-002"
        moved = store.mark_sent(conn, row2["id"], "FAKE-NEW-002")
        assert moved is True, moved
        assert store.get_outbox(conn, "FAKE-NEW-002")["status"] == "sent"
        assert store.get_inbound(conn, "FAKE-NEW-002")["status"] == "done"
        print("PASS T13-F mark_sent 对 claimed 行返回 True，出站 sent、入站 done")

        print("结果：T13 通过（单发送循环、启动恢复一次、claimed→sent 才标 done）")
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
