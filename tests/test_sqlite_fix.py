#!/usr/bin/env python3
"""QQ 桥 SQLite 修复验收测试（假消息，不碰生产、不连网关）。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_sqlite_fix.py

覆盖用户验收清单：
  T1 A 发送挂起时入队 B：B 保留，随后恰好发一次
  T2 同一入站消息被两个 worker 同时读到：只产生一条待发记录
  T3a 落库事务提交前退出：不漏处理（原消息仍 pending，可重新入队）
  T3b 落库事务提交后重复运行：不重复产生待发记录
  T4a 腾讯已收到、本地没记下回执（botpy 返回 None）：重启后不自动重发
  T4b 腾讯明确拒绝：标记 failed，重启后不重发
  T5 真实发送中断：模拟服务已收到，本地写 sent 前杀掉 daemon 进程；
      库里留 claimed，重启后转 needs_check 并告警，不再发，原 seq 不变
  T6 成功回执判定：只有含非空 id 的 dict 才算成功
"""
import asyncio
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FIXDIR = os.path.dirname(HERE)
# 子进程用运行本测试的同一个 Python 解释器（一般就是 venv 里的 python）。


def load_daemon(workdir):
    """把 daemon.py + store.py 拷进临时目录，从副本加载（不碰原文件）。"""
    os.makedirs(os.path.join(workdir, "hidden_files"), exist_ok=True)
    for name in ("store.py", "daemon.py"):
        shutil.copyfile(os.path.join(FIXDIR, name),
                        os.path.join(workdir, name))
    sys.path.insert(0, workdir)
    spec = importlib.util.spec_from_file_location(
        "daemon_under_test", os.path.join(workdir, "daemon.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["daemon_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeApi:
    """模拟 BotAPI。mode：ok | hang（挂起等放行）| none（botpy 超时行为：
    返回 None，不抛异常）| reject（腾讯明确拒绝）| raise-timeout（抛超时）
    | canned（返回 self.result 原样）。"""

    def __init__(self, mode="ok", result=None):
        self.mode = mode
        self.result = result
        self.calls = []            # (msg_id, msg_seq) 实际发出的调用
        self.server_received = []  # 模拟“腾讯侧已收到”
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        return await self._send(msg_id, msg_seq)

    async def post_group_message(self, group_openid, content, msg_type=0,
                                 msg_id=None, msg_seq=None):
        return await self._send(msg_id, msg_seq)

    async def _send(self, msg_id, msg_seq):
        self.calls.append((msg_id, msg_seq))
        self.started.set()
        if self.mode == "hang":
            await self.release.wait()
            return {"id": "fake"}
        if self.mode == "none":
            self.server_received.append(msg_id)
            return None
        if self.mode == "reject":
            from botpy.errors import ForbiddenError
            raise ForbiddenError("fake: forbidden by server")
        if self.mode == "raise-timeout":
            self.server_received.append(msg_id)
            raise asyncio.TimeoutError("fake timeout")
        if self.mode == "canned":
            return self.result
        return {"id": "fake"}


async def wait_until(pred, timeout=20, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


def make_bridge(mod, db_path, api):
    bridge = object.__new__(mod.Bridge)
    bridge.db = mod.store.connect(db_path)
    bridge.api = api
    return bridge


async def stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_t1(mod, workdir):
    """A 发送挂起时入队 B：B 保留，随后恰好发一次。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-A-001", "c2c", "fake-openid", "假消息 A")
    store.record_inbound(conn, "FAKE-B-002", "c2c", "fake-openid", "假消息 B")
    assert store.enqueue_reply(conn, "FAKE-A-001", "c2c", "fake-openid",
                               "回复 A")[0] == "enqueued"
    bridge = make_bridge(mod, db, FakeApi(mode="hang"))
    task = asyncio.create_task(bridge.send_loop())
    await asyncio.wait_for(bridge.api.started.wait(), timeout=15)
    # A 发送中：另一个连接（模拟 worker 进程）入队 B
    conn2 = store.connect(db)
    assert store.enqueue_reply(conn2, "FAKE-B-002", "c2c", "fake-openid",
                               "回复 B")[0] == "enqueued"
    row_b = store.get_outbox(bridge.db, "FAKE-B-002")
    assert row_b is not None and row_b["status"] == "pending", \
        f"B must be pending while A is in flight: {row_b}"
    bridge.api.release.set()
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-A-001")["status"] == "sent"
        and store.get_outbox(bridge.db, "FAKE-B-002")["status"] == "sent",
        timeout=20)
    await stop(task)
    a = [c for c in bridge.api.calls if c[0] == "FAKE-A-001"]
    b = [c for c in bridge.api.calls if c[0] == "FAKE-B-002"]
    assert ok and len(a) == 1 and len(b) == 1, f"A={a} B={b}"
    return "A 发 1 次，B 发 1 次；B 在 A 发送期间入队未被覆盖"


async def test_t2(mod, workdir):
    """同一入站消息被两个 worker 同时读到：只产生一条待发记录。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-M-001", "c2c", "fake-openid", "假消息 M")
    barrier = threading.Barrier(2)
    results = []

    def worker():
        try:
            c = store.connect(db)
            barrier.wait(timeout=10)
            results.append(store.enqueue_reply(
                c, "FAKE-M-001", "c2c", "fake-openid", "回复 M")[0])
            c.close()
        except Exception as exc:  # noqa: BLE001
            results.append(f"error:{exc}")

    t1, t2 = threading.Thread(target=worker), threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(15)
    t2.join(15)
    assert sorted(results) == ["duplicate", "enqueued"], f"results={results}"
    assert store.get_outbox(conn, "FAKE-M-001") is not None
    assert store.get_inbound(conn, "FAKE-M-001")["status"] == "queued"
    return "两个 worker 一个 enqueued、一个 duplicate；出站表只有 1 行"


async def test_t3a(mod, workdir):
    """落库事务提交前退出：不漏处理（原消息仍 pending，可重新入队）。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-K-001", "c2c", "fake-openid", "假消息 K")
    child = os.path.join(HERE, "crash_child.py")
    proc = subprocess.Popen(
        [sys.executable, child, db, "FAKE-K-001"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(2)  # 子进程已在事务内睡眠（提交前）
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)
    conn2 = store.connect(db)
    assert store.get_outbox(conn2, "FAKE-K-001") is None, \
        "uncommitted outbox row must not be visible"
    assert store.get_inbound(conn2, "FAKE-K-001")["status"] == "pending", \
        "inbound must stay pending after crash before commit"
    assert store.enqueue_reply(conn2, "FAKE-K-001", "c2c", "fake-openid",
                               "回复 K")[0] == "enqueued"
    return "提交前被 SIGKILL：无出站行、原消息仍 pending，重新入队成功"


async def test_t3b(mod, workdir):
    """落库事务提交后重复运行：不重复产生待发记录。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-K-002", "c2c", "fake-openid", "假消息 K2")
    assert store.enqueue_reply(conn, "FAKE-K-002", "c2c", "fake-openid",
                               "回复 K2")[0] == "enqueued"
    assert store.enqueue_reply(conn, "FAKE-K-002", "c2c", "fake-openid",
                               "回复 K2")[0] == "duplicate"
    assert store.get_outbox(conn, "FAKE-K-002") is not None
    assert store.get_inbound(conn, "FAKE-K-002")["status"] == "queued"
    return "提交后重复入队返回 duplicate；出站表仍只有 1 行"


async def test_t4a(mod, workdir):
    """腾讯已收到、本地没记下回执（botpy 返回 None）：重启后不自动重发。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-N-001", "c2c", "fake-openid", "假消息 N")
    store.enqueue_reply(conn, "FAKE-N-001", "c2c", "fake-openid", "回复 N")
    bridge = make_bridge(mod, db, FakeApi(mode="none"))
    task = asyncio.create_task(bridge.send_loop())
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-N-001")["status"] == "needs_check",
        timeout=20)
    await stop(task)
    row = store.get_outbox(bridge.db, "FAKE-N-001")
    assert ok and row["status"] == "needs_check", f"row={row}"
    assert bridge.api.server_received == ["FAKE-N-001"], "server got it once"
    assert len(bridge.api.calls) == 1
    seq_before = row["seq"]
    assert seq_before is not None
    alert = open(os.path.join(workdir, "hidden_files", "needs-check.log"),
                 encoding="utf-8").read()
    assert "FAKE-N-001" in alert, "needs-check.log must record the alert"
    # 重启：新循环 + 会成功的接口，必须 0 次调用
    bridge2 = make_bridge(mod, db, FakeApi(mode="ok"))
    task2 = asyncio.create_task(bridge2.send_loop())
    await asyncio.sleep(7)
    await stop(task2)
    assert bridge2.api.calls == [], f"must not auto-resend: {bridge2.api.calls}"
    row2 = store.get_outbox(bridge.db, "FAKE-N-001")
    assert row2["status"] == "needs_check" and row2["seq"] == seq_before, \
        f"row must stay needs_check with same seq: {row2}"
    return "回执丢失→needs_check+告警；重启后 0 次重发，seq 未变"


async def test_t4b(mod, workdir):
    """腾讯明确拒绝：标记 failed，重启后不重发。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-N-002", "c2c", "fake-openid", "假消息 N2")
    store.enqueue_reply(conn, "FAKE-N-002", "c2c", "fake-openid", "回复 N2")
    bridge = make_bridge(mod, db, FakeApi(mode="reject"))
    task = asyncio.create_task(bridge.send_loop())
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-N-002")["status"] == "failed",
        timeout=20)
    await stop(task)
    row = store.get_outbox(bridge.db, "FAKE-N-002")
    assert ok and row["status"] == "failed", f"row={row}"
    assert "ForbiddenError" in (row["last_error"] or ""), f"error={row['last_error']}"
    bridge2 = make_bridge(mod, db, FakeApi(mode="ok"))
    task2 = asyncio.create_task(bridge2.send_loop())
    await asyncio.sleep(7)
    await stop(task2)
    assert bridge2.api.calls == [], f"must not resend failed: {bridge2.api.calls}"
    assert store.get_outbox(bridge.db, "FAKE-N-002")["status"] == "failed"
    return "明确拒绝→failed+错误原文；重启后 0 次重发"


async def test_t5(mod, workdir):
    """真实发送中断：模拟服务已收到 A，本地写 sent 前杀掉 daemon。
    库里留 claimed，重启后转 needs_check 并告警，不再发 A，原 seq 不变。"""
    import signal as _signal
    import subprocess as _subprocess
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    hidden = os.path.join(workdir, "hidden_files")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-X-001", "c2c", "fake-openid", "假消息 X")
    store.enqueue_reply(conn, "FAKE-X-001", "c2c", "fake-openid", "回复 X")
    conn.close()
    child = os.path.join(HERE, "kill_child.py")
    marker = os.path.join(hidden, "started.marker")
    calls_path = os.path.join(hidden, "calls.jsonl")
    # 第一程：发送挂起（腾讯侧已收到）后 SIGKILL
    proc = _subprocess.Popen(
        [sys.executable, child, workdir, "hang"],
        stdout=_subprocess.PIPE, stderr=_subprocess.PIPE, text=True)
    ok = await wait_until(lambda: os.path.exists(marker), timeout=20)
    if not ok:
        _, err = proc.communicate(timeout=5)
        raise AssertionError(f"send never started; child stderr: {err}")
    proc.send_signal(_signal.SIGKILL)
    proc.wait(timeout=10)
    conn2 = store.connect(db)
    row = store.get_outbox(conn2, "FAKE-X-001")
    assert row is not None and row["status"] == "claimed", f"row={row}"
    assert row["seq"] == 1, f"seq={row['seq']}"
    calls = [json.loads(l) for l in open(calls_path, encoding="utf-8")
             if l.strip()]
    assert len(calls) == 1 and calls[0]["msg_id"] == "FAKE-X-001", \
        f"server must have received exactly once: {calls}"
    conn2.close()
    # 第二程：重启（会成功的接口），必须 0 次重发
    os.remove(marker)
    proc2 = _subprocess.Popen(
        [sys.executable, child, workdir, "ok"],
        stdout=_subprocess.PIPE, stderr=_subprocess.PIPE, text=True)
    await asyncio.sleep(8)  # 启动 reconcile + 一轮 5 秒睡眠
    proc2.send_signal(_signal.SIGKILL)
    proc2.wait(timeout=10)
    conn3 = store.connect(db)
    row2 = store.get_outbox(conn3, "FAKE-X-001")
    assert row2["status"] == "needs_check" and row2["seq"] == 1, f"row={row2}"
    calls2 = [json.loads(l) for l in open(calls_path, encoding="utf-8")
              if l.strip()]
    assert len(calls2) == 1, f"must not resend after restart: {calls2}"
    alert = open(os.path.join(hidden, "needs-check.log"),
                 encoding="utf-8").read()
    assert "FAKE-X-001" in alert, "needs-check.log must record the alert"
    conn3.close()
    return "中断前 claimed（seq=1）、腾讯侧已收到 1 次；重启后转 needs_check+告警，0 次重发，seq 不变"


async def test_t6(mod, workdir):
    """成功回执判定：只有含非空 id 的 dict 才算成功；空 dict、错误 dict、
    非 dict 一律不误标成功。"""
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    cases = [
        ("FAKE-S-001", {"id": "srv-msg-1", "timestamp": 123}, "sent"),
        ("FAKE-S-002", {}, "needs_check"),
        ("FAKE-S-003", {"code": 11298, "message": "接口访问源IP不在白名单"},
         "failed"),
        ("FAKE-S-004", {"err_code": 40023002, "message": "x"}, "failed"),
        ("FAKE-S-005", "ok", "needs_check"),
        ("FAKE-S-006", {"code": 0, "message": "ok"}, "needs_check"),
    ]
    for msg_id, result, expect in cases:
        conn = store.connect(db)
        store.record_inbound(conn, msg_id, "c2c", "fake-openid", "x")
        store.enqueue_reply(conn, msg_id, "c2c", "fake-openid", "reply")
        bridge = make_bridge(mod, db, FakeApi(mode="canned", result=result))
        row = store.claim_next(bridge.db)
        assert row is not None, f"no row claimed for {msg_id}"
        await bridge._send_one(row)
        got = store.get_outbox(bridge.db, msg_id)
        assert got["status"] == expect, \
            f"{msg_id}: {result!r} -> {got['status']}, want {expect}"
        if expect == "failed":
            assert str(11298) in (got["last_error"] or "") or \
                str(40023002) in (got["last_error"] or ""), \
                f"failed must record the error code: {got['last_error']}"
        if expect == "sent":
            assert store.get_inbound(bridge.db, msg_id)["status"] == "done"
        bridge.db.close()
        conn.close()
    return "含 id 的 dict 才 sent；{} / 错误 dict / 字符串 / code=0 无 id 均不误标成功"


async def main():
    tests = [("T1 race", test_t1), ("T2 concurrent", test_t2),
             ("T3a crash-before-commit", test_t3a),
             ("T3b rerun-after-commit", test_t3b),
             ("T4a receipt-lost", test_t4a),
             ("T4b explicit-reject", test_t4b),
             ("T5 kill-mid-send", test_t5),
             ("T6 receipt-shape", test_t6)]
    failures = 0
    for name, fn in tests:
        workdir = tempfile.mkdtemp(prefix="qq-sqlite-test-")
        try:
            mod = load_daemon(workdir)
            detail = await fn(mod, workdir)
            print(f"PASS {name}: {detail}", flush=True)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}", flush=True)
    print(f"结果：{len(tests) - failures}/{len(tests)} 通过")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
