#!/usr/bin/env python3
"""回滚演练 T7（假消息）：新版先处理 3 条假消息（已发送 / 明确失败 /
结果不明），再回滚到旧版，确认这些记录不重新进入旧版自动发送队列。
数据库留档（tests/archive/），不删除。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_rollback_drill.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FIXDIR = os.path.dirname(HERE)


def load_module(workdir, filename, module_name):
    sys.path.insert(0, workdir)
    spec = importlib.util.spec_from_file_location(
        module_name, os.path.join(workdir, filename))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


class PerMsgFakeApi:
    """按 msg_id 区分行为：ok → 成功回执；reject → 明确拒绝；
    none → botpy 超时行为（返回 None）。"""

    def __init__(self, behaviors):
        self.behaviors = behaviors
        self.calls = []

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        self.calls.append((msg_id, msg_seq))
        kind, value = self.behaviors[msg_id]
        if kind == "ok":
            return value
        if kind == "reject":
            raise value
        if kind == "none":
            return None
        raise AssertionError(kind)

    async def post_group_message(self, group_openid, content, msg_type=0,
                                 msg_id=None, msg_seq=None):
        return await self.post_c2c_message(
            group_openid, content, msg_type, msg_id, msg_seq)


async def wait_until(pred, timeout=30, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


async def stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def main():
    from botpy.errors import ForbiddenError

    # ---------- 第一阶段：新版处理 3 条假消息 ----------
    newdir = tempfile.mkdtemp(prefix="qq-rollback-new-")
    hidden = os.path.join(newdir, "hidden_files")
    os.makedirs(hidden, exist_ok=True)
    for name in ("store.py", "daemon.py"):
        shutil.copyfile(os.path.join(FIXDIR, name),
                        os.path.join(newdir, name))
    newmod = load_module(newdir, "daemon.py", "daemon_new")
    store = newmod.store
    db = os.path.join(hidden, "bridge.db")
    conn = store.connect(db)
    msgs = ["FAKE-R-001", "FAKE-R-002", "FAKE-R-003"]
    for m in msgs:
        store.record_inbound(conn, m, "c2c", "fake-openid", f"假消息 {m}")
        store.enqueue_reply(conn, m, "c2c", "fake-openid", f"回复 {m}")
    bridge = object.__new__(newmod.Bridge)
    bridge.db = store.connect(db)
    api = PerMsgFakeApi({
        "FAKE-R-001": ("ok", {"id": "srv-1", "timestamp": 1}),
        "FAKE-R-002": ("reject", ForbiddenError("fake: forbidden")),
        "FAKE-R-003": ("none", None),
    })
    bridge.api = api
    task = asyncio.create_task(bridge.send_loop())
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-R-001")["status"] == "sent"
        and store.get_outbox(bridge.db, "FAKE-R-002")["status"] == "failed"
        and store.get_outbox(bridge.db, "FAKE-R-003")["status"] == "needs_check",
        timeout=30)
    await stop(task)
    assert ok, f"terminal states not reached: {store.counts(bridge.db)}"
    assert sorted(c[0] for c in api.calls) == sorted(msgs), f"calls={api.calls}"
    assert len(api.calls) == 3, f"each message sent exactly once: {api.calls}"
    assert store.get_inbound(bridge.db, "FAKE-R-001")["status"] == "done"
    bridge.db.close()
    conn.close()
    print("PASS phase1 new-version: R1 sent, R2 failed, R3 needs_check, "
          "each called exactly once", flush=True)

    # ---------- 留档：数据库不删除 ----------
    archive_dir = os.path.join(FIXDIR, "tests", "archive")
    os.makedirs(archive_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    archived = os.path.join(archive_dir, f"rollback-drill-{stamp}.db")
    shutil.copyfile(db, archived)
    for suffix in ("-wal", "-shm"):
        src = db + suffix
        if os.path.exists(src):
            shutil.copyfile(src, archived + suffix)
    assert os.path.exists(db), "source DB must not be deleted"
    assert os.path.exists(archived), "archive copy must exist"
    print(f"PASS archive: DB kept at {archived} (source not deleted)",
          flush=True)

    # ---------- 第二阶段：export 到旧版目录 ----------
    sys.path.insert(0, FIXDIR)
    import migrate
    olddir = tempfile.mkdtemp(prefix="qq-rollback-old-")
    old_hidden = os.path.join(olddir, "hidden_files")
    rc = migrate.do_export(db, old_hidden)
    assert rc == 0
    outbox_bytes = os.path.getsize(os.path.join(old_hidden, "outbox.jsonl"))
    assert outbox_bytes == 0, f"outbox.jsonl must be empty, got {outbox_bytes} bytes"
    held = json.load(open(os.path.join(old_hidden, "held-back-export.json"),
                          encoding="utf-8"))
    held_ids = sorted(h["msg_id"] for h in held)
    assert held_ids == ["FAKE-R-002", "FAKE-R-003"], f"held={held_ids}"
    queue_rows = [json.loads(l) for l in
                  open(os.path.join(old_hidden, "queue.jsonl"), encoding="utf-8")
                  if l.strip()]
    assert len(queue_rows) == 3, f"queue rows={len(queue_rows)}"
    poller = json.load(open(os.path.join(old_hidden, "poller-seen.json"),
                            encoding="utf-8"))
    assert len(poller) == 3, f"poller rows={len(poller)}"
    print("PASS phase2 export: outbox.jsonl empty; R2+R3 in held-back-export.json; "
          "queue 3 rows, poller-seen 3 rows", flush=True)

    # ---------- 第三阶段：旧版 daemon 跑两轮，确认 0 次发送 ----------
    shutil.copyfile(
        os.path.join(HERE, "fixtures", "daemon_legacy.py"),
                    os.path.join(olddir, "daemon.py"))
    oldmod = load_module(olddir, "daemon.py", "daemon_old")
    old_bridge = object.__new__(oldmod.Bridge)
    old_bridge.seen = set()
    old_bridge.seq = {}

    class OldFakeApi:
        def __init__(self):
            self.calls = []

        async def post_c2c_message(self, openid, content, msg_type=0,
                                   msg_id=None, msg_seq=None):
            self.calls.append((msg_id, msg_seq))
            return {"id": "fake"}

        async def post_group_message(self, group_openid, content, msg_type=0,
                                     msg_id=None, msg_seq=None):
            self.calls.append((msg_id, msg_seq))
            return {"id": "fake"}

    old_api = OldFakeApi()
    old_bridge.api = old_api
    old_task = asyncio.create_task(old_bridge.outbox_loop())
    await asyncio.sleep(12)  # 两轮（每轮先睡 5 秒）
    await stop(old_task)
    assert old_api.calls == [], f"old daemon must not send: {old_api.calls}"
    assert os.path.getsize(os.path.join(old_hidden, "outbox.jsonl")) == 0
    queue_after = [json.loads(l) for l in
                   open(os.path.join(old_hidden, "queue.jsonl"), encoding="utf-8")
                   if l.strip()]
    assert len(queue_after) == 3
    print("PASS phase3 old-version: 0 send calls in 2 loop passes; "
          "outbox.jsonl still empty; queue intact (3 rows)", flush=True)
    print("结果：回滚演练通过（sent/failed/needs_check 均未重新进入旧版自动发送队列）")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
