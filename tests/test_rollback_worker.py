#!/usr/bin/env python3
"""T10：回滚演练补旧 worker。T7 只跑了旧 daemon，没跑旧回复 worker。
本测试把旧 worker 也跑起来：新版处理 3 条（sent/failed/needs_check）后
回滚，旧 worker 对这 3 条必须 0 入队；再补一条新假消息，旧版仍能正常回复。

说明：旧 worker 本身是定时任务里的 agent 指令
（crons/minutely/qq-bot-reply__interval@1m.md），不是脚本；这里按其
规则逐条转录（只验证路由/去重逻辑，不生成真实回复文本）。
关键规则：按 msg_id 去重，只处理本轮新出现的——去重依据是
poller-seen.json 里的 msg_id 集合；queue.jsonl 本身没有时间戳字段
（字段只有 scene/msg_id/openid/author_id/content/raw），窗口靠
“只处理新出现行”落实，不靠文件里的时间戳。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_rollback_worker.py
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


def run_old_worker(hidden_dir):
    """旧回复 worker 的规则转录。返回 {"jobs", "new_seen"}。"""
    owner = json.load(
        open(os.path.join(hidden_dir, "owner.json"), encoding="utf-8")
    )["user_openid"]
    poller_path = os.path.join(hidden_dir, "poller-seen.json")
    poller = json.load(open(poller_path, encoding="utf-8"))
    seen_ids = {e["msg_id"] for e in poller}
    queue = [json.loads(l) for l in
             open(os.path.join(hidden_dir, "queue.jsonl"), encoding="utf-8")
             if l.strip()]
    jobs, new_seen = [], []
    for r in queue:
        if r["msg_id"] in seen_ids:
            continue  # 已处理过，不翻旧账、不重复回复
        if r["openid"] != owner:
            continue  # 非 owner 只记录、不回复
        jobs.append({"scene": r["scene"], "openid": r["openid"],
                     "msg_id": r["msg_id"],
                     "text": f"模拟回复：{r['content']}"})
        new_seen.append({
            "msg_id": r["msg_id"], "disposition": "replied-to-owner",
            "note": "simulated old worker",
            "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    if jobs:
        with open(os.path.join(hidden_dir, "outbox.jsonl"), "a",
                  encoding="utf-8") as f:
            for j in jobs:
                f.write(json.dumps(j, ensure_ascii=False) + "\n")
    if new_seen:
        poller.extend(new_seen)
        with open(poller_path, "w", encoding="utf-8") as f:
            json.dump(poller, f, ensure_ascii=False)
    return {"jobs": len(jobs), "new_seen": len(new_seen)}


class PerMsgFakeApi:
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
    newdir = tempfile.mkdtemp(prefix="qq-rbworker-new-")
    hidden = os.path.join(newdir, "hidden_files")
    os.makedirs(hidden, exist_ok=True)
    for name in ("store.py", "daemon.py"):
        shutil.copyfile(os.path.join(FIXDIR, name),
                        os.path.join(newdir, name))
    newmod = load_module(newdir, "daemon.py", "daemon_new_w")
    store = newmod.store
    db = os.path.join(hidden, "bridge.db")
    conn = store.connect(db)
    msgs = ["FAKE-W-001", "FAKE-W-002", "FAKE-W-003"]
    for m in msgs:
        store.record_inbound(conn, m, "c2c", "fake-openid", f"假消息 {m}")
        store.enqueue_reply(conn, m, "c2c", "fake-openid", f"回复 {m}")
    bridge = object.__new__(newmod.Bridge)
    bridge.db = store.connect(db)
    bridge.api = PerMsgFakeApi({
        "FAKE-W-001": ("ok", {"id": "srv-1", "timestamp": 1}),
        "FAKE-W-002": ("reject", ForbiddenError("fake: forbidden")),
        "FAKE-W-003": ("none", None),
    })
    task = asyncio.create_task(bridge.send_loop())
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-W-001")["status"] == "sent"
        and store.get_outbox(bridge.db, "FAKE-W-002")["status"] == "failed"
        and store.get_outbox(bridge.db, "FAKE-W-003")["status"] == "needs_check",
        timeout=30)
    await stop(task)
    assert ok, f"terminal states not reached: {store.counts(bridge.db)}"
    bridge.db.close()
    conn.close()
    print("PASS phase1 new-version: W1 sent, W2 failed, W3 needs_check",
          flush=True)

    # ---------- 第二阶段：export + 旧 worker 第一轮 ----------
    sys.path.insert(0, FIXDIR)
    import migrate
    olddir = tempfile.mkdtemp(prefix="qq-rbworker-old-")
    old_hidden = os.path.join(olddir, "hidden_files")
    assert migrate.do_export(db, old_hidden) == 0
    with open(os.path.join(old_hidden, "owner.json"), "w",
              encoding="utf-8") as f:
        json.dump({"user_openid": "fake-openid"}, f)
    r1 = run_old_worker(old_hidden)
    assert r1 == {"jobs": 0, "new_seen": 0}, f"old worker must not re-enqueue: {r1}"
    assert os.path.getsize(os.path.join(old_hidden, "outbox.jsonl")) == 0
    poller = json.load(open(os.path.join(old_hidden, "poller-seen.json"),
                            encoding="utf-8"))
    assert len(poller) == 3, f"poller rows={len(poller)}"
    print("PASS phase2 old-worker round1: sent/failed/needs_check 0 入队，"
          "outbox 为空，poller-seen 仍为 3 条", flush=True)

    # ---------- 第三阶段：补一条新假消息，旧 worker + 旧 daemon ----------
    with open(os.path.join(old_hidden, "queue.jsonl"), "a",
              encoding="utf-8") as f:
        f.write(json.dumps({"scene": "c2c", "msg_id": "FAKE-W-004",
                            "openid": "fake-openid", "author_id": "fake-openid",
                            "content": "新假消息", "raw": {"id": "FAKE-W-004"}},
                           ensure_ascii=False) + "\n")
    r2 = run_old_worker(old_hidden)
    assert r2 == {"jobs": 1, "new_seen": 1}, f"new message must enqueue once: {r2}"
    poller = json.load(open(os.path.join(old_hidden, "poller-seen.json"),
                            encoding="utf-8"))
    assert len(poller) == 4
    jobs = [json.loads(l) for l in
            open(os.path.join(old_hidden, "outbox.jsonl"), encoding="utf-8")
            if l.strip()]
    assert len(jobs) == 1 and jobs[0]["msg_id"] == "FAKE-W-004", f"jobs={jobs}"
    print("PASS phase3 old-worker round2: 新假消息恰好入队 1 次，"
          "poller-seen 变为 4 条", flush=True)

    shutil.copyfile(
        os.path.join(HERE, "fixtures", "daemon_legacy.py"),
                    os.path.join(olddir, "daemon.py"))
    oldmod = load_module(olddir, "daemon.py", "daemon_old_w")
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
    await asyncio.sleep(12)
    await stop(old_task)
    assert old_api.calls == [("FAKE-W-004", 1)], \
        f"only the new message must be sent once: {old_api.calls}"
    assert os.path.getsize(os.path.join(old_hidden, "outbox.jsonl")) == 0
    print("PASS phase4 old-daemon: 新假消息恰好发 1 次（seq=1），"
          "W1/W2/W3 0 次重发，outbox 已清空", flush=True)
    print("结果：T10 通过（旧 worker 不重入队终态记录，新假消息旧版正常回复）")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
