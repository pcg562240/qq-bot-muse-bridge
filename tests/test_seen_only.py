#!/usr/bin/env python3
"""T9：seen-only 快照测试。旧版先写 seen 再入队，崩在中间会留下只在
seen 出现的 ID。迁移时只报告，不补发、不重入队。

合成快照：queue.jsonl 2 条，seen.json 3 个 id（多 1 个 seen-only），
poller-seen 2 条，outbox 空，seq.json 2 条。
断言：import 的 summary 报告 seen_only_warnings=1；库里该 ID 为 legacy，
无待发记录；export 后 seen.json 仍含该 ID，queue.jsonl 仍为原 2 条
（不凭空多一行），poller-seen 含该 ID（legacy-not-verified），
outbox.jsonl 为空。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_seen_only.py
"""
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
FIXDIR = os.path.dirname(HERE)
sys.path.insert(0, FIXDIR)

import migrate  # noqa: E402
import store  # noqa: E402


def main():
    src = tempfile.mkdtemp(prefix="qq-seenonly-src-")
    dst = tempfile.mkdtemp(prefix="qq-seenonly-dst-")
    db = os.path.join(tempfile.mkdtemp(prefix="qq-seenonly-db-"), "bridge.db")

    queue_rows = [
        {"scene": "c2c", "openid": "fake-openid", "msg_id": "FAKE-Q-001",
         "content": "消息 1", "received_at": 1795067000.0},
        {"scene": "c2c", "openid": "fake-openid", "msg_id": "FAKE-Q-002",
         "content": "消息 2", "received_at": 1795067100.0},
    ]
    with open(os.path.join(src, "queue.jsonl"), "w", encoding="utf-8") as f:
        for r in queue_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(os.path.join(src, "seen.json"), "w", encoding="utf-8") as f:
        json.dump(["FAKE-Q-001", "FAKE-Q-002", "FAKE-SEEN-ONLY-001"], f)
    with open(os.path.join(src, "poller-seen.json"), "w",
              encoding="utf-8") as f:
        json.dump([{"msg_id": "FAKE-Q-001", "disposition": "replied-to-owner",
                    "note": "", "ts_utc": "2026-11-19T00:00:00Z"},
                   {"msg_id": "FAKE-Q-002", "disposition": "replied-to-owner",
                    "note": "", "ts_utc": "2026-11-19T00:00:00Z"}], f)
    with open(os.path.join(src, "outbox.jsonl"), "w", encoding="utf-8") as f:
        f.write("")
    with open(os.path.join(src, "seq.json"), "w", encoding="utf-8") as f:
        json.dump({"FAKE-Q-001": 2, "FAKE-Q-002": 2}, f)

    rc = migrate.do_import(src, db, force=False)
    assert rc == 0
    # import 的 summary 在 do_import 内部打印；这里直接查库复核
    conn = store.connect(db)
    row = store.get_inbound(conn, "FAKE-SEEN-ONLY-001")
    assert row is not None and row["status"] == "legacy", f"row={row}"
    assert row["content"] == "" and row["received_at"] == 0
    assert store.get_outbox(conn, "FAKE-SEEN-ONLY-001") is None, \
        "seen-only ID must not create a pending outbox row"
    assert store.list_pending_inbound(conn) == [], \
        "nothing must be pending for reprocessing"
    conn.close()
    print("PASS import: seen-only ID 记为 legacy、无待发记录、不重入队",
          flush=True)

    rc = migrate.do_export(db, dst)
    assert rc == 0
    seen = json.load(open(os.path.join(dst, "seen.json"), encoding="utf-8"))
    assert "FAKE-SEEN-ONLY-001" in seen, "seen.json must keep the ID"
    queue_after = [json.loads(l) for l in
                   open(os.path.join(dst, "queue.jsonl"), encoding="utf-8")
                   if l.strip()]
    assert len(queue_after) == 2, \
        f"queue.jsonl must stay at 2 rows, got {len(queue_after)}"
    assert {r["msg_id"] for r in queue_after} == {"FAKE-Q-001", "FAKE-Q-002"}
    poller = json.load(open(os.path.join(dst, "poller-seen.json"),
                            encoding="utf-8"))
    disp = {p["msg_id"]: p["disposition"] for p in poller}
    assert disp.get("FAKE-SEEN-ONLY-001") == "legacy-not-verified", \
        f"poller={disp}"
    assert os.path.getsize(os.path.join(dst, "outbox.jsonl")) == 0
    print("PASS export: seen.json 保留该 ID；queue.jsonl 仍为原 2 条；"
          "poller-seen 为 legacy-not-verified；outbox 为空", flush=True)
    print("结果：T9 通过（seen-only 只报告，不补发）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
