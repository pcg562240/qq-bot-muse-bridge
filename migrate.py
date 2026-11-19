#!/usr/bin/env python3
"""旧版文件队列 ⇄ SQLite 桥库的迁移工具（只在切换 / 回滚时用，不常驻）。

import：读旧版 hidden_files/（queue.jsonl、outbox.jsonl、seen.json、
  poller-seen.json、seq.json），生成 bridge.db。只读旧文件，不改它们。
  目标 DB 已存在时拒绝（防重复导入），除非 --force。
  入站状态判定（历史记录一律不当作送达证据）：
  - legacy：poller-seen 里有——旧版已处理，但送达未核实。旧版把
    botpy 超时返回的 None 当成功，且尝试发送前就推进 seq，所以
    poller-seen + seq=2 不能证明送达。legacy 行不自动重放、不再处理。
  - pending：其余——下一轮 worker 正常处理。
  - seen 里有、queue 里没有的 id：旧版先写 seen 再入队，崩在中间就
    会留下这种 ID。只报告（warnings + summary 的 seen_only_warnings），
    不补发、不重入队：记为 legacy 行（防重入），导出时保留在 seen.json
    与 poller-seen（legacy），不进 queue.jsonl（received_at=0 的行
    不导出为入队记录）。
  seq.json 只用来恢复 seq_state（未来同 msg_id 再发送时的序号连续
  性），不作为送达证据。
  outbox.jsonl 里的 job（含旧版 status=failed 的）一律按 pending 导入，
  新 daemon 会在发送时重新分类（明确拒绝→failed，结果不明→needs_check）；
  其 msg_id 若无入站行则补一条 queued 行并警告。

export：读 bridge.db，写回旧版五个文件，供回滚用。只有 status='pending'
  的行进 outbox.jsonl（真正未发的待发回复）；claimed / failed /
  needs_check 的行绝不进旧版自动发送队列，另写 held-back-export.json
  并警告，人工核对后再定；sent 的行不导出（已确认发出，无需重发）。
  poller-seen.json 是近似导出（只保留 msg_id 集合语义，disposition
  按 done/queued/legacy 填），因为旧版的 note/ts 字段在库里没有对应项；
  回滚后旧 worker 只按 msg_id 去重，语义不受影响。

用法：
  migrate.py import --src <旧 hidden_files 目录> --db <目标 bridge.db>
  migrate.py export --db <bridge.db> --dst <目标目录>
"""
import argparse
import json
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import store  # noqa: E402


def _read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_json(path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def do_import(src, db_path, force):
    src = Path(src)
    db_path = Path(db_path)
    if db_path.exists() and not force:
        print(f"target DB already exists: {db_path} (use --force to overwrite)",
              file=sys.stderr)
        return 1
    if force and db_path.exists():
        db_path.unlink()
    queue_rows = _read_jsonl(src / "queue.jsonl")
    outbox_jobs = _read_jsonl(src / "outbox.jsonl")
    seen_ids = _read_json(src / "seen.json", [])
    poller_seen = _read_json(src / "poller-seen.json", [])
    seq_map = _read_json(src / "seq.json", {})
    poller_ids = {e["msg_id"] for e in poller_seen
                  if isinstance(e, dict) and "msg_id" in e}
    warnings = []
    conn = store.connect(db_path)
    try:
        inbound_n = {"legacy": 0, "pending": 0}
        for r in queue_rows:
            msg_id = r["msg_id"]
            # poller-seen 里有 = 旧版已处理，但送达未核实（见头部说明），
            # 一律标 legacy，不当作送达证据。
            status = "legacy" if msg_id in poller_ids else "pending"
            conn.execute(
                "INSERT OR REPLACE INTO inbound(msg_id, scene, openid, content,"
                " received_at, status) VALUES(?, ?, ?, ?, ?, ?)",
                (msg_id, r.get("scene", ""), r.get("openid") or "",
                 r.get("content", ""), time.time(), status))
            inbound_n[status] += 1
        queue_ids = {r["msg_id"] for r in queue_rows}
        seen_only = [m for m in seen_ids if m not in queue_ids]
        for msg_id in seen_only:
            conn.execute(
                "INSERT OR IGNORE INTO inbound(msg_id, scene, openid, content,"
                " received_at, status) VALUES(?, '', '', '', 0, 'legacy')",
                (msg_id,))
            warnings.append(f"seen-only id without queue record: {msg_id}")
        outbox_n = 0
        for job in outbox_jobs:
            msg_id = job.get("msg_id")
            if not msg_id or not job.get("text"):
                warnings.append(f"skipped outbox job without msg_id/text: {job}")
                continue
            if store.get_inbound(conn, msg_id) is None:
                conn.execute(
                    "INSERT INTO inbound(msg_id, scene, openid, content,"
                    " received_at, status) VALUES(?, ?, ?, '', ?, 'queued')",
                    (msg_id, job.get("scene", ""), job.get("openid") or "",
                     time.time()))
                warnings.append(
                    f"outbox job {msg_id} had no inbound row; created queued placeholder")
            if job.get("status") == "failed":
                warnings.append(
                    f"outbox job {msg_id} was failed in file; imported as pending,"
                    " new daemon will re-classify on send")
            conn.execute(
                "INSERT OR IGNORE INTO outbox(msg_id, scene, openid, text, status,"
                " created_at, updated_at) VALUES(?, ?, ?, ?, 'pending', ?, ?)",
                (msg_id, job.get("scene", ""), job.get("openid") or "",
                 job["text"], time.time(), time.time()))
            outbox_n += 1
        for msg_id, nxt in seq_map.items():
            if isinstance(nxt, int):
                conn.execute(
                    "INSERT OR REPLACE INTO seq_state(msg_id, next_seq) VALUES(?, ?)",
                    (msg_id, nxt))
        summary = {"inbound": inbound_n, "seen_only_warnings": len(seen_only),
                   "outbox_imported": outbox_n, "seq_state": len(seq_map),
                   "counts": store.counts(conn), "warnings": warnings}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


def do_export(db_path, dst):
    db_path, dst = Path(db_path), Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    conn = store.connect(db_path)
    try:
        inbound = conn.execute("SELECT * FROM inbound").fetchall()
        outbox = conn.execute("SELECT * FROM outbox").fetchall()
        seqs = conn.execute("SELECT * FROM seq_state").fetchall()
        warnings = []
        queue_rows = []
        for r in inbound:
            # received_at=0 的行是 seen-only 导入的占位（无原文），
            # 只保留在 seen.json / poller-seen，不进 queue.jsonl
            if r["received_at"] <= 0:
                continue
            queue_rows.append({
                "scene": r["scene"], "msg_id": r["msg_id"],
                "openid": r["openid"], "author_id": r["openid"],
                "content": r["content"], "raw": {"id": r["msg_id"]}})
        (dst / "queue.jsonl").write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in queue_rows),
            encoding="utf-8")
        (dst / "seen.json").write_text(
            json.dumps(sorted(r["msg_id"] for r in inbound), ensure_ascii=False),
            encoding="utf-8")
        (dst / "seq.json").write_text(
            json.dumps({r["msg_id"]: r["next_seq"] for r in seqs}, ensure_ascii=False),
            encoding="utf-8")
        jobs, held = [], []
        for r in outbox:
            job = {"scene": r["scene"], "openid": r["openid"],
                   "msg_id": r["msg_id"], "text": r["text"]}
            if r["status"] == "pending":
                jobs.append(job)
            elif r["status"] in ("claimed", "failed", "needs_check"):
                # 终态或结果不明的行绝不进旧版自动发送队列，留档待人工核对
                held.append({"msg_id": r["msg_id"], "status": r["status"],
                             "seq": r["seq"], "last_error": r["last_error"]})
            # sent 的行不导出：已确认发出，无需重发
        (dst / "outbox.jsonl").write_text(
            "".join(json.dumps(j, ensure_ascii=False) + "\n" for j in jobs),
            encoding="utf-8")
        poller = []
        dispositions = {"done": "replied-to-owner",
                        "queued": "queued-not-sent",
                        "legacy": "legacy-not-verified"}
        for r in inbound:
            if r["status"] in dispositions:
                poller.append({
                    "msg_id": r["msg_id"],
                    "disposition": dispositions[r["status"]],
                    "note": "exported from bridge.db (approximate)",
                    "ts_utc": time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(r["received_at"]))})
        (dst / "poller-seen.json").write_text(
            json.dumps(poller, ensure_ascii=False), encoding="utf-8")
        if held:
            (dst / "held-back-export.json").write_text(
                json.dumps(held, ensure_ascii=False, indent=2), encoding="utf-8")
            warnings.append(
                f"{len(held)} rows NOT exported to outbox.jsonl"
                " (claimed/failed/needs_check); see held-back-export.json,"
                " check manually before relying on rollback")
        summary = {"queue_rows": len(queue_rows), "outbox_jobs": len(jobs),
                   "held_back": len(held),
                   "poller_seen": len(poller), "warnings": warnings}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    pi = sub.add_parser("import")
    pi.add_argument("--src", required=True)
    pi.add_argument("--db", required=True)
    pi.add_argument("--force", action="store_true")
    pe = sub.add_parser("export")
    pe.add_argument("--db", required=True)
    pe.add_argument("--dst", required=True)
    args = ap.parse_args()
    if args.cmd == "import":
        return do_import(args.src, args.db, args.force)
    return do_export(args.db, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
