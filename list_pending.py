#!/usr/bin/env python3
"""worker 侧巡检 CLI：列出待处理的入站消息（status='pending'）。

供回复轮询每轮先读，再逐条生成回复、调 enqueue_reply.py 入队。
输出 JSON 数组，每条含 msg_id / scene / openid / content / received_at。
"""
import argparse
import json
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import store  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    db_path = args.db or os.environ.get("QQBOT_DB_PATH") or str(
        BASE / "hidden_files" / "bridge.db")
    conn = store.connect(db_path)
    try:
        rows = store.list_pending_inbound(conn)
    finally:
        conn.close()
    print(json.dumps(rows, ensure_ascii=False))


if __name__ == "__main__":
    main()
