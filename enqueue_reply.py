#!/usr/bin/env python3
"""worker 侧入队 CLI：把一条回复写进 SQLite 出站队列。

「INSERT 出站行 + 原消息标记 queued」在 store.enqueue_reply 的同一个
事务里：写入失败则原消息仍为 pending，下一轮可重处理；两个 worker
同时为同一条原消息入队，只产生一条待发记录（另一个返回 duplicate）。

用法：
  enqueue_reply.py --msg-id <原消息 id> --scene c2c|group \
      --openid <对方 openid> --text <回复正文>
  正文很长时用 --text - 从 stdin 读。
  --db 可指定 bridge.db 路径（默认与 daemon.py 同目录的
  hidden_files/bridge.db；QQBOT_DB_PATH 环境变量也可指定）。

退出码：0 已入队；2 已有待发记录（重复运行，不会多写）；1 出错。
"""
import argparse
import json
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import store  # noqa: E402


def default_db():
    return os.environ.get("QQBOT_DB_PATH") or str(
        BASE / "hidden_files" / "bridge.db")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--msg-id", required=True)
    ap.add_argument("--scene", required=True, choices=["c2c", "group"])
    ap.add_argument("--openid", required=True)
    ap.add_argument("--text", required=True, help="回复正文；'-' 表示从 stdin 读")
    args = ap.parse_args()
    text = sys.stdin.read() if args.text == "-" else args.text
    if not text.strip():
        print("empty reply text", file=sys.stderr)
        return 1
    db_path = args.db or default_db()
    conn = store.connect(db_path)
    try:
        status, row_id = store.enqueue_reply(
            conn, args.msg_id, args.scene, args.openid, text)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        conn.close()
    print(json.dumps({"status": status, "row_id": row_id, "msg_id": args.msg_id},
                     ensure_ascii=False))
    return 0 if status == "enqueued" else 2


if __name__ == "__main__":
    raise SystemExit(main())
