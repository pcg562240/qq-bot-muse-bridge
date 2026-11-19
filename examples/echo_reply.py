#!/usr/bin/env python3
"""最小回复示例：把 queue.jsonl 里的每条新消息原样 echo 回去。

演示 queue/outbox 协议的用法。生产环境把这里换成你的 Agent：
读 queue.jsonl 生成回复，写 outbox.jsonl 即可。

用法：python3 examples/echo_reply.py（常驻，每 5 秒扫一轮）
"""
import json
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
HIDDEN = BASE / "hidden_files"
QUEUE = HIDDEN / "queue.jsonl"
OUTBOX = HIDDEN / "outbox.jsonl"
SEEN = HIDDEN / "echo-seen.json"


def load_seen():
    try:
        return set(json.loads(SEEN.read_text(encoding="utf-8")))
    except Exception:
        return set()


def main():
    seen = load_seen()
    while True:
        try:
            lines = QUEUE.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            lines = []
        fresh = []
        for line in lines:
            if not line.strip():
                continue
            msg = json.loads(line)
            if msg["msg_id"] in seen:
                continue
            seen.add(msg["msg_id"])
            fresh.append(msg)
        if fresh:
            SEEN.write_text(json.dumps(sorted(seen), ensure_ascii=False),
                            encoding="utf-8")
            with OUTBOX.open("a", encoding="utf-8") as f:
                for msg in fresh:
                    job = {"scene": msg["scene"], "openid": msg["openid"],
                           "msg_id": msg["msg_id"],
                           "text": "echo: " + (msg.get("content") or "")}
                    f.write(json.dumps(job, ensure_ascii=False) + "\n")
            print(f"replied to {len(fresh)} message(s)", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
