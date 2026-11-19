#!/usr/bin/env python3
"""验收 T3a 的子进程：在事务内写两行后睡眠（提交前），等父进程杀掉。

用法：crash_child.py <bridge.db> <msg_id>
模拟进程在落库事务提交前退出：未提交的写入必须对其他连接不可见，
原消息必须仍为 pending。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import store  # noqa: E402

db_path, msg_id = sys.argv[1], sys.argv[2]
conn = store.connect(db_path)
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "INSERT INTO outbox(msg_id, scene, openid, text, status, created_at, updated_at)"
    " VALUES(?, 'c2c', 'fake-openid', '回复', 'pending', ?, ?)",
    (msg_id, time.time(), time.time()))
conn.execute(
    "UPDATE inbound SET status='queued' WHERE msg_id=?", (msg_id,))
print("in-transaction", flush=True)
time.sleep(10)  # 父进程在此期间 SIGKILL，COMMIT 永远到不了
conn.execute("COMMIT")
