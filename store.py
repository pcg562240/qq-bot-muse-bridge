"""QQ 桥 SQLite 存储层：入站消息 + 出站回复队列。

单文件 bridge.db，WAL 模式。连接用 isolation_level=None（autocommit），
写事务一律显式 BEGIN IMMEDIATE，事务边界全部由本模块控制。

表：
- inbound(msg_id PK, scene, openid, content, received_at, status)
  status: pending（待处理）/ queued（已排队待发）/
          done（已确认发出）/ missed（超时未回）/
          legacy（旧版已处理、送达未核实、不自动重放）
  legacy 只由迁移工具写入：旧版把 botpy 超时返回的 None 当成功，
  且尝试发送前就推进 seq，所以 poller-seen + seq 无法证明送达；
  历史记录一律标 legacy，不当作送达证据。
- outbox(id PK, msg_id UNIQUE, scene, openid, text, status, seq,
         attempts, last_error, created_at, updated_at)
  status: pending（待发）/ claimed（已认领，发送中）/
          sent（已确认发出）/ failed（腾讯明确拒绝，终态，不自动重试）/
          needs_check（结果不明，终态，需人工核对，不自动重发）
- seq_state(msg_id PK, next_seq)：每条原消息的下一个 msg_seq，
  在认领事务里分配并持久化。

修掉的三个问题（见 daemon.py 头部说明）：
1. 出站是表不是文件：worker INSERT（msg_id 唯一键），daemon 短事务
   逐条认领，不再整份重写共享文件。
2. 「回复进入待发队列」与「原消息已处理」在 enqueue_reply 的同一个
   事务里：写入失败则原消息仍为 pending，下一轮可重处理；重复运行
   只产生一条待发记录。scene / openid 以原入站记录为准，传入值
   不一致直接拒绝，不写出站行、不改入站状态。
3. 发送结果不明不自动重发：先持久化认领状态（claimed + 已分配的
   seq），再在事务外调腾讯 API；只有明确成功回执才标记 sent。
"""

import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbound (
  msg_id TEXT PRIMARY KEY,
  scene TEXT NOT NULL,
  openid TEXT NOT NULL,
  content TEXT NOT NULL DEFAULT '',
  received_at REAL NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  CHECK (status IN ('pending', 'queued', 'done', 'missed', 'legacy'))
);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT NOT NULL UNIQUE,
  scene TEXT NOT NULL,
  openid TEXT NOT NULL,
  text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  seq INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  CHECK (status IN ('pending', 'claimed', 'sent', 'failed', 'needs_check'))
);
CREATE TABLE IF NOT EXISTS seq_state (
  msg_id TEXT PRIMARY KEY,
  next_seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, id);
CREATE INDEX IF NOT EXISTS idx_inbound_status ON inbound(status);
"""


def _now():
    return time.time()


def connect(path):
    """打开 bridge.db（建表、WAL、busy_timeout）。调用方负责 close。"""
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def record_inbound(conn, msg_id, scene, openid, content):
    """daemon 入站去重：已存在返回 False，否则插入 pending 返回 True。"""
    cur = conn.execute(
        "INSERT OR IGNORE INTO inbound(msg_id, scene, openid, content, received_at, status)"
        " VALUES(?, ?, ?, ?, ?, 'pending')",
        (msg_id, scene, openid, content or "", _now()))
    return cur.rowcount == 1


def get_inbound(conn, msg_id):
    row = conn.execute(
        "SELECT * FROM inbound WHERE msg_id=?", (msg_id,)).fetchone()
    return dict(row) if row else None


def list_pending_inbound(conn):
    rows = conn.execute(
        "SELECT * FROM inbound WHERE status='pending' ORDER BY received_at").fetchall()
    return [dict(r) for r in rows]


def enqueue_reply(conn, msg_id, scene, openid, text):
    """worker 入队：INSERT 出站行 + inbound 改 queued，同一个事务。

    scene / openid 以原入站记录为准：传入值（None 表示未提供）与原记录
    不一致时报 ValueError，不写出站行、不改入站状态（事务回滚）。
    返回 ('enqueued', row_id) 或 ('duplicate', 已有 row_id)。
    inbound 行不存在、或状态已是 done/missed 时报 ValueError，
    调用方不应为这种消息写回复。
    """
    if not text:
        raise ValueError("empty reply text")
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT scene, openid, status FROM inbound WHERE msg_id=?",
            (msg_id,)).fetchone()
        if row is None:
            raise ValueError(f"inbound row missing for {msg_id}")
        if scene is not None and scene != row["scene"]:
            raise ValueError(
                f"scene mismatch for {msg_id}: caller passed {scene!r},"
                f" inbound record has {row['scene']!r}")
        if openid is not None and openid != row["openid"]:
            raise ValueError(
                f"openid mismatch for {msg_id}: caller passed {openid!r},"
                f" inbound record has {row['openid']!r}")
        if row["status"] not in ("pending", "queued"):
            raise ValueError(f"inbound {msg_id} status is {row['status']}")
        try:
            cur = conn.execute(
                "INSERT INTO outbox(msg_id, scene, openid, text, status, created_at, updated_at)"
                " VALUES(?, ?, ?, ?, 'pending', ?, ?)",
                (msg_id, row["scene"], row["openid"], text, _now(), _now()))
        except sqlite3.IntegrityError:
            old = conn.execute(
                "SELECT id FROM outbox WHERE msg_id=?", (msg_id,)).fetchone()
            conn.execute("ROLLBACK")
            return ("duplicate", old["id"] if old else None)
        conn.execute(
            "UPDATE inbound SET status='queued' WHERE msg_id=?", (msg_id,))
        conn.execute("COMMIT")
        return ("enqueued", cur.lastrowid)
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def claim_next(conn):
    """daemon 认领：短事务内取一条 pending 行，分配 seq，改为 claimed。

    seq 在认领时分配并持久化（seq_state 表），发送在事务外进行。
    无待发行返回 None。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT id, msg_id, scene, openid, text FROM outbox"
            " WHERE status='pending' ORDER BY id LIMIT 1").fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return None
        msg_id = row["msg_id"]
        r = conn.execute(
            "SELECT next_seq FROM seq_state WHERE msg_id=?", (msg_id,)).fetchone()
        seq = r["next_seq"] if r else 1
        conn.execute(
            "INSERT INTO seq_state(msg_id, next_seq) VALUES(?, ?)"
            " ON CONFLICT(msg_id) DO UPDATE SET next_seq=excluded.next_seq",
            (msg_id, seq + 1))
        conn.execute(
            "UPDATE outbox SET status='claimed', seq=?, attempts=attempts+1,"
            " updated_at=? WHERE id=? AND status='pending'",
            (seq, _now(), row["id"]))
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            conn.execute("ROLLBACK")
            return None
        conn.execute("COMMIT")
        d = dict(row)
        d["seq"] = seq
        return d
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def mark_sent(conn, row_id, msg_id):
    """收到明确成功回执：出站行 sent + 原消息 done，同一个事务。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE outbox SET status='sent', updated_at=? WHERE id=? AND status='claimed'",
            (_now(), row_id))
        conn.execute(
            "UPDATE inbound SET status='done' WHERE msg_id=?", (msg_id,))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def mark_failed(conn, row_id, error):
    """腾讯明确拒绝：出站行 failed（终态，不自动重试），记录错误原文。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE outbox SET status='failed', last_error=?, updated_at=?"
            " WHERE id=? AND status='claimed'",
            (error, _now(), row_id))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def mark_needs_check(conn, row_id, error):
    """发送结果不明：出站行 needs_check（终态，需人工核对，不自动重发）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE outbox SET status='needs_check', last_error=?, updated_at=?"
            " WHERE id=? AND status='claimed'",
            (error, _now(), row_id))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def reconcile_claimed(conn):
    """daemon 启动时调用：上个进程遗留的 claimed 行转为 needs_check。

    返回受影响的 msg_id 列表，调用方据此告警。认领后、回执前的任何
    时刻进程退出都会留下 claimed 行；这些行一律不自动重发。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id, msg_id FROM outbox WHERE status='claimed'").fetchall()
        conn.execute(
            "UPDATE outbox SET status='needs_check',"
            " last_error='daemon 重启时仍处于 claimed 状态，发送结果不明',"
            " updated_at=? WHERE status='claimed'", (_now(),))
        conn.execute("COMMIT")
        return [r["msg_id"] for r in rows]
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def get_outbox(conn, msg_id):
    row = conn.execute(
        "SELECT * FROM outbox WHERE msg_id=?", (msg_id,)).fetchone()
    return dict(row) if row else None


def list_needs_check(conn):
    rows = conn.execute(
        "SELECT * FROM outbox WHERE status='needs_check' ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def counts(conn):
    """各状态计数，供巡检与迁移核对。"""
    out = {}
    for (status, n) in conn.execute(
            "SELECT status, COUNT(*) FROM outbox GROUP BY status"):
        out[f"outbox.{status}"] = n
    for (status, n) in conn.execute(
            "SELECT status, COUNT(*) FROM inbound GROUP BY status"):
        out[f"inbound.{status}"] = n
    return out
