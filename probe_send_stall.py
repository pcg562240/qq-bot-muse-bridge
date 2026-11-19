#!/usr/bin/env python3
"""发送停摆探针（只告警，不重启、不改库、不碰生产）。

背景：保活只看 daemon 进程在不在。发送循环（send_loop）是进程里的
一个 asyncio 任务：它一旦异常退出（比如 claim_next 抛错），进程还活
着、网关还连着，但没有新的 READY 会再把它拉起来——入站照常写库，
出站行永远没人认领，静默停摆。本探针从库里看积压来发现这种停摆。

用法（由保活 hook 或每分钟定时任务每 60 秒调一次，单次检查）：
  python3 probe_send_stall.py --db <bridge.db 路径> --state-dir <状态目录>

退出码（含义是“本轮有没有新内容要报”，不是“当前有没有问题”）：
  0  正常 / 首次积压信号（等下一轮确认）/ 同一异常已报过（静默）
  1  本轮确认告警（已把告警行写入 send-stall-alerts.log 并打印到 stdout）
  2  本轮新报的探针故障：数据库打不开或读不到（已把故障行写入
     probe-errors.log 并打印到 stdout）。同一故障重复出现时静默
     （exit 0）。这不是“队列为空”，也不是“发送停了”。

积压信号（阈值可调）：
  pending 行 created_at 超过 60 秒未被认领，或 claimed 行 updated_at
  超过 120 秒未解决。健康时循环每 5 秒扫一次，认领到发出只要几秒，
  所以这两个数在健康时永远是 0。

确认规则（防抖）：
  连续两轮都看到积压才告警，且第二轮的最老积压行必须是同一条
  msg_id（同行卡住 = 循环停了；行换了 = 发送只是在慢走，照常流动
  的慢发送不会误报）。180 秒窗口只用于首次两轮确认：一旦告警，
  同一行持续卡住时不再因窗口过期重新告警——已告警的异常只有在
  该行离开积压（发出或转终态）后才算结束；之后的新积压重新走
  两轮确认再报。状态记在 --state-dir/probe-state.json；状态文件
  损坏按无状态处理。上一轮信号超过 180 秒（探针停跑过）不算连续，
  重新从首轮信号计。

探针故障单独处理：
  打开或读库抛任何异常（文件不存在、不是数据库、权限、锁超时等）
  都走 probe_error：不认作队列为空，不认作发送停摆。同一故障只报
  一次（按错误文本去重），恢复后清除。

已知盲区（重要）：
  队列为空时，这个探针发现不了发送循环已死——没有积压行可看。
  要覆盖那种情况，需要 daemon 侧的心跳（send_loop 每轮写时间戳）
  或 fail-fast（循环异常退出即让进程退出，交给现有保活重启）。
  那是另一个改动，不在本探针里。

只读保证：库用 sqlite3 URI mode=ro 打开，timeout=5 秒，并加
PRAGMA query_only=ON；探针只 SELECT，不写库。WAL 模式下只读连接
仍可能创建/读取 -shm 边车文件，但绝不写 .db 文件本身。
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.parse

PENDING_THRESHOLD_S = 60
CLAIMED_THRESHOLD_S = 120
CONFIRM_GAP_S = 180  # 上一轮信号超过这么久不算连续，重新确认
DB_TIMEOUT_S = 5

STATE_FILE = "probe-state.json"
ALERT_LOG = "send-stall-alerts.log"
ERROR_LOG = "probe-errors.log"

EXIT_OK = 0
EXIT_ALERT = 1
EXIT_PROBE_ERROR = 2


def _fmt_wait(seconds):
    """等了多久：秒数 + 易读形式（如 187 秒 / 3分07秒）。"""
    seconds = int(seconds)
    if seconds < 60:
        return "%d 秒" % seconds
    return "%d 秒（%d分%02d秒）" % (seconds, seconds // 60, seconds % 60)


def _read_stale(conn, now, pending_threshold=PENDING_THRESHOLD_S,
                claimed_threshold=CLAIMED_THRESHOLD_S):
    """读积压：返回 {'pending': n, 'claimed': n,
    'oldest_msg_id': str|None, 'oldest_wait_s': float}。

    oldest 取 pending 最老 created_at 与 claimed 最老 updated_at 中
    更老的那一个；无积压时 oldest 为 None。
    """
    cur = conn.cursor()
    pending_n = cur.execute(
        "SELECT COUNT(*) FROM outbox"
        " WHERE status='pending' AND created_at <= ?",
        (now - pending_threshold,)).fetchone()[0]
    pending_oldest = cur.execute(
        "SELECT msg_id, created_at FROM outbox"
        " WHERE status='pending' AND created_at <= ?"
        " ORDER BY created_at ASC LIMIT 1",
        (now - pending_threshold,)).fetchone()
    claimed_n = cur.execute(
        "SELECT COUNT(*) FROM outbox"
        " WHERE status='claimed' AND updated_at <= ?",
        (now - claimed_threshold,)).fetchone()[0]
    claimed_oldest = cur.execute(
        "SELECT msg_id, updated_at FROM outbox"
        " WHERE status='claimed' AND updated_at <= ?"
        " ORDER BY updated_at ASC LIMIT 1",
        (now - claimed_threshold,)).fetchone()
    candidates = []
    if pending_oldest is not None:
        candidates.append((pending_oldest[0], now - pending_oldest[1]))
    if claimed_oldest is not None:
        candidates.append((claimed_oldest[0], now - claimed_oldest[1]))
    oldest_msg_id, oldest_wait = (None, 0.0)
    if candidates:
        oldest_msg_id, oldest_wait = max(candidates, key=lambda c: c[1])
    return {"pending": pending_n, "claimed": claimed_n,
            "oldest_msg_id": oldest_msg_id, "oldest_wait_s": oldest_wait}


def _load_state(state_dir):
    path = os.path.join(state_dir, STATE_FILE)
    try:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            return {}
        return state
    except (OSError, ValueError):
        # 状态文件不存在或损坏：按无状态处理，不影响本轮判断。
        return {}


def _save_state(state_dir, state):
    os.makedirs(state_dir, exist_ok=True)
    path = os.path.join(state_dir, STATE_FILE)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, path)


def _append_log(state_dir, filename, line):
    os.makedirs(state_dir, exist_ok=True)
    with open(os.path.join(state_dir, filename), "a",
              encoding="utf-8") as f:
        f.write(line + "\n")


def _alert_text(stale, now):
    total = stale["pending"] + stale["claimed"]
    ts = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now))
    return (
        "%s SEND_STALL 发送停摆：待发积压 %d 条"
        "（pending %d 条，claimed %d 条），"
        "最老记录 %s 已等 %s；"
        "发送循环可能已停（进程存活不代表循环在跑），请检查 daemon。"
        % (ts, total, stale["pending"], stale["claimed"],
           stale["oldest_msg_id"], _fmt_wait(stale["oldest_wait_s"])))


def _error_text(error, now):
    ts = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now))
    return (
        "%s PROBE_ERROR 探针读库失败：%s；"
        "无法判断队列状态（不是“队列为空”，也不是“发送停了”），"
        "请检查数据库路径与权限。"
        % (ts, error))


def check(db_path, state_dir, now=None,
          pending_threshold=PENDING_THRESHOLD_S,
          claimed_threshold=CLAIMED_THRESHOLD_S,
          confirm_gap=CONFIRM_GAP_S):
    """跑一轮探针。返回 dict，至少含 outcome 与 exit_code。

    outcome: ok | first_signal | alert | already_alerted | probe_error。
    now 可注入（测试用）；不传则用 time.time()。
    本函数只读库；状态与日志写在 state_dir，不碰 bridge.db。
    """
    now = time.time() if now is None else now
    state = _load_state(state_dir)

    # --- 读库：任何异常都是探针故障，单独处理 ---
    try:
        conn = sqlite3.connect(
            "file:%s?mode=ro" % urllib.parse.quote(db_path), uri=True,
            timeout=DB_TIMEOUT_S)
        try:
            conn.execute("PRAGMA query_only=ON")
            stale = _read_stale(conn, now, pending_threshold,
                                claimed_threshold)
        finally:
            conn.close()
    except Exception as exc:  # 文件不存在 / 不是数据库 / 锁超时 / 权限
        error = "%s: %s" % (type(exc).__name__, exc)
        if state.get("probe_error") != error:
            state["probe_error"] = error
            _save_state(state_dir, state)
            line = _error_text(error, now)
            _append_log(state_dir, ERROR_LOG, line)
            return {"outcome": "probe_error", "exit_code": EXIT_PROBE_ERROR,
                    "error": error, "text": line, "reported": True}
        # 同一故障已报过：本轮无新内容，静默（exit 0），不重复告警。
        return {"outcome": "probe_error", "exit_code": EXIT_OK,
                "error": error, "text": _error_text(error, now),
                "reported": False}

    # 读库成功：清除已记录的探针故障（恢复了）。
    if state.get("probe_error") is not None:
        state.pop("probe_error", None)
        _save_state(state_dir, state)

    # --- 无积压：干净，清除本轮异常标记 ---
    if stale["pending"] == 0 and stale["claimed"] == 0:
        if state.get("first_seen") is not None or state.get("alerted"):
            state.pop("first_seen", None)
            state.pop("oldest_msg_id", None)
            state.pop("alerted", None)
            _save_state(state_dir, state)
        return {"outcome": "ok", "exit_code": EXIT_OK, "stale": stale}

    # --- 有积压：两轮确认，同行卡住才告警 ---
    # 已告警且同一行仍卡着：180 秒窗口只用于首次两轮确认，一旦告警
    # 就不再因窗口过期重新告警。只有该行离开积压（发出或转终态），
    # 本次异常才算结束；之后的新积压重新走两轮确认。
    if (state.get("alerted")
            and state.get("oldest_msg_id") == stale["oldest_msg_id"]):
        return {"outcome": "already_alerted", "exit_code": EXIT_OK,
                "stale": stale}
    first_seen = state.get("first_seen")
    prev_oldest = state.get("oldest_msg_id")
    confirmed = (
        first_seen is not None
        and prev_oldest == stale["oldest_msg_id"]
        and (now - first_seen) <= confirm_gap)
    if not confirmed:
        # 首轮信号，或上一轮信号太久 / 最老的行换了：重新从首轮计。
        state["first_seen"] = now
        state["oldest_msg_id"] = stale["oldest_msg_id"]
        state.pop("alerted", None)
        _save_state(state_dir, state)
        return {"outcome": "first_signal", "exit_code": EXIT_OK,
                "stale": stale}
    # 到这里：同行连续两轮确认，且尚未告警（已告警同行已在上面返回）。
    state["alerted"] = True
    _save_state(state_dir, state)
    line = _alert_text(stale, now)
    _append_log(state_dir, ALERT_LOG, line)
    return {"outcome": "alert", "exit_code": EXIT_ALERT,
            "stale": stale, "text": line}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="QQ 桥发送停摆探针（只告警，不重启、不改库）。")
    parser.add_argument("--db", required=True,
                        help="bridge.db 路径（只读打开）")
    parser.add_argument("--state-dir", required=True,
                        help="状态与日志目录（probe-state.json、"
                             "send-stall-alerts.log、probe-errors.log）")
    parser.add_argument("--pending-threshold", type=int,
                        default=PENDING_THRESHOLD_S,
                        help="pending 行超期秒数（默认 60）")
    parser.add_argument("--claimed-threshold", type=int,
                        default=CLAIMED_THRESHOLD_S,
                        help="claimed 行超期秒数（默认 120）")
    args = parser.parse_args(argv)
    result = check(args.db, args.state_dir, now=None,
                   pending_threshold=args.pending_threshold,
                   claimed_threshold=args.claimed_threshold)
    if result["outcome"] in ("alert", "probe_error") and result.get("reported", True):
        print(result["text"])
    return result["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
