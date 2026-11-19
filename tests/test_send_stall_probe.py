#!/usr/bin/env python3
"""发送停摆探针离线测试 T15（假库、假时间，不碰生产、不连网关）。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_send_stall_probe.py

探针：probe_send_stall.py，只读打开 bridge.db，每 60 秒一轮。
pending 超 60 秒 / claimed 超 120 秒算积压信号；连续两轮同行卡住
才告警；同一异常只报一次；干净一轮清除标记；读库失败单独报
探针故障（不当成队列为空，也不当成发送停了）。

覆盖用户验收清单：
  T15-A 正常发送不报
  T15-B 短暂慢发送恢复后不报；持续流动的慢发送也不报（最老的行换了）
  T15-C 进程仍在但发送循环退出、无新 READY 时能报（同异常只报一次，
        恢复后清除，新异常重新确认再报）
  T15-D 数据库读取失败能单独识别（不存在 / 损坏 / 恢复后清除）
  T15-E 队列为空时探针发现不了发送循环已死（已知盲区，见注释）
  T15-F 探针只读：跑完库文件字节与行内容不变
  T15-G CLI 实跑：两轮退出码 0 再 1，第二轮 stdout 有 SEND_STALL
  T15-H 同一行持续卡住 10 分钟只报一次；恢复后新积压能重新告警
"""
import hashlib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPODIR = os.path.dirname(HERE)

BASE = 1_800_000_000.0  # 固定假时间，轮次每次 +60 秒


def load_module(workdir, name, filename):
    path = os.path.join(workdir, filename)
    shutil.copyfile(os.path.join(REPODIR, filename), path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def make_db(workdir, store):
    db = os.path.join(workdir, "bridge.db")
    conn = store.connect(db)
    return db, conn


def seed_pending(store, conn, msg_id, age_s, now):
    """入队一条 pending 行，并把 created_at/updated_at 拨到 now-age_s。"""
    store.record_inbound(conn, msg_id, "c2c", "OWNER", "test")
    store.enqueue_reply(conn, msg_id, "c2c", "OWNER", "reply " + msg_id)
    conn.execute("UPDATE outbox SET created_at=?, updated_at=? "
                 "WHERE msg_id=?", (now - age_s, now - age_s, msg_id))


def seed_claimed(store, conn, msg_id, age_s, now):
    """入队并认领一条 claimed 行，updated_at 拨到 now-age_s。"""
    seed_pending(store, conn, msg_id, age_s + 1000, now)
    row = store.claim_next(conn)
    assert row is not None and row["msg_id"] == msg_id, row
    conn.execute("UPDATE outbox SET updated_at=? WHERE msg_id=?",
                 (now - age_s, msg_id))
    return row


def mark_sent(conn, msg_id, now):
    conn.execute("UPDATE outbox SET status='sent', updated_at=? "
                 "WHERE msg_id=?", (now, msg_id))


def alert_lines(state_dir):
    path = os.path.join(state_dir, "send-stall-alerts.log")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [ln for ln in f.read().splitlines() if ln]


def error_lines(state_dir):
    path = os.path.join(state_dir, "probe-errors.log")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [ln for ln in f.read().splitlines() if ln]


def check(name, cond, detail=""):
    print("%s %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        raise AssertionError(name + " " + detail)


def test_a_normal():
    """T15-A：正常发送（行几秒内解决）两轮都不报。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_a", "probe_send_stall.py")
        store = load_module(td, "store_a", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        # 注意顺序：先认领 claimed 行再补 pending 行——claim_next
        # 取 id 最小的 pending 行，先 seed pending 会认领错行。
        seed_claimed(store, conn, "A-2", 5, BASE)
        seed_pending(store, conn, "A-1", 10, BASE)
        r1 = probe.check(db, sd, now=BASE)
        check("T15-A r1 ok", r1["outcome"] == "ok", str(r1))
        # 循环正常工作：两行都发出，又来一条新鲜的。
        mark_sent(conn, "A-1", BASE + 5)
        mark_sent(conn, "A-2", BASE + 5)
        seed_pending(store, conn, "A-3", 5, BASE + 60)
        r2 = probe.check(db, sd, now=BASE + 60)
        check("T15-A r2 ok", r2["outcome"] == "ok", str(r2))
        check("T15-A 无告警", alert_lines(sd) == [])
        check("T15-A 无探针故障", error_lines(sd) == [])
        conn.close()


def test_b_slow_recovers():
    """T15-B：短暂慢发送恢复后不报；持续流动的慢发送也不报。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_b", "probe_send_stall.py")
        store = load_module(td, "store_b", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        # 一行 pending 卡了 65 秒：首轮信号，不报。
        seed_pending(store, conn, "B-1", 65, BASE)
        r1 = probe.check(db, sd, now=BASE)
        check("T15-B 首轮 first_signal",
              r1["outcome"] == "first_signal", str(r1))
        # 慢发送终于发出：下一轮干净，不报。
        mark_sent(conn, "B-1", BASE + 30)
        r2 = probe.check(db, sd, now=BASE + 60)
        check("T15-B 恢复后 ok", r2["outcome"] == "ok", str(r2))
        # claimed 卡 130 秒同样：首轮信号，随后发出，下一轮干净。
        seed_claimed(store, conn, "B-2", 130, BASE + 60)
        r3 = probe.check(db, sd, now=BASE + 60)
        check("T15-B claimed 首轮 first_signal",
              r3["outcome"] == "first_signal", str(r3))
        mark_sent(conn, "B-2", BASE + 90)
        r4 = probe.check(db, sd, now=BASE + 120)
        check("T15-B claimed 恢复后 ok", r4["outcome"] == "ok", str(r4))
        # 持续流动的慢发送：每轮最老的行都换了（上一轮的行已发出），
        # 三轮都是 first_signal，始终不告警。
        seed_pending(store, conn, "B-3", 65, BASE + 120)
        r5 = probe.check(db, sd, now=BASE + 120)
        check("T15-B 流动 r1 first_signal",
              r5["outcome"] == "first_signal", str(r5))
        mark_sent(conn, "B-3", BASE + 150)
        seed_pending(store, conn, "B-4", 62, BASE + 180)
        r6 = probe.check(db, sd, now=BASE + 180)
        check("T15-B 流动 r2 first_signal（最老行换了）",
              r6["outcome"] == "first_signal", str(r6))
        mark_sent(conn, "B-4", BASE + 210)
        seed_pending(store, conn, "B-5", 61, BASE + 240)
        r7 = probe.check(db, sd, now=BASE + 240)
        check("T15-B 流动 r3 first_signal",
              r7["outcome"] == "first_signal", str(r7))
        check("T15-B 全程无告警", alert_lines(sd) == [])
        conn.close()


def test_c_dead_loop_alerts():
    """T15-C：进程仍在、循环退出、无新 READY——同行卡住两轮后告警，
    同异常只报一次，恢复后清除，新异常重新确认再报。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_c", "probe_send_stall.py")
        store = load_module(td, "store_c", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        # 假 daemon 进程：活着，但它的发送循环已死（不碰库）。
        dummy = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"])
        try:
            # 先认领 claimed 行再补 pending 行（claim_next 取 id 最小）。
            seed_claimed(store, conn, "C-2", 300, BASE)
            seed_pending(store, conn, "C-1", 200, BASE)
            r1 = probe.check(db, sd, now=BASE)
            check("T15-C r1 first_signal，不报",
                  r1["outcome"] == "first_signal" and r1["exit_code"] == 0,
                  str(r1))
            check("T15-C r1 无告警", alert_lines(sd) == [])
            # 第二轮：行没动过（循环已死），确认告警。
            r2 = probe.check(db, sd, now=BASE + 60)
            check("T15-C r2 alert",
                  r2["outcome"] == "alert" and r2["exit_code"] == 1, str(r2))
            check("T15-C 告警写明 pending 数量",
                  "pending 1 条" in r2["text"], r2["text"])
            check("T15-C 告警写明 claimed 数量",
                  "claimed 1 条" in r2["text"], r2["text"])
            check("T15-C 告警写明最老等待（360 秒）",
                  "360 秒" in r2["text"], r2["text"])
            check("T15-C 告警落盘 1 行", len(alert_lines(sd)) == 1)
            check("T15-C 进程仍活着（保活会判健康）",
                  dummy.poll() is None)
            # 第三轮仍卡着：同异常不重复报。
            r3 = probe.check(db, sd, now=BASE + 120)
            check("T15-C r3 already_alerted",
                  r3["outcome"] == "already_alerted", str(r3))
            check("T15-C 告警仍只有 1 行", len(alert_lines(sd)) == 1)
            # 模拟循环恢复：行发出，下一轮干净，标记清除。
            mark_sent(conn, "C-1", BASE + 150)
            mark_sent(conn, "C-2", BASE + 150)
            r4 = probe.check(db, sd, now=BASE + 180)
            check("T15-C 恢复后 ok", r4["outcome"] == "ok", str(r4))
            # 新异常：重新走两轮确认再报。
            seed_pending(store, conn, "C-3", 100, BASE + 240)
            r5 = probe.check(db, sd, now=BASE + 240)
            check("T15-C 新异常首轮 first_signal",
                  r5["outcome"] == "first_signal", str(r5))
            r6 = probe.check(db, sd, now=BASE + 300)
            check("T15-C 新异常第二轮 alert",
                  r6["outcome"] == "alert", str(r6))
            check("T15-C 告警累计 2 行", len(alert_lines(sd)) == 2)
        finally:
            dummy.terminate()
            dummy.wait(timeout=10)
        conn.close()


def test_d_probe_error():
    """T15-D：数据库读不到单独报探针故障，不当成队列为空，
    也不当成发送停了；同一故障只报一次；恢复后清除。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_d", "probe_send_stall.py")
        store = load_module(td, "store_d", "store.py")
        sd = os.path.join(td, "state")
        missing = os.path.join(td, "no-such.db")
        r1 = probe.check(missing, sd, now=BASE)
        check("T15-D 不存在 probe_error",
              r1["outcome"] == "probe_error" and r1["exit_code"] == 2,
              str(r1))
        check("T15-D 首报 reported", r1.get("reported") is True, str(r1))
        check("T15-D 故障文本含 PROBE_ERROR",
              "PROBE_ERROR" in r1["text"], r1["text"])
        check("T15-D 故障文本说明不是队列为空",
              "不是“队列为空”" in r1["text"], r1["text"])
        check("T15-D 无停摆告警", alert_lines(sd) == [])
        check("T15-D 故障落盘 1 行", len(error_lines(sd)) == 1)
        r2 = probe.check(missing, sd, now=BASE + 60)
        check("T15-D 同故障不重复报",
              r2["outcome"] == "probe_error" and r2.get("reported") is False,
              str(r2))
        check("T15-D 故障仍 1 行", len(error_lines(sd)) == 1)
        # 损坏文件：另一种故障，单独再报一次。
        bad = os.path.join(td, "bad.db")
        with open(bad, "wb") as f:
            f.write(b"this is not a database")
        r3 = probe.check(bad, sd, now=BASE + 120)
        check("T15-D 损坏 probe_error",
              r3["outcome"] == "probe_error" and r3.get("reported") is True,
              str(r3))
        check("T15-D 故障累计 2 行", len(error_lines(sd)) == 2)
        # 恢复：换成有效空库，下一轮 ok，故障标记清除。
        db, conn = make_db(td, store)
        r4 = probe.check(db, sd, now=BASE + 180)
        check("T15-D 恢复后 ok", r4["outcome"] == "ok", str(r4))
        check("T15-D 故障不再增长", len(error_lines(sd)) == 2)
        conn.close()


def test_e_empty_queue_blind_spot():
    """T15-E（已知盲区，钉住它）：队列为空时探针报 ok——它发现不了
    发送循环已死。空队列无积压信号可看；覆盖这种情况需要 daemon
    侧心跳或 fail-fast，不在本探针里。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_e", "probe_send_stall.py")
        store = load_module(td, "store_e", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        r1 = probe.check(db, sd, now=BASE)
        r2 = probe.check(db, sd, now=BASE + 60)
        check("T15-E 空队列 ok（按设计：看不出循环死活）",
              r1["outcome"] == "ok" and r2["outcome"] == "ok",
              "%s %s" % (r1, r2))
        check("T15-E 空队列无告警", alert_lines(sd) == [])
        conn.close()


def test_f_read_only():
    """T15-F：探针只读——跑完库文件字节与行内容不变，无 journal。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_f", "probe_send_stall.py")
        store = load_module(td, "store_f", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        seed_pending(store, conn, "F-1", 200, BASE)
        conn.close()
        with open(db, "rb") as f:
            before_bytes = f.read()
        rows_before = _dump_rows(db)
        probe.check(db, sd, now=BASE)
        probe.check(db, sd, now=BASE + 60)
        with open(db, "rb") as f:
            after_bytes = f.read()
        check("T15-F 库文件字节不变", before_bytes == after_bytes)
        check("T15-F 行内容不变", rows_before == _dump_rows(db))
        check("T15-F 无 journal 文件",
              not os.path.exists(db + "-journal"))


def _dump_rows(db):
    import sqlite3
    conn = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    try:
        return conn.execute(
            "SELECT msg_id, status, created_at, updated_at FROM outbox"
            " ORDER BY id").fetchall()
    finally:
        conn.close()


def test_h_ten_minute_stall_single_alert():
    """T15-H：同一行持续卡住至少 10 分钟只报一次；恢复后新积压能
    重新告警。回归：旧去重逻辑里 180 秒窗口一过就会重置 first_seen
    并重新告警（第 5 轮 first_signal、第 6 轮又 alert），新逻辑
    一旦告警、同行仍卡住就不再因窗口过期重报。"""
    with tempfile.TemporaryDirectory() as td:
        probe = load_module(td, "probe_h", "probe_send_stall.py")
        store = load_module(td, "store_h", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        seed_pending(store, conn, "H-1", 200, BASE)
        # 11 轮、每轮 +60 秒：覆盖 10 分钟（BASE..BASE+600）。
        outcomes = [probe.check(db, sd, now=BASE + 60 * k)["outcome"]
                    for k in range(11)]
        check("T15-H 首轮 first_signal",
              outcomes[0] == "first_signal", str(outcomes))
        check("T15-H 第二轮 alert",
              outcomes[1] == "alert", str(outcomes))
        check("T15-H 之后 9 轮全是 already_alerted",
              outcomes[2:] == ["already_alerted"] * 9, str(outcomes))
        check("T15-H 10 分钟只报 1 次", len(alert_lines(sd)) == 1)
        # 恢复：行发出，下一轮干净，标记清除。
        mark_sent(conn, "H-1", BASE + 660)
        r = probe.check(db, sd, now=BASE + 660)
        check("T15-H 恢复后 ok", r["outcome"] == "ok", str(r))
        # 新积压：重新走两轮确认再报。
        seed_pending(store, conn, "H-2", 100, BASE + 720)
        r1 = probe.check(db, sd, now=BASE + 720)
        check("T15-H 新积压首轮 first_signal",
              r1["outcome"] == "first_signal", str(r1))
        r2 = probe.check(db, sd, now=BASE + 780)
        check("T15-H 新积压第二轮 alert",
              r2["outcome"] == "alert", str(r2))
        check("T15-H 告警累计 2 行", len(alert_lines(sd)) == 2)
        conn.close()


def test_g_cli():
    """T15-G：CLI 实跑——停摆库两轮退出码 0 再 1，第二轮 stdout 告警。"""
    with tempfile.TemporaryDirectory() as td:
        probe_path = os.path.join(td, "probe_send_stall.py")
        shutil.copyfile(os.path.join(REPODIR, "probe_send_stall.py"),
                        probe_path)
        store = load_module(td, "store_g", "store.py")
        db, conn = make_db(td, store)
        sd = os.path.join(td, "state")
        seed_pending(store, conn, "G-1", 200, BASE)
        conn.close()
        # CLI 用真实时间：把行拨到“60 秒前”相对当前时间。
        import sqlite3
        now = time.time()
        c2 = sqlite3.connect(db)
        c2.execute("UPDATE outbox SET created_at=?, updated_at=?",
                   (now - 200, now - 200))
        c2.commit()
        c2.close()
        p1 = subprocess.run(
            [sys.executable, probe_path, "--db", db, "--state-dir", sd],
            capture_output=True, text=True, timeout=30)
        check("T15-G CLI 首轮 exit 0", p1.returncode == 0, p1.stdout)
        check("T15-G CLI 首轮静默", p1.stdout.strip() == "", p1.stdout)
        p2 = subprocess.run(
            [sys.executable, probe_path, "--db", db, "--state-dir", sd],
            capture_output=True, text=True, timeout=30)
        check("T15-G CLI 次轮 exit 1", p2.returncode == 1, p2.stdout)
        check("T15-G CLI 次轮 stdout 有 SEND_STALL",
              "SEND_STALL" in p2.stdout, p2.stdout)


def main():
    tests = [test_a_normal, test_b_slow_recovers, test_c_dead_loop_alerts,
             test_d_probe_error, test_e_empty_queue_blind_spot,
             test_f_read_only, test_g_cli,
             test_h_ten_minute_stall_single_alert]
    for fn in tests:
        fn()
    print("结果：T15 通过（8 项：正常/慢恢复/停摆告警/读库故障/空队盲区/只读/CLI/10 分钟单次告警）")


if __name__ == "__main__":
    main()
