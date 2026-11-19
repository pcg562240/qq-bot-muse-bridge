#!/usr/bin/env python3
"""T12：enqueue_reply 的 scene / openid 以原入站记录为准。

- 正常回复（参数与原记录一致）能入队，出站行用原记录的 scene/openid。
- 传错 openid / 传错 scene 都被拒绝：无出站行，入站仍 pending。
- 拒绝后用正确参数仍能入队。
- 参数传 None 视为未提供，用原记录的值。
- CLI 层：传错 openid 时 enqueue_reply.py 退出 1，不写库。
全部用假消息、临时库，离线运行。
"""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
import store  # noqa: E402


def main():
    tmp = tempfile.mkdtemp(prefix="qq-t12-")
    db = os.path.join(tmp, "bridge.db")
    conn = store.connect(db)
    try:
        # 1. 正常回复能入队
        assert store.record_inbound(conn, "FAKE-A-001", "c2c", "fake-openid-1", "你好")
        status, _ = store.enqueue_reply(conn, "FAKE-A-001", "c2c", "fake-openid-1", "你好呀")
        assert status == "enqueued", status
        out = store.get_outbox(conn, "FAKE-A-001")
        assert out["scene"] == "c2c" and out["openid"] == "fake-openid-1"
        assert store.get_inbound(conn, "FAKE-A-001")["status"] == "queued"
        print("PASS T12-1 正常回复：参数与原记录一致，入队成功，出站行用原记录的 scene/openid")

        # 2. 传错 openid 被拒绝
        assert store.record_inbound(conn, "FAKE-A-002", "c2c", "fake-openid-2", "你好")
        try:
            store.enqueue_reply(conn, "FAKE-A-002", "c2c", "wrong-openid", "你好呀")
        except ValueError as exc:
            assert "openid mismatch" in str(exc), exc
        else:
            raise AssertionError("传错 openid 未被拒绝")
        assert store.get_outbox(conn, "FAKE-A-002") is None
        assert store.get_inbound(conn, "FAKE-A-002")["status"] == "pending"
        print("PASS T12-2 传错 openid：拒绝，无出站行，入站仍 pending")

        # 3. 传错 scene 被拒绝
        assert store.record_inbound(conn, "FAKE-A-003", "group", "fake-group-openid", "你好")
        try:
            store.enqueue_reply(conn, "FAKE-A-003", "c2c", "fake-group-openid", "你好呀")
        except ValueError as exc:
            assert "scene mismatch" in str(exc), exc
        else:
            raise AssertionError("传错 scene 未被拒绝")
        assert store.get_outbox(conn, "FAKE-A-003") is None
        assert store.get_inbound(conn, "FAKE-A-003")["status"] == "pending"
        print("PASS T12-3 传错 scene：拒绝，无出站行，入站仍 pending")

        # 4. 拒绝后用正确参数仍能入队
        status, _ = store.enqueue_reply(conn, "FAKE-A-002", "c2c", "fake-openid-2", "你好呀")
        assert status == "enqueued", status
        status, _ = store.enqueue_reply(conn, "FAKE-A-003", "group", "fake-group-openid", "你好呀")
        assert status == "enqueued", status
        print("PASS T12-4 拒绝后用正确参数：两条都成功入队")

        # 5. 参数传 None 视为未提供，用原记录的值
        assert store.record_inbound(conn, "FAKE-A-004", "c2c", "fake-openid-4", "你好")
        status, _ = store.enqueue_reply(conn, "FAKE-A-004", None, None, "你好呀")
        assert status == "enqueued", status
        out = store.get_outbox(conn, "FAKE-A-004")
        assert out["scene"] == "c2c" and out["openid"] == "fake-openid-4"
        print("PASS T12-5 参数为 None：用原记录的 scene/openid 入队")

        # 6. CLI 层：传错 openid 退出 1，不写库
        assert store.record_inbound(conn, "FAKE-A-005", "c2c", "fake-openid-5", "你好")
        proc = subprocess.run(
            [sys.executable, os.path.join(ROOT, "enqueue_reply.py"),
             "--db", db, "--msg-id", "FAKE-A-005", "--scene", "c2c",
             "--openid", "wrong-openid", "--text", "你好呀"],
            capture_output=True, text=True)
        assert proc.returncode == 1, proc
        assert "openid mismatch" in proc.stderr, proc.stderr
        assert store.get_outbox(conn, "FAKE-A-005") is None
        assert store.get_inbound(conn, "FAKE-A-005")["status"] == "pending"
        print("PASS T12-6 CLI 传错 openid：退出 1，无出站行，入站仍 pending")

        # 7. 最终计数：4 条已入队 pending，1 条仍 pending，无多余行
        c = store.counts(conn)
        assert c.get("outbox.pending") == 4, c
        assert c.get("inbound.queued") == 4, c
        assert c.get("inbound.pending") == 1, c
        print("PASS T12-7 最终计数：出站 pending 4、入站 queued 4、入站 pending 1")
        print("结果：T12 通过（scene/openid 以原入站记录为准）")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
