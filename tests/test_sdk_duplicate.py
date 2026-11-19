#!/usr/bin/env python3
"""T11：首次连接重置，SDK 重试收到重复发送错误。

场景：第一次 POST 抛 ConnectionResetError；SDK 用原参数重试，第二次
POST 收到腾讯的重复发送错误（HTTP 400，同一 msg_id + msg_seq 已用过）。
确认：最终是 needs_check，不会被通用错误码规则标为 failed；重启后
不会自动补发。

为什么不是 failed：重复发送错误只证明“这个 msg_id + msg_seq 已经被
用过”，第一次 POST 可能已经送达。botpy 收到 HTTP 400 会抛
ServerError（400 不在 HttpErrorDict 里），不在明确拒绝之列；
通用错误码规则（classify_send_result）只处理 HTTP 200 的返回体，
HTTP 错误走异常分支，碰不到那条规则。

分两层测（都用真实 BotHttp + 假传输）：
  L1 SDK 层：real BotHttp.request → 抛 ServerError，POST 2 次，两次 seq 相同。
  L2 daemon 层：新 daemon 经 SDK 发 → needs_check；重启后 0 次补发。

跑法（用桥的 venv python）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_sdk_duplicate.py
"""
import asyncio
import importlib.util
import os
import shutil
import sys
import tempfile
import time
from types import SimpleNamespace

from botpy.errors import ServerError
from botpy.http import BotHttp, Route

HERE = os.path.dirname(os.path.abspath(__file__))
FIXDIR = os.path.dirname(HERE)


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self.headers = {"content-type": "application/json"}
        self.request_info = SimpleNamespace(
            url="https://sandbox.api.sgroup.qq.com/v2/users/fake/messages")
        self._payload = payload

    async def json(self):
        return self._payload


class FakeCM:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc

    async def __aenter__(self):
        if self.exc is not None:
            raise self.exc
        return self.response

    async def __aexit__(self, *args):
        return False


class FakeSession:
    """假传输。script 元素：("reset",) 或 ("http", status, payload)。"""

    closed = False

    def __init__(self, script):
        self.script = script
        self.calls = []

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        idx = len(self.calls)
        self.calls.append({"method": method, "url": url,
                           "json": kwargs.get("json")})
        action = self.script[idx]
        if action[0] == "reset":
            return FakeCM(exc=ConnectionResetError("fake: connection reset"))
        return FakeCM(response=FakeResponse(action[1], action[2]))

    async def close(self):
        return None


def make_http(script):
    http = object.__new__(BotHttp)
    http._session = FakeSession(script)
    http._headers = {}
    http.timeout = 5
    http.is_sandbox = True

    async def _noop_check():
        return None

    http.check_session = _noop_check
    return http


class SdkBackedFakeApi:
    """Fake API：内部走真实 BotHttp.request（配假传输），让 SDK 的重试/
    异常行为原样发生。"""

    def __init__(self, http):
        self.http = http
        self.calls = []

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        self.calls.append((msg_id, msg_seq))
        payload = {"openid": openid, "content": content, "msg_type": msg_type,
                   "msg_id": msg_id, "msg_seq": msg_seq}
        return await self.http.request(
            Route("POST", "/v2/users/{openid}/messages", openid=openid),
            json=payload)

    async def post_group_message(self, group_openid, content, msg_type=0,
                                 msg_id=None, msg_seq=None):
        self.calls.append((msg_id, msg_seq))
        payload = {"group_openid": group_openid, "content": content,
                   "msg_type": msg_type, "msg_id": msg_id, "msg_seq": msg_seq}
        return await self.http.request(
            Route("POST", "/v2/groups/{group_openid}/messages",
                  group_openid=group_openid), json=payload)


def load_daemon(workdir):
    os.makedirs(os.path.join(workdir, "hidden_files"), exist_ok=True)
    for name in ("store.py", "daemon.py"):
        shutil.copyfile(os.path.join(FIXDIR, name),
                        os.path.join(workdir, name))
    sys.path.insert(0, workdir)
    spec = importlib.util.spec_from_file_location(
        "daemon_dup", os.path.join(workdir, "daemon.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["daemon_dup"] = mod
    spec.loader.exec_module(mod)
    return mod


async def wait_until(pred, timeout=20, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


async def stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def main():
    dup_body = {"code": 400, "message": "duplicate msg_id+msg_seq"}
    script = [("reset",), ("http", 400, dup_body)]
    payload = {"openid": "fake-openid", "content": "回复", "msg_type": 0,
               "msg_id": "FAKE-D-001", "msg_seq": 1}

    # ---------- L1：SDK 层 ----------
    http = make_http(script)
    try:
        await http.request(
            Route("POST", "/v2/users/{openid}/messages", openid="fake-openid"),
            json=payload)
        raise AssertionError("expected ServerError from the duplicate error")
    except ServerError as exc:
        print(f"PASS L1 SDK: 重试收到重复发送错误 → 抛 ServerError: {exc}",
              flush=True)
    calls = http._session.calls
    assert len(calls) == 2, f"expected 2 POSTs, got {len(calls)}"
    assert calls[0]["json"]["msg_seq"] == calls[1]["json"]["msg_seq"] == 1
    assert calls[0]["json"]["msg_id"] == calls[1]["json"]["msg_id"] \
        == "FAKE-D-001"
    print("PASS L1: POST 2 次，两次 msg_seq 均为 1（原参数重试）", flush=True)
    http._session = None

    # ---------- L2：daemon 层 ----------
    workdir = tempfile.mkdtemp(prefix="qq-sdk-dup-")
    mod = load_daemon(workdir)
    store = mod.store
    db = os.path.join(workdir, "hidden_files", "bridge.db")
    hidden = os.path.join(workdir, "hidden_files")
    conn = store.connect(db)
    store.record_inbound(conn, "FAKE-D-001", "c2c", "fake-openid", "假消息 D")
    store.enqueue_reply(conn, "FAKE-D-001", "c2c", "fake-openid", "回复 D")
    conn.close()

    http2 = make_http(script)
    bridge = object.__new__(mod.Bridge)
    bridge.db = store.connect(db)
    bridge.api = SdkBackedFakeApi(http2)
    task = asyncio.create_task(bridge.send_loop())
    ok = await wait_until(
        lambda: store.get_outbox(bridge.db, "FAKE-D-001")["status"]
        == "needs_check", timeout=20)
    await stop(task)
    row = store.get_outbox(bridge.db, "FAKE-D-001")
    assert ok and row["status"] == "needs_check", f"row={row}"
    assert row["status"] != "failed", "must not be marked failed"
    assert "ServerError" in (row["last_error"] or ""), \
        f"last_error must name ServerError: {row['last_error']}"
    assert store.get_inbound(bridge.db, "FAKE-D-001")["status"] == "queued"
    assert len(http2._session.calls) == 2, \
        f"transport POSTs={len(http2._session.calls)}"
    alert = open(os.path.join(hidden, "needs-check.log"),
                 encoding="utf-8").read()
    assert "FAKE-D-001" in alert
    http2._session = None
    print("PASS L2 daemon: 重复发送错误 → needs_check（非 failed），"
          "错误原文 ServerError，告警已写", flush=True)

    # ---------- 重启：0 次补发 ----------
    http3 = make_http([("http", 200, {"id": "srv-ok-1", "timestamp": 1})])
    bridge2 = object.__new__(mod.Bridge)
    bridge2.db = store.connect(db)
    bridge2.api = SdkBackedFakeApi(http3)
    task2 = asyncio.create_task(bridge2.send_loop())
    await asyncio.sleep(7)
    await stop(task2)
    assert http3._session.calls == [], \
        f"must not auto-resend after restart: {http3._session.calls}"
    row2 = store.get_outbox(bridge.db, "FAKE-D-001")
    assert row2["status"] == "needs_check" and row2["seq"] == 1, f"row={row2}"
    http3._session = None
    print("PASS restart: 0 次补发，行仍为 needs_check，seq 不变", flush=True)
    print("结果：T11 通过（重复发送错误 → needs_check，不标 failed，不自动补发）")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
