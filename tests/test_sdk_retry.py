#!/usr/bin/env python3
"""T8：对实际安装的 qq-botpy 1.2.1 做假传输测试。

场景：第一次 POST 抛 ConnectionResetError，第二次成功返回含 id 的回执。
记录：实际 POST 次数、两次的 msg_seq、上层收到的返回值。
预期（源码已确认）：SDK 内部用原参数重试，但外层没有 return 重试的回执，
上层拿到 None——即使第二次发成功了。

跑法（用桥的 venv python，botpy 从 venv 的 site-packages 导入）：
  ~/workspace/goals/qq-bot-muse/venv/bin/python tests/test_sdk_retry.py
"""
import asyncio
import importlib.metadata as metadata
from types import SimpleNamespace

from botpy.http import BotHttp, Route


class FakeResponse:
    def __init__(self, payload):
        self.status = 200
        self.headers = {"content-type": "application/json"}
        self.request_info = SimpleNamespace(
            url="https://sandbox.api.sgroup.qq.com/v2/users/fake/messages")
        self._payload = payload

    async def json(self):
        return self._payload


class FakeCM:
    """模拟 aiohttp 请求的异步上下文管理器。"""

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
    """假传输：按 script 依次动作；记录每次 POST 的 payload。"""

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
        return FakeCM(response=FakeResponse(action[1]))

    async def close(self):
        return None


async def main():
    version = metadata.version("qq-botpy")
    print(f"installed qq-botpy: {version}")
    assert version == "1.2.1", f"expected 1.2.1, got {version}"

    http = object.__new__(BotHttp)
    http._session = FakeSession([
        ("reset", None),
        ("ok", {"id": "srv-t8-1", "timestamp": 1234567890}),
    ])
    http._headers = {}
    http.timeout = 5
    http.is_sandbox = True

    async def _noop_check():
        return None

    http.check_session = _noop_check

    payload = {"openid": "fake-openid", "content": "回复", "msg_type": 0,
               "msg_id": "FAKE-T8-001", "msg_seq": 1}
    result = await http.request(
        Route("POST", "/v2/users/{openid}/messages", openid="fake-openid"),
        json=payload)

    calls = http._session.calls
    print(f"实际 POST 次数: {len(calls)}")
    for i, call in enumerate(calls, 1):
        j = call["json"]
        print(f"  第 {i} 次: msg_id={j['msg_id']} msg_seq={j['msg_seq']}")
    print(f"上层收到的返回值: {result!r}")
    http._session = None  # 避免 __del__ 触碰假 session

    assert len(calls) == 2, f"expected 2 POSTs, got {len(calls)}"
    assert calls[0]["json"]["msg_seq"] == calls[1]["json"]["msg_seq"] == 1, \
        "retry must reuse the same msg_seq"
    assert calls[0]["json"]["msg_id"] == calls[1]["json"]["msg_id"] \
        == "FAKE-T8-001", "retry must reuse the same msg_id"
    assert result is None, \
        f"caller must get None (SDK drops the retry receipt), got {result!r}"
    print("PASS T8: SDK 重试丢回执已复现——第二次成功发往腾讯，"
          "上层仍拿到 None")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
