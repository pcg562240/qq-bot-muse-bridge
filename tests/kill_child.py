#!/usr/bin/env python3
"""T5 子进程：跑新 daemon 的 send_loop，模拟发送中断后被父进程杀掉。

用法：kill_child.py <workdir> <mode>
mode=hang：fake 在记录 server_received 后挂起（模拟腾讯已收到、回执未回），
  每次调用同步追加写进 calls.jsonl，并 touch started.marker。
mode=ok：fake 立即返回成功回执（模拟重启后的 daemon）。
父进程用 started.marker 确认发送已在途，再 SIGKILL。
"""
import asyncio
import importlib.util
import json
import os
import sys

workdir, mode = sys.argv[1], sys.argv[2]
hidden = os.path.join(workdir, "hidden_files")
os.makedirs(hidden, exist_ok=True)
sys.path.insert(0, workdir)
spec = importlib.util.spec_from_file_location(
    "daemon_under_test", os.path.join(workdir, "daemon.py"))
mod = importlib.util.module_from_spec(spec)
sys.modules["daemon_under_test"] = mod
spec.loader.exec_module(mod)


class FakeApi:
    def __init__(self):
        self.calls_path = os.path.join(hidden, "calls.jsonl")
        self.marker = os.path.join(hidden, "started.marker")

    def _record(self, msg_id, msg_seq):
        with open(self.calls_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"msg_id": msg_id, "seq": msg_seq},
                               ensure_ascii=False) + "\n")

    async def post_c2c_message(self, openid, content, msg_type=0,
                               msg_id=None, msg_seq=None):
        self._record(msg_id, msg_seq)
        with open(self.marker, "w", encoding="utf-8") as f:
            f.write("started")
        if mode == "hang":
            await asyncio.Event().wait()  # 永远挂起，等父进程杀掉
        return {"id": "fake-sent-id", "timestamp": 0}

    async def post_group_message(self, group_openid, content, msg_type=0,
                                 msg_id=None, msg_seq=None):
        return await self.post_c2c_message(
            group_openid, content, msg_type, msg_id, msg_seq)


async def main():
    bridge = object.__new__(mod.Bridge)
    bridge.db = mod.store.connect(os.path.join(hidden, "bridge.db"))
    bridge.api = FakeApi()
    await bridge.send_loop()


asyncio.run(main())
