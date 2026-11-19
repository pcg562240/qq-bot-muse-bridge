#!/usr/bin/env python3
"""QQ 官方 Bot 桥接 daemon（botpy，常驻网关连接）。

职责：
- 连 QQ 网关收 C2C 私聊与群 @ 消息，写入 hidden_files/queue.jsonl。
- 每 5 秒扫 hidden_files/outbox.jsonl，把待发回复经 BotAPI 发出。
- 不直接回消息。回复正文由 Muse 的定时任务生成后写入 outbox。

凭据：环境变量 QQBOT_APPID / QQBOT_SECRET；若环境变量缺失，则读
hidden_files/credentials.env（0600，仅这两行，永不打印）。
QQBOT_SECRET 允许填 Secure Vault 的 surrogate：daemon 原样传给
botpy，换 token 的 POST 到 bots.qq.com 时出口层把它换成真凭据。
QQBOT_APPID 不是秘密，可明文存放。

网络：本机直连出口被拦截，botpy 的 aiohttp 必须走环境代理。下面的
monkeypatch 把 ClientSession 默认 trust_env 改为 True，否则网关与
API 调用都会超时。实测：直连 bots.qq.com 超时，走环境代理返回
HTTP 200（2026-11-19）。

回复窗口（以 QQ 开放平台实际规则为准）：
- C2C 私聊：收到消息后 60 分钟内可被动回复，最多 5 条。
- 群聊：收到 @ 消息后 5 分钟内可被动回复，最多 5 条。
- 正式环境主动消息极严（私聊每用户每月 4 条），以被动回复为主。
- 同一 msg_id + msg_seq 重复发送会失败，seq 由本文件维护。

状态文件都在 hidden_files/ 下：queue.jsonl（入站队列）、
outbox.jsonl（待发回复）、seen.json（已收消息 id 去重）、
seq.json（每条 msg_id 的下一个 msg_seq）、daemon.log。
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import aiohttp

# botpy 建 ClientSession 时不读环境代理，必须强制 trust_env=True。
_orig_session_init = aiohttp.ClientSession.__init__


def _patched_session_init(self, *args, **kwargs):
    kwargs.setdefault("trust_env", True)
    _orig_session_init(self, *args, **kwargs)


aiohttp.ClientSession.__init__ = _patched_session_init

import botpy  # noqa: E402
from botpy.message import C2CMessage, GroupMessage, Message  # noqa: E402

BASE = Path(__file__).resolve().parent
HIDDEN = BASE / "hidden_files"
QUEUE = HIDDEN / "queue.jsonl"
OUTBOX = HIDDEN / "outbox.jsonl"
SEEN = HIDDEN / "seen.json"
SEQ = HIDDEN / "seq.json"
LOG = HIDDEN / "daemon.log"

# botpy 在 import 时调过一次 basicConfig（无 level，root 默认为 WARNING，
# 且其后 basicConfig 是 no-op），又把 "botpy" logger 的 propagate 设为
# False：只靠 basicConfig，文件日志是空的，我自己的 INFO 也会被丢掉
#（2026-11-19 实测 daemon.log 为空）。所以这里手动配：root 只挂文件
# handler；"botpy" logger 额外挂文件 handler（只挂文件），两边都设为
# INFO。注意不要再挂 StreamHandler 到 stdout：hook 是用
# `>> hidden_files/daemon.log 2>&1` 拉起 daemon 的，stdout 本就重定向
# 到同一个文件，挂了就会每行写两份（2026-11-19 实测：我的行全双份，
# botpy 的行单份）。
_file_handler = logging.FileHandler(LOG, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.root.handlers = [_file_handler]
logging.root.setLevel(logging.INFO)
_botpy_logger = logging.getLogger("botpy")
if _file_handler not in _botpy_logger.handlers:
    _botpy_logger.addHandler(_file_handler)
_botpy_logger.setLevel(logging.INFO)
_botpy_logger.propagate = False  # 否则记录会同时走 botpy logger 与 root，两份进文件
log = logging.getLogger("qq-daemon")


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def append_jsonl(path, item):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


class Bridge(botpy.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = set(load_json(SEEN, []))
        self.seq = load_json(SEQ, {})

    def _enqueue(self, scene, msg_id, openid, author_id, content, raw):
        if msg_id in self.seen:
            return False
        self.seen.add(msg_id)
        if len(self.seen) > 5000:
            self.seen = set(list(self.seen)[-5000:])
        save_json(SEEN, sorted(self.seen))
        append_jsonl(
            QUEUE,
            {
                "scene": scene,
                "msg_id": msg_id,
                "openid": openid,
                "author_id": author_id,
                "content": content,
                "raw": raw,
            },
        )
        log.info("queued %s %s from %s: %s", scene, msg_id, openid, content[:80])
        return True

    async def on_c2c_message_create(self, message: C2CMessage):
        # C2CMessage 的 author 只有 user_openid，没有 id（2026-11-19 实测：
        # 用 author.id 读到 None，导致 owner 校验失败、首轮没回）。
        author = message.author
        openid = getattr(author, "user_openid", None) or getattr(author, "id", None)
        self._enqueue("c2c", message.id, openid, openid, message.content or "",
                      {"id": message.id, "repr": repr(message)})

    async def on_group_at_message_create(self, message: GroupMessage):
        # GroupMessage 的 author 只有 member_openid，没有 id。
        group_openid = getattr(message, "group_openid", None)
        author = message.author
        author_id = getattr(author, "member_openid", None) or getattr(author, "id", None)
        self._enqueue("group", message.id, group_openid, author_id,
                      message.content or "", {"id": message.id, "repr": repr(message)})

    def _next_seq(self, msg_id):
        seq = int(self.seq.get(msg_id, 1))
        self.seq[msg_id] = seq + 1
        save_json(SEQ, self.seq)
        return seq

    async def _send_one(self, job):
        scene = job.get("scene")
        msg_id = job.get("msg_id")
        text = job.get("text", "")
        if not text or not msg_id:
            return True
        seq = self._next_seq(msg_id)
        try:
            if scene == "c2c":
                await self.api.post_c2c_message(
                    openid=job["openid"], content=text, msg_type=0,
                    msg_id=msg_id, msg_seq=seq)
            elif scene == "group":
                await self.api.post_group_message(
                    group_openid=job["openid"], content=text, msg_type=0,
                    msg_id=msg_id, msg_seq=seq)
            else:
                return True
        except Exception as exc:
            log.warning("send failed %s %s seq=%s: %s", scene, msg_id, seq, exc)
            return False
        log.info("sent %s %s seq=%s", scene, msg_id, seq)
        return True

    async def outbox_loop(self):
        while True:
            await asyncio.sleep(5)
            if not OUTBOX.exists():
                continue
            try:
                jobs = [json.loads(line) for line in
                        OUTBOX.read_text(encoding="utf-8").splitlines() if line.strip()]
            except Exception as exc:
                log.warning("outbox read failed: %s", exc)
                continue
            remaining = []
            for job in jobs:
                if job.get("status") == "sent":
                    continue
                ok = await self._send_one(job)
                if not ok:
                    job["status"] = "failed"
                    remaining.append(job)
            OUTBOX.write_text(
                "".join(json.dumps(j, ensure_ascii=False) + "\n" for j in remaining),
                encoding="utf-8")

    async def on_ready(self):
        log.info("gateway ready, listening for C2C and group @ messages")
        asyncio.create_task(self.outbox_loop())


def load_credentials():
    """从环境变量或 hidden_files/credentials.env 取凭据。

    文件格式为 QQBOT_APPID=... / QQBOT_SECRET=... 两行，不打印值。
    QQBOT_SANDBOX=1 时走沙箱网关（默认 1；本机出口 IP 轮换，
    正式环境 IP 白名单填不了，只能走沙箱）。
    """
    appid = os.environ.get("QQBOT_APPID", "")
    secret = os.environ.get("QQBOT_SECRET", "")
    sandbox = os.environ.get("QQBOT_SANDBOX", "1")
    if appid and secret:
        return appid, secret, sandbox != "0"
    cred_file = HIDDEN / "credentials.env"
    try:
        for line in cred_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "QQBOT_APPID" and not appid:
                appid = value.strip()
            elif key.strip() == "QQBOT_SECRET" and not secret:
                secret = value.strip()
            elif key.strip() == "QQBOT_SANDBOX":
                sandbox = value.strip()
    except FileNotFoundError:
        pass
    return appid, secret, sandbox != "0"


def main():
    HIDDEN.mkdir(parents=True, exist_ok=True)
    appid, secret, is_sandbox = load_credentials()
    if not appid or not secret:
        print("QQBOT_APPID / QQBOT_SECRET not set", file=sys.stderr)
        return 2
    intents = botpy.Intents(public_messages=True)
    # ext_handlers=False：不要 botpy 默认往 ./botpy.log 写一份（与 daemon.log
    # 重复，还会污染工作目录）；日志只走上面配好的 daemon.log 与控制台。
    client = Bridge(intents=intents, is_sandbox=is_sandbox, ext_handlers=False)
    log.info("starting, sandbox=%s", is_sandbox)
    client.run(appid=appid, secret=secret)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
