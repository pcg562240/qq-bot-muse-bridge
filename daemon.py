#!/usr/bin/env python3
"""QQ 官方 Bot 桥接 daemon（botpy，常驻网关连接）— SQLite 版。

职责：
- 连 QQ 网关收 C2C 私聊与群 @ 消息，写入 SQLite 入站表。
- 每 5 秒从 SQLite 出站表逐条认领待发回复，经 BotAPI 发出。
- 不直接回消息。回复正文由 Muse 的定时任务经 enqueue_reply.py
  写进出站表（与原消息标记已处理在同一个事务里）。

相对旧版（queue.jsonl / outbox.jsonl / seen.json / seq.json）修掉的：
1. outbox 整份读→await→整份重写：worker 在 await 期间追加的回复
   会被重写丢弃（2026-11-19 假消息复现确认）。现为 SQLite 表，
   worker INSERT（msg_id 唯一键），daemon 短事务逐条认领，不再重写。
2. poller-seen.json 在 worker 写 outbox 时就标记，早于 daemon 实际
   发出；被丢的回复下一轮直接跳过。现为同一事务：INSERT 出站行 +
   inbound 状态改为 queued；原消息只有真正发出后才标记 done。
3. 旧版把 botpy 超时当成功：BotHttp.request 在超时、断线重试耗尽时
   返回 None（不抛异常），而旧 _send_one 只看有没有异常，None 就当
   发出成功。现为：只有收到明确成功回执才标记 sent；返回 None、超时、
   断线 → needs_check + 告警，不自动重发；腾讯明确拒绝（401/403/
   404/405/429，服务端已应答并拒绝）→ failed，另行分类；
   ServerError（500/504 等）按结果不明处理，因为不能证明消息未送达。

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
- 同一 msg_id + msg_seq 重复发送会失败，seq 在认领事务里分配并
  持久化（store.seq_state），needs_check 的行保留原 seq，不自动换
  新 seq 重发。

状态：hidden_files/bridge.db（SQLite，WAL）+ daemon.log +
needs-check.log（待核对告警，轮询任务应巡检此文件）。
数据库路径可用 QQBOT_DB_PATH 环境变量覆盖。
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import aiohttp

# botpy 建 ClientSession 时不读环境代理，必须强制 trust_env=True。
_orig_session_init = aiohttp.ClientSession.__init__


def _patched_session_init(self, *args, **kwargs):
    kwargs.setdefault("trust_env", True)
    _orig_session_init(self, *args, **kwargs)


aiohttp.ClientSession.__init__ = _patched_session_init

import botpy  # noqa: E402
from botpy.errors import (  # noqa: E402
    AuthenticationFailedError,
    ForbiddenError,
    MethodNotAllowedError,
    NotFoundError,
    SequenceNumberError,
)
from botpy.message import C2CMessage, GroupMessage, Message  # noqa: E402

BASE = Path(__file__).resolve().parent
HIDDEN = BASE / "hidden_files"
DB = HIDDEN / "bridge.db"
LOG = HIDDEN / "daemon.log"
NEEDS_CHECK_LOG = HIDDEN / "needs-check.log"

sys.path.insert(0, str(BASE))
import store  # noqa: E402

# 腾讯明确拒绝：服务端已应答并拒绝（botpy 的 _handle_response 收到
# 错误状态才抛这些异常）。注意 ServerError（500/504 等）不在此列：
# 不能证明消息未送达，按结果不明处理。
DEFINITE_REJECTION = (
    AuthenticationFailedError,  # 401
    NotFoundError,              # 404
    MethodNotAllowedError,      # 405
    ForbiddenError,             # 403
    SequenceNumberError,        # 429
)

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


def _error_code(payload):
    """回执里的错误码（code / err_code / errcode），非零才算。"""
    for key in ("code", "err_code", "errcode"):
        value = payload.get(key)
        if isinstance(value, int) and value != 0:
            return value
        if isinstance(value, str) and value not in ("", "0"):
            return value
    return None


def classify_send_result(result):
    """判定发送回执。返回 (kind, detail)：
    - ('sent', id)：明确成功。botpy 原样返回服务端 JSON；发送成功
      的回执是含新消息 id 的 dict（botpy 的 Message 类型含 id）。
      只看“返回了 dict”不够：空 dict、错误 dict 都不是成功。
    - ('rejected', detail)：dict 含非零错误码：服务端已应答并拒绝。
    - ('unknown', detail)：其余一切（None、空 dict、无 id 无错误码、
      非 dict）：无法证明送达。
    """
    if result is None:
        return ("unknown", "botpy 返回 None：请求超时或断线重试耗尽，结果不明")
    if not isinstance(result, dict):
        return ("unknown", f"回执类型异常：{type(result).__name__}，无法确认送达")
    sent_id = result.get("id")
    if isinstance(sent_id, str) and sent_id:
        return ("sent", sent_id)
    code = _error_code(result)
    if code is not None:
        return ("rejected", f"腾讯返回错误码 {code}：{result.get('message', '')}")
    return ("unknown", f"回执无 id、无错误码：{result!r}，无法确认送达")


class Bridge(botpy.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        db_path = os.environ.get("QQBOT_DB_PATH") or str(DB)
        self.db = store.connect(db_path)
        # 发送循环只起一个：网关重连会再次触发 on_ready，没有守卫
        # 就会起第二个循环，第二个循环开头的 reconcile 会把第一个
        # 循环正在发送的 claimed 行误当成旧进程遗留记录转 needs_check。
        self._send_task = None
        # 旧进程遗留的 claimed 行只在进程启动时恢复一次。
        self._reconciled = False

    def _enqueue(self, scene, msg_id, openid, author_id, content, raw):
        if store.record_inbound(self.db, msg_id, scene, openid, content):
            log.info("queued %s %s from %s: %s", scene, msg_id, openid, content[:80])
            return True
        return False

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

    def _alert_needs_check(self, msg_id, reason):
        """结果不明：记日志 + 写 needs-check.log，供轮询任务巡检告警。"""
        line = f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')} {msg_id} {reason}"
        log.warning("NEEDS_CHECK %s", line)
        try:
            with NEEDS_CHECK_LOG.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as exc:
            log.warning("needs-check log write failed: %s", exc)

    async def _send_one(self, row):
        row_id = row["id"]
        msg_id = row["msg_id"]
        scene = row["scene"]
        seq = row["seq"]
        text = row["text"]
        try:
            if scene == "c2c":
                result = await self.api.post_c2c_message(
                    openid=row["openid"], content=text, msg_type=0,
                    msg_id=msg_id, msg_seq=seq)
            elif scene == "group":
                result = await self.api.post_group_message(
                    group_openid=row["openid"], content=text, msg_type=0,
                    msg_id=msg_id, msg_seq=seq)
            else:
                store.mark_failed(self.db, row_id, f"unknown scene: {scene}")
                log.warning("send failed %s %s: unknown scene", scene, msg_id)
                return
        except DEFINITE_REJECTION as exc:
            # 腾讯明确拒绝：服务端已应答并拒绝，终态，不自动重试。
            store.mark_failed(self.db, row_id, f"{type(exc).__name__}: {exc}")
            log.warning("send rejected %s %s seq=%s: %s", scene, msg_id, seq, exc)
            return
        except Exception as exc:
            # 超时、断线、其他异常：结果不明，不自动重发。
            # 注意 asyncio.CancelledError 是 BaseException，不会被这里
            # 捕获：进程退出时行保持 claimed，启动时 reconcile 处理。
            store.mark_needs_check(self.db, row_id, f"{type(exc).__name__}: {exc}")
            self._alert_needs_check(
                msg_id, f"发送异常，结果不明：{type(exc).__name__}: {exc}")
            return
        kind, detail = classify_send_result(result)
        if kind == "sent":
            store.mark_sent(self.db, row_id, msg_id)
            log.info("sent %s %s seq=%s id=%s", scene, msg_id, seq, detail)
        elif kind == "rejected":
            store.mark_failed(self.db, row_id, detail)
            log.warning("send rejected %s %s seq=%s: %s", scene, msg_id, seq, detail)
        else:
            store.mark_needs_check(self.db, row_id, detail)
            self._alert_needs_check(msg_id, detail)

    async def send_loop(self):
        # 启动时先处理上个进程遗留的 claimed 行：认领后、回执前的任何
        # 时刻退出都会留下 claimed 行，一律转 needs_check 并告警，
        # 不自动重发。只做一次：循环重启（或 on_ready 重复触发）时
        # 不再跑，否则会把正在发送的 claimed 行误当成遗留记录。
        # 用 getattr：测试里有用 object.__new__ 绕开 __init__ 构造
        # Bridge 的旧用法，属性缺失时按未恢复处理，不应崩。
        if not getattr(self, "_reconciled", False):
            self._reconciled = True
            stale = store.reconcile_claimed(self.db)
            for msg_id in stale:
                self._alert_needs_check(msg_id, "daemon 重启时仍处于 claimed 状态，发送结果不明")
        while True:
            await asyncio.sleep(5)
            row = store.claim_next(self.db)
            if row is None:
                continue
            await self._send_one(row)

    async def on_ready(self):
        log.info("gateway ready, listening for C2C and group @ messages")
        # 只起一个发送循环：循环还活着时重复 READY 不再起新任务；
        # 循环已退出（异常结束）时才重起。getattr 同上，兼容绕开
        # __init__ 的构造方式。
        task = getattr(self, "_send_task", None)
        if task is None or task.done():
            self._send_task = asyncio.create_task(self.send_loop())


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
