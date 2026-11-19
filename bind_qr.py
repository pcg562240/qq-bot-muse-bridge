#!/usr/bin/env python3
"""QQ 官方 Connector 扫码绑定（q.qq.com/lite，与 OpenClaw qqbot-cli 同款协议）。

流程：
  1. 生成 32 字节 AES-256 key（base64），POST /lite/create_bind_task 取 task_id。
  2. 二维码内容为 https://q.qq.com/qqbot/openclaw/connect.html?task_id=...
     用手机 QQ 扫码确认。
  3. 轮询 POST /lite/poll_bind_result，status=2 时返回 bot_appid、
     bot_encrypt_secret（base64: nonce[12] + 密文 + tag[16]，AES-256-GCM
     解密得 AppSecret）、user_openid。

用法：
  bind_qr.py create   # 建任务，写 hidden_files/bind_task.json（0600）与 bind_qr.png
  bind_qr.py poll     # 轮询至成功/过期/超时，成功则写 credentials.env（0600）
  bind_qr.py status   # 只查一次当前状态

注意：bind_task.json 里的 key 能解出 AppSecret，视同凭据，永不打印、
永不进聊天记录。AppSecret 本身也只写进 credentials.env，绝不打印。
"""

import base64
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import segno

BASE = Path(__file__).resolve().parent
HIDDEN = BASE / "hidden_files"
TASK_FILE = HIDDEN / "bind_task.json"
QR_FILE = HIDDEN / "bind_qr.png"
CRED_FILE = HIDDEN / "credentials.env"

LITE = "https://q.qq.com/lite"
CONNECT_URL = "https://q.qq.com/qqbot/openclaw/connect.html?task_id={task_id}&_wv=2&source=muse"


def post_json(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_task():
    return json.loads(TASK_FILE.read_text(encoding="utf-8"))


def cmd_create():
    HIDDEN.mkdir(parents=True, exist_ok=True)
    key = os.urandom(32)
    key_b64 = base64.b64encode(key).decode("ascii")
    resp = post_json(f"{LITE}/create_bind_task", {"key": key_b64})
    task_id = resp.get("task_id") or resp.get("taskId") or resp.get("data", {}).get("task_id")
    if not task_id:
        print("create_bind_task failed:", json.dumps(resp, ensure_ascii=False)[:300])
        return 1
    TASK_FILE.write_text(
        json.dumps({"task_id": task_id, "key": key_b64}, ensure_ascii=False),
        encoding="utf-8",
    )
    os.chmod(TASK_FILE, 0o600)
    url = CONNECT_URL.format(task_id=task_id)
    qr = segno.make(url)
    qr.save(str(QR_FILE), kind="png", scale=8, border=2)
    print("task_id:", task_id)
    print("qr_url:", url)
    print("qr_png:", QR_FILE)
    return 0


def poll_once(task_id):
    return post_json(f"{LITE}/poll_bind_result", {"task_id": task_id})


def decrypt_secret(key_b64, enc_b64):
    key = base64.b64decode(key_b64)
    raw = base64.b64decode(enc_b64)
    nonce, rest = raw[:12], raw[12:]
    ciphertext, tag = rest[:-16], rest[-16:]
    return AESGCM(key).decrypt(nonce, ciphertext + tag, None).decode("utf-8")


def cmd_poll(timeout=290, interval=5):
    task = load_task()
    task_id, key_b64 = task["task_id"], task["key"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = poll_once(task_id)
        except Exception as exc:
            # 后台跑时出口可能受限，单次网络失败只记一行并继续轮询，不直接崩掉
            print("poll network error, retrying:", type(exc).__name__, str(exc)[:120])
            time.sleep(interval)
            continue
        data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
        status = data.get("status")
        if status == 2:
            appid = data.get("bot_appid")
            enc = data.get("bot_encrypt_secret")
            user_openid = data.get("user_openid")
            if not appid or not enc:
                print("bind succeeded but response lacks credentials:",
                      json.dumps(resp, ensure_ascii=False)[:300])
                return 1
            secret = decrypt_secret(key_b64, enc)
            CRED_FILE.write_text(
                f"QQBOT_APPID={appid}\nQQBOT_SECRET={secret}\nQQBOT_SANDBOX=1\n",
                encoding="utf-8",
            )
            os.chmod(CRED_FILE, 0o600)
            if user_openid:
                (HIDDEN / "owner.json").write_text(
                    json.dumps({"user_openid": user_openid}, ensure_ascii=False),
                    encoding="utf-8",
                )
            print("bind ok")
            print("bot_appid:", appid)
            print("user_openid:", user_openid)
            print("credentials written to", CRED_FILE)
            return 0
        print("poll status:", json.dumps(resp, ensure_ascii=False)[:200])
        time.sleep(interval)
    print("poll timed out; QR may have expired, run `create` again for a fresh code")
    return 2


def cmd_status():
    task = load_task()
    resp = poll_once(task["task_id"])
    print(json.dumps(resp, ensure_ascii=False)[:500])
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    cmd = sys.argv[1]
    if cmd == "create":
        return cmd_create()
    if cmd == "poll":
        return cmd_poll()
    if cmd == "status":
        return cmd_status()
    print("unknown command:", cmd)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
