# qq-bot-muse-bridge

把 Muse（或任何能读写文件的 Agent）接进 QQ。

基于腾讯 **QQ 开放平台官方 Bot** + 官方 Python SDK `botpy`：扫码绑定登录，
常驻网关收 C2C 私聊与群 @ 消息；回复逻辑不在本仓库——daemon 只负责传输，
收到的消息写进 `hidden_files/bridge.db`（SQLite 入站表），你的回复程序
用 `enqueue_reply.py` 把回复写进出站表，daemon 每 5 秒认领一条发出。

不走个人号逆向协议（NapCat / Lagrange 之类），不挂个人 QQ，没有封号风险。

## 架构

```
手机 QQ ──> 腾讯网关 ──> daemon.py (botpy 常驻)
                              │ 入站写 bridge.db（inbound 表）
                              ▼
                        你的回复程序（Muse / LLM / 脚本）
                              │ 回复写 bridge.db（outbox 表）
                              ▼
                        daemon.py 每 5 秒认领一条发出
```

登录走腾讯官方 Connector Portal 扫码绑定流程（与 OpenClaw `qqbot-cli`
同款协议）：`bind_qr.py create` 建任务 → 手机 QQ 扫码 → `bind_qr.py poll`
解密拿 AppSecret，写进 `hidden_files/credentials.env`。

## 快速开始

```bash
# 0. 先在 q.qq.com 注册开发者、创建机器人（名字需审核；审核期间用沙箱）
#    功能配置里打开 C2C 私聊、群聊场景
pip install -r requirements.txt   # Python 3.10+

# 1. 扫码绑定（只需一次；登录态存 hidden_files/credentials.env，0600）
python3 bind_qr.py create         # 打印二维码链接与图片路径，手机 QQ 扫码
python3 bind_qr.py poll           # 等待确认，成功后写 credentials.env + owner.json

# 2. 起 daemon（默认沙箱网关）
python3 daemon.py

# 3. 回复程序：list_pending.py 取待处理消息，enqueue_reply.py 写回复
#    旧版文件队列示例见 examples/echo_reply.py（未随本次改动更新，仅供参考）；
#    生产可用任何 Agent 每分钟扫一轮
```

用 `examples/keepalive.sh` 托管 daemon：每 60 秒检查一次，挂了自动拉起，
凭据错误时不反复拉起。

## SQLite 队列协议

出站队列原来是 `hidden_files/outbox.jsonl`：daemon 整份读进内存、await
腾讯接口、再整份重写——worker 在 await 期间追加的回复会被重写丢弃，
且回复是否送达没有可靠记录。现改为 `hidden_files/bridge.db`（SQLite，
WAL 模式），见 `MIGRATION.md`：

- `inbound`：入站消息，`status` 为 pending / queued / done / missed /
  legacy（旧版已处理、送达未核实）。
- `outbox`：待发回复，`msg_id UNIQUE` 防重复排队，`status` 为 pending /
  claimed / sent / failed / needs_check。
- `seq_state`：每条原消息的下一个 `msg_seq`，在认领事务里分配并持久化。

回复流程：`list_pending.py` 取 pending 的新入站消息 → 生成回复 →
`enqueue_reply.py --msg-id ... --scene c2c|group --openid ... --text ...`
入队（入队与原消息标记 queued 在同一个事务里；重复运行返回
duplicate，不会多写）。daemon 每 5 秒从库里认领一条（短事务改
claimed 并分配 seq），事务外调腾讯 API：只有收到含新消息 `id` 的回执
才标 sent；腾讯明确拒绝（401/403/404/405/429）标 failed；返回 None、
超时、断线或结果不明标 needs_check 并写 `needs-check.log` 告警，不自动
重发。进程退出时遗留的 claimed 行，启动时转 needs_check。

旧版文件（`queue.jsonl` / `outbox.jsonl` / `seen.json` / `seq.json` /
`poller-seen.json`）与新版不兼容，迁移与回滚见 `MIGRATION.md`；旧版
文件队列示例 `examples/echo_reply.py` 未随本次改动更新。

## 回复限制（QQ 开放平台正式环境）

- C2C 私聊：收到消息后 **60 分钟内**可被动回复，最多 **5 条**
- 群聊：收到 @ 后 **5 分钟内**可被动回复，最多 **5 条**
- 主动消息：私聊每用户每月 **4 条**，以被动回复为主

## 沙箱与正式环境

默认 `QQBOT_SANDBOX=1` 走沙箱网关（无需审核，开发者 + 测试成员可用；
2026 年起沙箱不再支持群聊测试，只支持频道与私聊）。

机器人审核通过后，`hidden_files/credentials.env` 里设 `QQBOT_SANDBOX=0`
切正式环境。注意：IP 白名单是**按应用启用**的策略，不是所有机器人都要走；
先拿真实凭据实测，出现「接口访问源 IP 不在白名单」（code 11298 /
err_code 40023002）再填服务器实际出口 IP。出口 IP 会轮换时白名单填不了，
只能留在沙箱。

## 安全

- `hidden_files/credentials.env`、`hidden_files/bind_task.json` 是凭据，
  0600 权限，只存本地，已进 `.gitignore`，**绝不提交、绝不贴到聊天里**
- `QQBOT_APPID` 不是秘密，可明文存放；`QQBOT_SECRET` 绝不打印
- 只回复 `owner.json` 白名单里的人，其他人发来的消息只记录、不回复

## 已知坑（都已在代码里处理，注释有说明）

- botpy 的 aiohttp 不读环境代理：daemon 里 patch 了 `ClientSession`
  默认 `trust_env=True`，否则直连出口被拦截时网关与 API 全超时
- `C2CMessage.author` 只有 `user_openid`，`GroupMessage.author` 只有
  `member_openid`，都没有 `id`——用 `author.id` 会读到 None
- botpy import 时调过 `basicConfig` 且把自家 logger 的 `propagate` 设为
  False，只靠默认配置日志文件是空的；daemon 手动配了文件 handler，
  且不挂 StreamHandler 到 stdout（否则每行进文件两份）
- 长轮询断线、网关 1006 断开会重连，一般 10 秒内恢复

## License

MIT
