# QQ 桥 SQLite 修复：改动摘要、测试结果、迁移与回滚

状态：已于 2026-11-19 14:05 切生产，用户 QQ 测试消息确认只收到一次，
切换已通过。生产现跑 SQLite 版；旧版文件与旧任务配置在
`backup-20261119-1403/`（含 `daemon.py.legacy` 与旧 cron 定义）。

## 背景

- 已确认风险（2026-11-19 假消息复现）：`outbox.jsonl` 整份读→await→
  整份重写，worker 在 await 期间追加的回复会被重写丢弃；`poller-seen.json`
  在 worker 写 outbox 时就标记，被丢的回复下一轮直接跳过。
- 附加发现：botpy 的 `BotHttp.request` 在超时、断线重试耗尽时返回 `None`
  且不抛异常，旧 `_send_one` 只看有没有异常，`None` 就当发出成功
  （超时当成功）。

## 改动摘要

本次改动的文件（仓库根目录与 `tests/`）：

- `store.py`（新增，SQLite 层）：`bridge.db`，WAL 模式；写事务一律
  `BEGIN IMMEDIATE`。三张表：
  - `inbound`：入站消息，`status` 为 pending / queued / done / missed。
  - `outbox`：出站回复，`msg_id UNIQUE`，`status` 为 pending / claimed /
    sent / failed / needs_check。
  - `seq_state`：每条原消息的下一个 `msg_seq`，在认领事务里分配并持久化。
- `daemon.py`（改）：入站写 `inbound` 表（`INSERT OR IGNORE` 去重）；
  出站改为 `send_loop`：启动时先把上个进程遗留的 `claimed` 行转
  `needs_check` 并告警，每 5 秒 `claim_next` 逐条认领、事务外调 API。
  发送分类见下。`QQBOT_DB_PATH` 可覆盖数据库路径。
- `enqueue_reply.py`（新增，worker 入队 CLI）：`INSERT 出站行 + inbound
  改 queued` 在同一个事务里；重复运行返回 `duplicate`（退出码 2），
  不会多写。退出码 0 已入队，1 出错。
- `list_pending.py`（新增，worker 巡检 CLI）：输出 `pending` 入站消息。
- `migrate.py`（新增）：`import`（旧文件→库）与 `export`（库→旧文件，
  供回滚）。
- `tests/test_sqlite_fix.py`、`tests/crash_child.py`：验收测试。

行为变化（相对旧版）：

1. 入站：daemon 不再写 `queue.jsonl` / `seen.json`；worker 不再读
   `queue.jsonl` / `poller-seen.json`，改读 `list_pending.py`。
2. 原消息状态：pending → queued（入队事务内）→ done（确认发出后）。
   只有真正发出才算处理完。
3. 发送分类（以 botpy 源码为准）：
   - 明确成功回执 → `sent` + 原消息 `done`。botpy 原样返回服务端
     JSON，发送成功的回执是含新消息 `id` 的 dict；只有含非空 `id`
     的 dict 才算成功，空 dict、错误 dict、非 dict 一律不误标成功。
   - 腾讯明确拒绝（401 / 403 / 404 / 405 / 429，服务端已应答并拒绝）
     → `failed`（终态，不自动重试），记录错误原文；回执 dict 含非零
     错误码（code / err_code / errcode）也按明确拒绝处理。
   - 返回 `None` / 超时 / 断线 / 其他异常 / 无 id 无错误码的回执
     → `needs_check` + 写 `needs-check.log` 告警，不自动重发。
   - `ServerError`（500 / 504 等）按结果不明处理，因为不能证明消息未送达。
   - 进程退出时遗留的 `claimed` 行，启动时转 `needs_check` 并告警。
   - SDK 重试：botpy 的 `BotHttp.request` 只在 `ConnectionResetError`
     时重试（最多 2 次），用同一个 route + 同一个 JSON payload（同一
     msg_id + msg_seq）；超时不重试、直接返回 `None`；HTTP 错误直接抛，
     不重试。所以应用层不重发时，SDK 也不会换新 seq 重发；SDK 的重试
     若撞上“同一 msg_id + msg_seq 重复发送会失败”，会以异常形式返回，
     按结果不明处理，不自动补发。
   - **SDK 重试丢回执（botpy 1.2.1，已实测，见 T8）**：重试那行是
     `await self.request(route, retry_time + 1, **kwargs)`，前面没有
     `return`——重试拿到的回执被丢弃，外层函数落到底返回 `None`。
     也就是说第二次请求即使发成功了，调用方仍拿到 `None`。所以
     `None` 绝不能当成功：它可能意味着“重试已成功送达，但回执丢了”。
     新版把 `None` 归为 needs_check、不自动重发，正是针对这个行为。
4. 失败不再无限重试：旧版 `failed` 每 5 秒无限重试（每次烧一个新 seq）；
   新版 `failed` / `needs_check` 均为终态，需人工处理。
5. `seq` 在认领事务里分配并持久化；`needs_check` 的行保留原 `seq`，
   不自动换新 `seq` 重发。

## 测试结果（假消息，8/8 + 回滚演练通过）

测试：`tests/test_sqlite_fix.py`（venv python，临时目录，模拟 BotAPI，
不碰生产、不连网关），2026-11-19 13:xx 实测；回滚演练：
`tests/test_rollback_drill.py`：

| 编号 | 验收项 | 结果 |
| --- | --- | --- |
| T1 | A 发送挂起时入队 B | B 保留，随后恰好发 1 次；A 发 1 次 |
| T2 | 同一入站消息被两个 worker 同时读到 | 一个 enqueued、一个 duplicate；出站表只有 1 行 |
| T3a | 落库事务提交前退出（子进程 SIGKILL） | 无出站行、原消息仍 pending，重新入队成功，不漏处理 |
| T3b | 落库事务提交后重复运行 | 返回 duplicate，出站表仍只有 1 行 |
| T4a | 腾讯已收到、本地没记下回执（botpy 返回 None） | needs_check + 告警；重启后 0 次重发，seq 未变 |
| T4b | 腾讯明确拒绝（ForbiddenError） | failed + 错误原文；重启后 0 次重发 |
| T5 | 真实发送中断：模拟服务已收到，本地写 sent 前杀掉 daemon 进程 | 中断前 claimed（seq=1）、腾讯侧已收到 1 次；重启后转 needs_check + 告警，0 次重发，seq 不变 |
| T6 | 成功回执判定 | 含 id 的 dict 才 sent；`{}` / 错误 dict / 字符串 / code=0 无 id 均不误标成功 |
| T7 | 回滚演练：新版处理 3 条（sent / failed / needs_check）后回滚到旧版 | export 的 outbox.jsonl 为空，R2+R3 在 held-back-export.json；旧版跑两轮 0 次发送，queue 完好；数据库留档 `tests/archive/rollback-drill-*.db`，未删除 |
| T8 | SDK 重试丢回执（真实 BotHttp + 假传输，qq-botpy 1.2.1） | 第一次连接重置、第二次成功：实际 POST 2 次，两次 msg_seq 均为 1；上层收到 `None`（重试的回执被 SDK 丢弃） |
| T9 | seen-only 快照：queue 2 条、seen 3 个 id | import 报告 seen_only_warnings=1，该 ID 记 legacy、无待发记录；export 后 seen.json 保留该 ID，queue.jsonl 仍为原 2 条，poller-seen 为 legacy-not-verified，outbox 为空 |
| T10 | 回滚演练补旧 worker：旧 worker 对 sent/failed/needs_check 0 入队，新假消息旧版正常回复 | 旧 worker 第一轮 0 入队；新假消息恰好入队 1 次；旧 daemon 恰好发 1 次（seq=1），W1/W2/W3 0 次重发 |
| T11 | 首次连接重置、SDK 重试收到重复发送错误（HTTP 400） | SDK 层抛 ServerError，POST 2 次、两次 seq 均为 1；daemon 层 needs_check（非 failed），重启 0 次补发，seq 不变 |

说明：T7 只跑了旧 daemon，没跑旧回复 worker（旧 worker 是定时任务里
的 agent 指令，不是脚本）；T10 按其正文逐条转录补上。T11 的重复发送
错误以 HTTP 400 形式返回——botpy 对非 200 一律抛异常，不走
classify_send_result 的通用错误码规则，所以不会被标为 failed。

生产快照 seen / queue 显式比对（2026-11-19 13:xx 实测）：seen 10 个、
queue 10 个，只在 seen 出现：无，只在 queue 出现：无。当前快照没有
seen-only ID；若未来快照出现，迁移时只报告、不补发（见下）。

迁移工具验证（生产快照副本，`/tmp/migrate-test2`）：`import` 得到
inbound legacy 10 / pending 0，warnings 0，seq_state 10；
`export` 回导后 queue msg_id、seen、seq 与原件一致，outbox 为空，
poller-seen 10 条（disposition 均为 legacy-not-verified）。

## 切换前核对（生产当前状态，2026-11-19 13:xx 实测）

- daemon 在跑；`queue.jsonl` 10 条；`outbox.jsonl` 0 字节；
  `seen.json` / `poller-seen.json` 各 10 条；`seq.json` 10 条全为 2
  （各发过 1 次，无重试史）。
- 切换时 `outbox.jsonl` 必须为空：若有待发 job，先等旧 daemon 发完；
  非空也可迁移（`import` 按 pending 导入），但要先核对。

## 迁移步骤（切生产时按顺序做，不可并行）

0. 先暂停写入方，再做最终快照（防快照后又有新写入丢失）：
   a. 停用回复轮询 `qq-bot-reply`（cron.update 改为停用，不删除），
      等正在跑的一轮结束（最多 90 秒）。
   b. 停旧 daemon：`echo 0 > hidden_files/enabled`，等保活不再拉起，
      确认 `pgrep` 无 daemon.py 进程。
   c. 确认无在途发送：`outbox.jsonl` 为空，且隔 10 秒再读一次仍为空；
      `daemon.log` 最近无正在发送的记录。旧 daemon 是读完文件才
      await 发送、发完才重写文件，进程已退出且文件为空即无在途。
1. 备份：`cp -r hidden_files hidden_files.bak.<时间>`；
   `cp daemon.py daemon.py.legacy`；另存当前 `qq-bot-reply` 的 cron
   定义（`crons/minutely/qq-bot-reply__interval@1m.md`）一份——
   迁移时要改它的正文，回滚时要恢复为旧版 worker 流程；
   记录当前 daemon pid 与 queue / outbox 状态。
2. 导入：`migrate.py import --src hidden_files --db hidden_files/bridge.db`；
   核对输出：inbound legacy 应为 10、pending 0、warnings 0；seq_state 10。
   历史一律标 legacy（旧版已处理、送达未核实、不自动重放），不当作
   送达证据。导入前先比对 seen.json 与 queue.jsonl：旧版先写 seen
   再入队，崩在中间会留下只在 seen 出现的 ID——这种 ID 只报告
   （summary 的 seen_only_warnings + warnings），不补发、不重入队；
   当前快照比对结果为 0 个。
3. 换文件：把仓库根目录的 `daemon.py`、`store.py`、
   `enqueue_reply.py`、`list_pending.py`、`migrate.py` 拷到目标目录。
4. 更新轮询任务 `qq-bot-reply` 正文为新 worker 流程（见下），并重新启用。
5. 启动：`echo 1 > hidden_files/enabled`；保活 60 秒内拉起新 daemon；
   核对 `daemon.log` 出现 `gateway ready`；用一条测试消息端到端验证
   （入队→`list_pending`→`enqueue_reply`→daemon 发出→库里 sent+done）。
6. 观察一轮：检查 `bridge.db` counts（无 pending 积压、无 needs_check），
   `needs-check.log` 为空。
7. 旧文件保留不动，作为回滚依据；GitHub 仓库随后同步更新（另一步，
   不在切换内自动做）。

新 worker 流程（`qq-bot-reply` 正文草稿）：

1. 运行 `list_pending.py` 取 pending 入站消息。
2. 有新消息：核对 owner；生成回复；调 `enqueue_reply.py --msg-id ...
   --scene ... --openid ... --text ...` 入队（返回 duplicate 说明
   另一轮已处理，跳过）。
3. 无新消息：安静；同时巡检 `needs-check.log`，非空时向主聊天报告一次。
4. 其余纪律不变（被动窗口、主动额度、超时不补发）。

## 回滚步骤

1. 停新 daemon：`echo 0 > enabled`；确认进程退出。
2. 导出新库里的增量：`migrate.py export --db hidden_files/bridge.db
   --dst /tmp/rollback-export`；先看 `held-back-export.json`——
   claimed / failed / needs_check 的行都在里面，不会进 `outbox.jsonl`，
   先人工核对再定。
3. 恢复：用 export 的五个文件直接覆盖恢复后的 `hidden_files`
   （它们已包含全部历史 + 切换窗口内的新消息）；把
   `daemon.py.legacy` 拷回 `daemon.py`。注意导出的 `poller-seen.json`
   是近似导出（只保 msg_id 集合语义），回滚后旧 worker 只按 msg_id
   去重，语义不受影响。
4. 把 `qq-bot-reply` 正文恢复为备份里的旧版 worker 流程
   （cron.update），再恢复旧 daemon：`echo 1 > enabled`；保活拉起；
   核对 `gateway ready`；发一条测试消息验证。
5. 数据库留档，不删除：把 `bridge.db`（及 `-wal` / `-shm`，如有）
   拷到 `tests/archive/` 存档（该目录不在版本控制内）；不要直接删库——回滚后若
   发现问题，还能从库里查每条记录的终态。`migrate import` 遇到
   已存在的 DB 会拒绝，所以留档的库不会被下次迁移误用。

## 切换窗口风险

- 切换窗口（旧 daemon 停到新 daemon 起之间）**可能漏收，未验证腾讯补投**：
  网关断开期间用户发来的消息暂存在腾讯侧，尚无证据表明腾讯会在
  机器人重连后补投。窗口内切完、被动回复窗口（C2C 60 分钟 / 群 5 分钟）
  未过的消息可正常回；窗口外按旧纪律记 missed，不补发。
- 绝不能新旧 daemon 同时跑：两个 botpy 客户端同 AppID 抢网关会话，
  且各自读自己的出站队列。迁移步骤 0、5 的顺序就是防这个。
- 历史消息不会重新入队：`import` 一律标 legacy（旧版已处理、送达
  未核实、不自动重放）；回滚 `export` 也保留 msg_id 集合。
