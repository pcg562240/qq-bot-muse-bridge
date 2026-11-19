#!/usr/bin/env bash
# keepalive-hook.sh — QQ daemon 保活 hook（含发送停摆探针）。
#
# 本文件 = ~/hooks/scripts/qq-daemon-keepalive.sh（生产正在跑的版本）
# 加上两处改动，其余一字未动：
#   1. QQ_D 可用环境变量覆盖（默认生产路径），只为离线联调；
#      生产 hook 运行时 QQ_D 未设置，走默认路径，行为不变。
#   2. daemon 存活分支里加发送停摆探针（只告警，不重启）。
#
# 部署 = 把本文件拷到 ~/hooks/scripts/qq-daemon-keepalive.sh，并把
# probe_send_stall.py 拷到生产 goal 目录（$QQ_D/probe_send_stall.py）。
# 停用 = 删掉 $QQ_D/probe_send_stall.py（[ -f "$PROBE" ] 守卫会静默
# 跳过探针），或把 hook 脚本恢复成改动前的版本；两种都不用重启。
# qq-daemon-keepalive — 每 60 秒检查 QQ 桥接 daemon 是否存活。
# daemon 是 botpy 常驻网关连接（收 C2C 私聊与群 @ 消息，写 queue.jsonl；
# 每 5 秒扫 outbox.jsonl 发回复）。只在 hidden_files/enabled 为 1 时管理，
# 未到凭据、未启用时安静跳过，不拉起、不报错。
# 凭据由 daemon 自己从 hidden_files/credentials.env 读，本脚本不碰凭据。
# 遵循 muse-mac-tunnel-keepalive 的写法：健康 silent，拉起/失败 wake。
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

QQ_D="${QQ_D:-/home/hatch/workspace/goals/qq-bot-muse}"
DAEMON="$QQ_D/daemon.py"
PY="$QQ_D/venv/bin/python"
ENABLED_F="$QQ_D/hidden_files/enabled"
LOG_F="$QQ_D/hidden_files/daemon.log"

DRY="${HATCH_HOOK_DRY_RUN:-0}"

daemon_alive() {
    local p cmd state
    for p in $(pgrep -f "daemon\.py" 2>/dev/null || true); do
        [ -r "/proc/$p/cmdline" ] || continue
        cmd=$(tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null) || continue
        case "$cmd" in *"$DAEMON"*) ;; *) continue ;; esac
        state=$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null) || continue
        [ "$state" = "Z" ] && continue
        echo "$p"
        return 0
    done
    return 1
}

if [ "$(cat "$ENABLED_F" 2>/dev/null || echo 0)" != "1" ]; then
    silent "qq daemon not enabled"
    exit 0
fi

if daemon_alive >/dev/null; then
    rm -f "$QQ_D/hidden_files/auth-failed-flag"
    # 发送停摆探针（只告警，不重启）：进程活着不代表发送循环在跑，
    # 循环死了网关还连着就不会有新 READY 来重起它。探针只读查库：
    # pending 超 60 秒 / claimed 超 120 秒、连续两轮同行卡住才告警。
    # probe 退出码：0 无事/首轮信号/已报过；1 本轮新告警；2 本轮新报的探针故障。
    # 注意 set -e：必须用 || 接住非零退出，否则探针一报警就会中断保活。
    # 探针文件不存在时静默跳过（未部署或已停用），保活其余逻辑不受影响。
    # daemon 死了不跑探针：走下面的重启分支，停摆期间积压是预期内的，
    # 重启后的 reconcile 会处理遗留的 claimed 行。
    PROBE="$QQ_D/probe_send_stall.py"
    if [ -f "$PROBE" ]; then
        probe_rc=0
        probe_out="$("$PY" "$PROBE" --db "$QQ_D/hidden_files/bridge.db" --state-dir "$QQ_D/hidden_files/probe-state" 2>&1)" || probe_rc=$?
        if [ "$probe_rc" -eq 1 ]; then
            wake "keepalive: $probe_out" '{"send_stall":true}'
        elif [ "$probe_rc" -eq 2 ]; then
            wake "keepalive: $probe_out" '{"probe_error":true}'
        fi
    fi
    silent "qq daemon healthy"
    exit 0
fi

# 凭据错误时 daemon 立即退出；无守卫会每 60 秒拉起一次并反复 wake。
# 最近日志里出现 token/appid 失败时不拉起，只在新的失败出现时报一次。
if [ -f "$LOG_F" ]; then
    # 注意 set -e：grep 无匹配时退出码为 1，必须用 || true 兜住，
    # 否则脚本会在这里静默退出（2026-11-19 实测踩过这个坑）。
    auth_lines="$(tail -30 "$LOG_F" 2>/dev/null | grep -E "appid invalid|获取token失败" || true)"
    if [ -n "$auth_lines" ]; then
        auth_sig="$(printf '%s' "$auth_lines" | sha256sum | cut -d' ' -f1)"
        flag="$QQ_D/hidden_files/auth-failed-flag"
        if [ "$(cat "$flag" 2>/dev/null)" != "$auth_sig" ]; then
            printf '%s' "$auth_sig" > "$flag"
            wake "keepalive: qq daemon auth failed (bad credentials?), not restarting" '{"failed":true}'
        else
            silent "qq daemon auth failure already reported"
        fi
        exit 0
    fi
fi

if [ "$DRY" = "1" ]; then
    wake "keepalive dry-run: qq daemon down, would restart (no action taken)" '{"dry_run":true}'
    exit 0
fi

cd "$QQ_D" && nohup "$PY" "$DAEMON" >> "$LOG_F" 2>&1 &
sleep 2
if daemon_alive >/dev/null; then
    wake "keepalive: qq daemon restarted" '{}'
else
    wake "keepalive: qq-daemon-restart-failed" '{"failed":true}'
fi
