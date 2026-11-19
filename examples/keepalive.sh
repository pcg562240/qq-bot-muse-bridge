#!/usr/bin/env bash
# keepalive.sh — 托管 daemon.py 的最小示例（无 Muse hook 运行时依赖）。
# 每 60 秒检查一次 daemon 是否存活，挂了自动拉起；凭据错误时不反复拉起。
# 用法：nohup ./examples/keepalive.sh >> hidden_files/keepalive.log 2>&1 &
# 停用：touch hidden_files/disabled（存在时不再拉起）；恢复：删掉该文件。
set -uo pipefail

BRIDGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DAEMON="$BRIDGE_DIR/daemon.py"
PY="${PY:-$BRIDGE_DIR/venv/bin/python}"
[ -x "$PY" ] || PY="$(command -v python3)"
LOG_F="$BRIDGE_DIR/hidden_files/daemon.log"
DISABLED_F="$BRIDGE_DIR/hidden_files/disabled"

daemon_alive() {
    local p cmd state
    for p in $(pgrep -f "daemon\.py" 2>/dev/null || true); do
        [ -r "/proc/$p/cmdline" ] || continue
        cmd=$(tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null) || continue
        case "$cmd" in *"$DAEMON"*) ;; *) continue ;; esac
        state=$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null) || continue
        [ "$state" = "Z" ] && continue
        return 0
    done
    return 1
}

while true; do
    if [ -f "$DISABLED_F" ]; then
        sleep 60
        continue
    fi
    if ! daemon_alive; then
        # 凭据错误时 daemon 立即退出；最近日志里有 token/appid 失败就不拉起。
        auth_lines="$(tail -30 "$LOG_F" 2>/dev/null | grep -E "appid invalid|获取token失败" || true)"
        if [ -n "$auth_lines" ]; then
            echo "$(date '+%F %T') auth failed, not restarting" >&2
        else
            echo "$(date '+%F %T') daemon down, restarting" >&2
            cd "$BRIDGE_DIR" && nohup "$PY" "$DAEMON" >> "$LOG_F" 2>&1 &
        fi
    fi
    sleep 60
done
