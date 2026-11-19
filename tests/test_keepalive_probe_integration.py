#!/usr/bin/env python3
"""保活 + 探针离线联调 T16（不碰生产）。

用保活 hook 的实际调用方式跑 examples/keepalive-hook.sh：
hook 运行时每 60 秒调一次脚本，脚本 source $HATCH_HOOK_RUNTIME
得到 silent()/wake()；wake 就是用户看得到的告警渠道。

联调环境全部在临时目录里搭一套生产同款布局：
  <tmp>/daemon.py（假 daemon，只 sleep；cmdline 含该路径，供 pgrep 认）
  <tmp>/venv/bin/python -> 当前解释器
  <tmp>/hidden_files/enabled = "1"
  <tmp>/hidden_files/daemon.log（无凭据错误行）
  <tmp>/hidden_files/bridge.db（按用例准备）
  <tmp>/probe_send_stall.py（仓库里的探针原样拷入）
  <tmp>/keepalive-hook.sh（仓库里的 hook 脚本原样拷入）
  stub 的 HATCH_HOOK_RUNTIME：silent()/wake() 只记到 hook.log
QQ_D 用环境变量指到临时目录（脚本里 QQ_D="${QQ_D:-生产路径}"，
生产运行时未设置，走默认路径，行为不变）。

覆盖：
  T16-A SEND_STALL 能送到 wake；保活退出 0、不中断、不重启
  T16-B PROBE_ERROR 能送到 wake（与停摆区分开）；保活退出 0、不重启
  T16-C 健康时不 wake，保活正常走完
  T16-D 探针文件不存在时保活静默跳过（未部署/已停用的状态）
"""
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPODIR = os.path.dirname(HERE)

DUMMY_DAEMON = "#!/usr/bin/env python3\nimport time\ntime.sleep(600)\n"
STUB_RUNTIME = """silent() { echo "SILENT: $*" >> "$HOOK_LOG"; }
wake() { echo "WAKE: $*" >> "$HOOK_LOG"; }
"""


def load_store(workdir):
    path = os.path.join(workdir, "store.py")
    shutil.copyfile(os.path.join(REPODIR, "store.py"), path)
    spec = importlib.util.spec_from_file_location("store_t16", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["store_t16"] = mod
    spec.loader.exec_module(mod)
    return mod


def build_env(tmp):
    """搭一套生产同款布局，返回各路径；不启动任何真 daemon。"""
    qd = os.path.join(tmp, "qq-bot-muse")
    hidden = os.path.join(qd, "hidden_files")
    os.makedirs(os.path.join(qd, "venv", "bin"))
    os.makedirs(hidden)
    os.symlink(sys.executable, os.path.join(qd, "venv", "bin", "python"))
    with open(os.path.join(qd, "daemon.py"), "w") as f:
        f.write(DUMMY_DAEMON)
    with open(os.path.join(hidden, "enabled"), "w") as f:
        f.write("1")
    with open(os.path.join(hidden, "daemon.log"), "w") as f:
        f.write("2026-11-19T07:00:00+0000 starting, sandbox=True\n")
        f.write("2026-11-19T07:00:01+0000 gateway ready, "
                "listening for C2C and group @ messages\n")
    shutil.copyfile(os.path.join(REPODIR, "probe_send_stall.py"),
                    os.path.join(qd, "probe_send_stall.py"))
    hook = os.path.join(tmp, "keepalive-hook.sh")
    shutil.copyfile(os.path.join(REPODIR, "examples", "keepalive-hook.sh"),
                    hook)
    os.chmod(hook, 0o755)
    stub = os.path.join(tmp, "hook-runtime.sh")
    with open(stub, "w") as f:
        f.write(STUB_RUNTIME)
    hook_log = os.path.join(tmp, "hook.log")
    open(hook_log, "w").close()
    return {"qd": qd, "hidden": hidden, "hook": hook, "stub": stub,
            "hook_log": hook_log,
            "db": os.path.join(hidden, "bridge.db")}


def start_dummy_daemon(qd):
    """启动假 daemon 进程（只 sleep，cmdline 含该路径）。"""
    proc = subprocess.Popen(
        [sys.executable, os.path.join(qd, "daemon.py")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        if proc.poll() is None and _daemon_pids(qd):
            return proc
        time.sleep(0.1)
    raise RuntimeError("dummy daemon did not appear in pgrep")


def _daemon_pids(qd):
    out = subprocess.run(["pgrep", "-f", os.path.join(qd, "daemon.py")],
                         capture_output=True, text=True)
    if out.returncode != 0:
        return []
    return [int(x) for x in out.stdout.split()]


def run_hook(env_paths):
    """跑一次保活 hook（一个 60 秒 tick），返回 (exit_code, hook 日志行)。"""
    env = dict(os.environ)
    env["QQ_D"] = env_paths["qd"]
    env["HATCH_HOOK_RUNTIME"] = env_paths["stub"]
    env["HATCH_HOOK_DRY_RUN"] = "0"
    env["HOOK_LOG"] = env_paths["hook_log"]
    proc = subprocess.run(["bash", env_paths["hook"]], env=env,
                          capture_output=True, text=True, timeout=30)
    with open(env_paths["hook_log"], encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if ln]
    return proc.returncode, lines, proc.stdout, proc.stderr


def hook_lines(env_paths, kind):
    with open(env_paths["hook_log"], encoding="utf-8") as f:
        return [ln for ln in f.read().splitlines() if ln.startswith(kind)]


def check(name, cond, detail=""):
    print("%s %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        raise AssertionError(name + " " + detail)


def test_a_send_stall_reaches_wake():
    """T16-A：停摆时 SEND_STALL 送到 wake；保活退出 0，不中断、不重启。"""
    with tempfile.TemporaryDirectory() as tmp:
        envp = build_env(tmp)
        store = load_store(tmp)
        conn = store.connect(envp["db"])
        store.record_inbound(conn, "K-1", "c2c", "OWNER", "test")
        store.enqueue_reply(conn, "K-1", "c2c", "OWNER", "reply K-1")
        now = time.time()
        conn.execute("UPDATE outbox SET created_at=?, updated_at=?",
                     (now - 200, now - 200))
        conn.commit()
        conn.close()
        dummy = start_dummy_daemon(envp["qd"])
        try:
            # tick 1：探针首轮信号（exit 0），不 wake；保活正常走完。
            rc1, _, out1, err1 = run_hook(envp)
            check("T16-A tick1 保活退出 0",
                  rc1 == 0, "rc=%s out=%s err=%s" % (rc1, out1, err1))
            check("T16-A tick1 无 wake",
                  hook_lines(envp, "WAKE:") == [])
            check("T16-A tick1 有 healthy",
                  any("healthy" in ln for ln in hook_lines(envp, "SILENT:")))
            # tick 2：探针确认告警（exit 1）-> wake 带 SEND_STALL。
            rc2, _, out2, err2 = run_hook(envp)
            check("T16-A tick2 保活退出 0（探针 exit 1 未中断保活）",
                  rc2 == 0, "rc=%s out=%s err=%s" % (rc2, out2, err2))
            wakes = hook_lines(envp, "WAKE:")
            check("T16-A 恰好 1 次 wake", len(wakes) == 1, str(wakes))
            check("T16-A wake 带 SEND_STALL",
                  "SEND_STALL" in wakes[0], wakes[0])
            check("T16-A wake 写明待发数量",
                  "pending 1 条" in wakes[0], wakes[0])
            check("T16-A wake 写明最老等待",
                  "已等" in wakes[0] and "秒" in wakes[0], wakes[0])
            # 没有触发重启：假 daemon 还是原来那个 pid，且只有一个。
            check("T16-A 假 daemon 未重启",
                  dummy.poll() is None and _daemon_pids(envp["qd"]) == [dummy.pid],
                  "poll=%s pids=%s" % (dummy.poll(), _daemon_pids(envp["qd"])))
            check("T16-A tick2 仍有 healthy（wake 后保活继续走完）",
                  any("healthy" in ln for ln in hook_lines(envp, "SILENT:")))
        finally:
            dummy.terminate()
            dummy.wait(timeout=10)


def test_b_probe_error_reaches_wake():
    """T16-B：库读不到时 PROBE_ERROR 送到 wake（与停摆区分开）；
    保活退出 0，不重启；同一故障不重复 wake。"""
    with tempfile.TemporaryDirectory() as tmp:
        envp = build_env(tmp)
        # bridge.db 不存在：探针读库失败。
        dummy = start_dummy_daemon(envp["qd"])
        try:
            rc1, _, out1, err1 = run_hook(envp)
            check("T16-B tick1 保活退出 0（探针 exit 2 未中断保活）",
                  rc1 == 0, "rc=%s out=%s err=%s" % (rc1, out1, err1))
            wakes = hook_lines(envp, "WAKE:")
            check("T16-B 恰好 1 次 wake", len(wakes) == 1, str(wakes))
            check("T16-B wake 带 PROBE_ERROR",
                  "PROBE_ERROR" in wakes[0], wakes[0])
            check("T16-B wake 说明不是队列为空",
                  "不是“队列为空”" in wakes[0], wakes[0])
            check("T16-B wake 没有 SEND_STALL",
                  "SEND_STALL" not in wakes[0], wakes[0])
            rc2, _, _, _ = run_hook(envp)
            check("T16-B tick2 保活退出 0", rc2 == 0)
            check("T16-B 同故障不重复 wake",
                  len(hook_lines(envp, "WAKE:")) == 1)
            check("T16-B 假 daemon 未重启",
                  dummy.poll() is None and _daemon_pids(envp["qd"]) == [dummy.pid])
        finally:
            dummy.terminate()
            dummy.wait(timeout=10)


def test_c_healthy_no_wake():
    """T16-C：健康时不 wake，保活正常走完，不重启。"""
    with tempfile.TemporaryDirectory() as tmp:
        envp = build_env(tmp)
        store = load_store(tmp)
        conn = store.connect(envp["db"])
        conn.close()
        dummy = start_dummy_daemon(envp["qd"])
        try:
            rc, _, out, err = run_hook(envp)
            check("T16-C 保活退出 0",
                  rc == 0, "rc=%s out=%s err=%s" % (rc, out, err))
            check("T16-C 无 wake", hook_lines(envp, "WAKE:") == [])
            check("T16-C 有 healthy",
                  any("healthy" in ln for ln in hook_lines(envp, "SILENT:")))
            check("T16-C 假 daemon 未重启",
                  dummy.poll() is None and _daemon_pids(envp["qd"]) == [dummy.pid])
        finally:
            dummy.terminate()
            dummy.wait(timeout=10)


def test_d_probe_absent_skipped():
    """T16-D：探针文件不存在时保活静默跳过（未部署/已停用的状态），
    其余逻辑照常，不 wake、不重启。"""
    with tempfile.TemporaryDirectory() as tmp:
        envp = build_env(tmp)
        os.remove(os.path.join(envp["qd"], "probe_send_stall.py"))
        dummy = start_dummy_daemon(envp["qd"])
        try:
            rc, _, out, err = run_hook(envp)
            check("T16-D 保活退出 0",
                  rc == 0, "rc=%s out=%s err=%s" % (rc, out, err))
            check("T16-D 无 wake", hook_lines(envp, "WAKE:") == [])
            check("T16-D 有 healthy",
                  any("healthy" in ln for ln in hook_lines(envp, "SILENT:")))
            check("T16-D 假 daemon 未重启",
                  dummy.poll() is None and _daemon_pids(envp["qd"]) == [dummy.pid])
        finally:
            dummy.terminate()
            dummy.wait(timeout=10)


def main():
    tests = [test_a_send_stall_reaches_wake,
             test_b_probe_error_reaches_wake,
             test_c_healthy_no_wake,
             test_d_probe_absent_skipped]
    for fn in tests:
        fn()
    print("结果：T16 通过（4 项：停摆告警进 wake/探针故障进 wake/健康无 wake/探针缺失跳过，均不中断保活、不重启）")


if __name__ == "__main__":
    main()
