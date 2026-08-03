#!/usr/bin/env python3
"""jcode 流水线监督器（黑心资本家自动化版）:
每轮执行:
1. 查 Windows 侧 jcode 进程 + 任务日志
2. 若进程活着 → 汇报 in_progress（含日志尾巴）
3. 若进程死了 + 日志有完成标记 → 验证 git diff → 标记 done → 派下一单
4. 若进程死了 + 无完成标记 → 标记 failed → 记录原因 → 派下一单（或重试）

用法: python3 supervise_jcode.py [--next]   # --next 强制派下一单
"""
import json, os, subprocess, sys, time, datetime

WIN = "trade@172.17.32.1"
PS = "123qazasd"
REPO = r"C:\Users\trade\mqtt-broker-rs"
BACKLOG = "/home/trade/mqtt_lab/jcode_backlog.json"
CMDS = "/tmp"  # 生成的 cmd 文件目录

def sh(cmd, timeout=60):
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
    return r.stdout.strip(), r.returncode

def win(cmd, timeout=60):
    out, rc = sh(f"sshpass -p '{PS}' ssh -o StrictHostKeyChecking=no {WIN} '{cmd}'", timeout)
    return out, rc

def load_backlog():
    with open(BACKLOG) as f:
        return json.load(f)

def save_backlog(b):
    with open(BACKLOG, "w") as f:
        json.dump(b, f, ensure_ascii=False, indent=2)

def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def jcode_running():
    out, _ = win("tasklist | findstr /i jcode")
    return "jcode" in out

def git_head():
    out, _ = win(f"cd /d {REPO} && git log --oneline -1")
    return out.splitlines()[0] if out else "?"

def git_diff_stat():
    out, _ = win(f"cd /d {REPO} && git diff --stat")
    return out

def launch_task(task):
    """scp cmd + 后台 SSH 长连接启动（唯一可靠进程分离方案）"""
    cid = task["id"]
    cmd_file = f"{CMDS}/jcode_task_{cid}.cmd"
    if not os.path.exists(cmd_file):
        return False, f"cmd file missing: {cmd_file}"
    # scp（remote 路径加单引号保护反斜杠）
    _, rc = sh(f"sshpass -p '{PS}' scp -o StrictHostKeyChecking=no {cmd_file} '{WIN}:{REPO}/jcode_task_{cid}.cmd'")
    if rc != 0:
        return False, "scp failed"
    # 后台 SSH 长连接（ServerAlive 保活），由 supervise 调用方以 background 方式持有
    # 统一用正斜杠路径：Windows cmd 与 SSH 均接受，避免 bash 吃反斜杠
    repo_f = REPO.replace("\\", "/")
    log = f"{repo_f}/jcode_task_{cid}.log"
    cmd = f"sshpass -p '{PS}' ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10000 {WIN} 'cmd /c {repo_f}/jcode_task_{cid}.cmd > {log} 2>&1'"
    # 写一个 launcher 脚本，由 cron 的 background 进程持有
    launcher = f"/tmp/launch_{cid}.sh"
    with open(launcher, "w") as f:
        f.write("#!/bin/bash\n" + cmd + "\n")
    os.chmod(launcher, 0o755)
    # 用 setsid 让 SSH 脱离控制终端，nohup 防 SIGHUP
    sh(f"setsid nohup {launcher} > /tmp/launch_{cid}.out 2>&1 &")
    time.sleep(8)
    if not jcode_running():
        return False, "jcode not running after launch"
    return True, "launched via setsid nohup ssh"

def check_log(task_id):
    """返回 (exists, size, last_mod, tail)"""
    log = f"{REPO}/jcode_task_{task_id}.log"
    out, _ = win(f"powershell -Command \"if (Test-Path '{log}') {{ $f=Get-Item '{log}'; Write-Output \\\"$($f.Length)|$($f.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))\\\" }} else {{ Write-Output 'MISSING' }}\"")
    if "MISSING" in out:
        return False, 0, "", ""
    parts = out.split("|")
    try:
        size = int(parts[0]); mtime = parts[1] if len(parts) > 1 else ""
    except (ValueError, IndexError):
        size, mtime = 0, ""
    tail, _ = win(f"powershell -Command \"Get-Content '{log}' -Tail 5 -ErrorAction SilentlyContinue\"")
    return True, size, mtime, tail

def done_marker(task_id):
    """日志里找完成标记（jcode 退出 + 无 error 字样）"""
    log = f"{REPO}/jcode_task_{task_id}.log"
    tail, _ = win(f"powershell -Command \"Get-Content '{log}' -Tail 20 -ErrorAction SilentlyContinue\"")
    has_error = "400 Bad Request" in tail or "Error:" in tail or "panic" in tail
    return tail, has_error

def main():
    force_next = "--next" in sys.argv
    b = load_backlog()
    pending = [t for t in b if t["status"] == "pending"]
    in_prog = [t for t in b if t["status"] == "in_progress"]
    running = jcode_running()

    lines = []
    lines.append(f"[{now()}] jcode running: {running}")

    if running:
        # 找 in_progress 任务，报日志状态
        if in_prog:
            t = in_prog[0]
            exists, size, mtime, tail = check_log(t["id"])
            lines.append(f"  in_progress: {t['id']} (log {size}B @ {mtime})")
            if tail:
                lines.append("  tail: " + tail.replace("\n", " | ")[-300:])
        else:
            lines.append("  running but no in_progress task in backlog!")
        print("\n".join(lines))
        return

    # jcode 没在跑
    if in_prog:
        t = in_prog[0]
        head_before = t.get("started_head", "")
        head_now = git_head()
        tail, has_error = done_marker(t["id"])
        head_changed = head_now != head_before
        # 完成判定 v2: HEAD 变了(有提交) + 编译冒烟通过; 日志有 error 直接失败
        if has_error:
            t["status"] = "failed"
            t["finished_at"] = now()
            t["note"] = "error in log: " + tail.replace("\n", " ")[-200:]
            lines.append(f"  FAILED: {t['id']} {t['note']}")
        elif head_changed:
            # 编译冒烟: cargo build --release, 失败 = 假货
            build_out, build_rc = win(f"cd /d {REPO} && cargo build --release 2>&1 | findstr /i error")
            if build_rc != 0:
                t["status"] = "failed"
                t["finished_at"] = now()
                t["note"] = f"build smoke FAILED: {build_out[-300:]}"
                lines.append(f"  FAILED(build): {t['id']} {t['note']}")
            else:
                t["status"] = "done"
                t["finished_at"] = now()
                t["note"] = f"head {head_before} -> {head_now} + build OK"
                lines.append(f"  DONE: {t['id']} {t['note']}")
        else:
            t["status"] = "failed"
            t["finished_at"] = now()
            t["note"] = "no commit made, log tail: " + tail.replace("\n", " ")[-200:]
            lines.append(f"  FAILED(no-commit): {t['id']} {t['note']}")
        save_backlog(b)
        # 重新载入
        b = load_backlog()

    pending = [t for t in b if t["status"] == "pending"]
    if pending:
        nxt = pending[0]
        nxt["status"] = "in_progress"
        nxt["started_at"] = now()
        nxt["started_head"] = git_head()
        ok, msg = launch_task(nxt)
        if ok:
            lines.append(f"  LAUNCHED next: {nxt['id']} ({nxt['desc']})")
        else:
            nxt["status"] = "failed"
            nxt["note"] = f"launch failed: {msg}"
            lines.append(f"  LAUNCH FAILED: {nxt['id']} {msg}")
        save_backlog(b)
    else:
        lines.append("  backlog empty - ALL DONE 🎉")

    print("\n".join(lines))

if __name__ == "__main__":
    main()
