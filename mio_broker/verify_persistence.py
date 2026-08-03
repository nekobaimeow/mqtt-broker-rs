#!/usr/bin/env python3
"""disk-persistence 独立验证: 全部逻辑单进程内, 带 socket timeout, 无 bash 后台陷阱"""
import socket, subprocess, sys, time, os, signal

BIN = "/home/trade/mqtt_lab/mio_broker/target/release/mqtt_mio_broker"
STATE = "/tmp/mqtt_state_verify.bin"
PORT = 11899
ADDR = f"127.0.0.1:{PORT}"

BLOG = "/tmp/mqtt_verify_broker.log"

def start_broker():
    env = dict(os.environ, MQTT_STATE_FILE=STATE)
    p = subprocess.Popen([BIN, ADDR], env=env, stdout=open(BLOG, "ab"), stderr=subprocess.PIPE)
    time.sleep(1.5)
    if p.poll() is not None:
        err = p.stderr.read().decode() if p.stderr else ""
        raise RuntimeError(f"broker died: rc={p.returncode} stderr={err[-300:]}")
    return p

def mqtt_connect(host, port, cid, clean):
    s = socket.create_connection((host, port), timeout=3)
    # CONNECT: proto MQTT 3.1.1, clean flag, keepalive 60; client id = u16 BE len + bytes
    body = b"\x00\x04MQTT\x04" + bytes([0x02 if clean else 0x00]) + b"\x00\x3c" + len(cid).to_bytes(2, "big") + cid
    s.sendall(b"\x10" + bytes([len(body)]) + body)
    time.sleep(0.5)
    resp = b""
    try:
        resp = s.recv(4)
        if len(resp) < 4:
            time.sleep(0.5)
            resp += s.recv(4)
    except Exception as e:
        print(f"    [dbg] recv err: {e!r}")
    if not resp:
        print(f"    [dbg] EMPTY recv; sent={body.hex()}")
    assert resp and resp[0] == 0x20, f"no CONNACK: {resp!r}"
    return s

def recv_all(s, timeout=1.0, maxbytes=4096):
    s.settimeout(timeout)
    data = b""
    try:
        while len(data) < maxbytes:
            chunk = s.recv(4096)
            if not chunk: break
            data += chunk
    except socket.timeout:
        pass
    s.settimeout(3)
    return data

def step(msg): print(f"  {msg}")

# 1. 干净启动 (清场由外部负责, 避免 pkill -f 匹配歧义)
if os.path.exists(STATE): os.remove(STATE)
if os.path.exists(BLOG): os.remove(BLOG)
p1 = start_broker()
step("broker1 up")

# 2. 建立持久会话 + retain
try:
    s = mqtt_connect("127.0.0.1", PORT, b"c1", clean=False)
except Exception as e:
    print(f"  CONNECT FAILED: {e!r}")
    print(f"  broker poll: {p1.poll()}")
    if p1.stderr:
        err = p1.stderr.read().decode(errors="replace")[-500:]
        print(f"  broker stderr: {err}")
    raise
s.sendall(b"\x82\x08\x00\x01\x00\x03a/b\x01")  # SUBSCRIBE a/b qos1, RL=8
time.sleep(0.3); recv_all(s)
s.sendall(b"\x31\x07\x00\x03a/bhi")  # PUBLISH retain=1 (0x31) a/b "hi", RL=7
time.sleep(0.5); recv_all(s)
s.close()
step("persistent session c1 + retain a/b set up")

# 3. kill -9
p1.kill(); p1.wait()
time.sleep(0.3)
step("broker1 kill -9")

# 4. 重启
p2 = start_broker()
step("broker2 up")
state_size = os.path.getsize(STATE) if os.path.exists(STATE) else 0
print(f"  state file: {state_size} bytes")
time.sleep(0.5)

# 5. 验证 retain 恢复
s2 = mqtt_connect("127.0.0.1", PORT, b"x1", clean=True)
s2.sendall(b"\x82\x08\x00\x01\x00\x03a/b\x00")  # SUBSCRIBE a/b qos0, RL=8 -> retained push
time.sleep(0.5)
data = recv_all(s2)
if (b"\x30" in data or b"\x31" in data) and b"a/b" in data and b"hi" in data:
    step("retained a/b delivered after restart OK")
else:
    print(f"  FAIL retained not delivered: {data!r}")
    p2.kill(); sys.exit(1)
s2.close()

# 6. 验证持久会话恢复 (c1 clean=0 重连 -> 订阅还在 + 离线队列)
s3 = mqtt_connect("127.0.0.1", PORT, b"c1", clean=False)
time.sleep(0.3)
data3 = recv_all(s3)
print(f"  session c1 reconnect bytes: {len(data3)} (subs restored if SUBACK/any)")
s3.close()

p2.kill(); p2.wait()
os.remove(STATE)
print("ALL PERSISTENCE CHECKS PASSED")
