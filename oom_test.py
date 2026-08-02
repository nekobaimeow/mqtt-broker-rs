#!/usr/bin/env python3
"""OOM stress: one slow subscriber (never reads), one fast subscriber (reads),
publisher blasts 100k msgs. Prints broker RSS and delivery stats."""
import socket, struct, time, threading, sys

def enc_rem(n):
    out = b""
    while True:
        b = n % 128
        n //= 128
        if n: b |= 0x80
        out += bytes([b])
        if not n: return out

def connect(cid):
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1] if len(sys.argv) > 1 else 11883)))
    body = b"\x00\x04MQTT\x04\x02\x00\x3c" + struct.pack(">H", len(cid)) + cid.encode()
    s.sendall(b"\x10" + enc_rem(len(body)) + body)
    hdr = s.recv(4)
    assert hdr[0] == 0x20, f"bad connack: {hdr.hex()}"
    return s

def subscribe(s, topic):
    body = struct.pack(">H", 1) + struct.pack(">H", len(topic)) + topic.encode() + b"\x00"
    s.sendall(b"\x82" + enc_rem(len(body)) + body)
    time.sleep(0.2)

def publish(s, topic, payload, qos=0):
    body = struct.pack(">H", len(topic)) + topic.encode() + payload
    s.sendall(b"\x30" + enc_rem(len(body)) + body)

def rss_of(procname):
    for line in open("/proc/loadavg"): pass
    import subprocess
    out = subprocess.run(["pgrep", "-f", procname], capture_output=True, text=True).stdout.split()
    if not out: return 0
    rss = 0
    for pid in out:
        try:
            with open(f"/proc/{pid}/status") as f:
                for l in f:
                    if l.startswith("VmRSS"):
                        rss += int(l.split()[1])
        except FileNotFoundError:
            pass
    return rss  # kB

addr_port = int(sys.argv[1]) if len(sys.argv) > 1 else 11883
name = sys.argv[2] if len(sys.argv) > 2 else "broker"

# slow subscriber: subscribes, never reads
slow = connect("slow-sub")
subscribe(slow, "bench/#")

# fast subscriber: reads in a thread
fast = connect("fast-sub")
subscribe(fast, "bench/#")
fast_got = 0
stop = threading.Event()
def fast_reader():
    global fast_got
    while not stop.is_set():
        try:
            d = fast.recv(65536)
            if not d: break
            fast_got += 1
        except socket.timeout:
            pass
fast.settimeout(1.0)
t = threading.Thread(target=fast_reader, daemon=True)
t.start()

pub = connect("pub")
N = 100000
t0 = time.time()
for i in range(N):
    publish(pub, "bench/x", b"x" * 32)
t1 = time.time()
print(f"[{name}] published {N} msgs in {t1-t0:.2f}s ({N/(t1-t0):.0f} msg/s)")
time.sleep(2)
print(f"[{name}] fast subscriber got ~{fast_got} msgs")
print(f"[{name}] broker RSS after burst: {rss_of(name)} kB")
stop.set()
