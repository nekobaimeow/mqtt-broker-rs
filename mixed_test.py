#!/usr/bin/env python3
"""Mixed test: Rust fast subscriber + Python slow subscriber (never reads) + 100k burst.
Shows whether a slow subscriber steals messages from a fast one."""
import socket, struct, time, sys, subprocess, os, signal

def enc_rem(n):
    out = b""
    while True:
        b = n % 128
        n //= 128
        if n: b |= 0x80
        out += bytes([b])
        if not n: return out

def connect(cid, port):
    s = socket.create_connection(("127.0.0.1", port))
    body = b"\x00\x04MQTT\x04\x02\x00\x3c" + struct.pack(">H", len(cid)) + cid.encode()
    s.sendall(b"\x10" + enc_rem(len(body)) + body)
    hdr = s.recv(4)
    assert hdr[0] == 0x20, f"bad connack: {hdr.hex()}"
    return s

def subscribe(s, topic):
    body = struct.pack(">H", 1) + struct.pack(">H", len(topic)) + topic.encode() + b"\x00"
    s.sendall(b"\x82" + enc_rem(len(body)) + body)
    time.sleep(0.2)

def publish(s, topic, payload):
    body = struct.pack(">H", len(topic)) + topic.encode() + payload
    s.sendall(b"\x30" + enc_rem(len(body)) + body)

port = int(sys.argv[1])
name = sys.argv[2]
N = 100_000

# start Rust fast subscriber in background
fast = subprocess.Popen(["./fast_sub", str(port), str(N)],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(1.0)  # let it subscribe

# slow subscriber: subscribes, never reads
slow = connect("py-slow", port)
subscribe(slow, "bench/#")

# publisher bursts N msgs
pub = connect("py-pub", port)
t0 = time.time()
for i in range(N):
    publish(pub, "bench/x", b"x" * 32)
t1 = time.time()
print(f"[{name}] published {N} msgs in {t1-t0:.2f}s")

# wait for fast subscriber to finish (its 3s read timeout covers the burst window)
try:
    out, _ = fast.communicate(timeout=15)
except subprocess.TimeoutExpired:
    fast.kill()
    out, _ = fast.communicate()
print(f"[{name}] fast subscriber: {out.strip().splitlines()[-1]}")
