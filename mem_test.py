#!/usr/bin/env python3
"""Memory comparison: N idle connections (subscribed, no traffic) vs RSS.
Usage: mem_test.py <port> <name> <nconns>"""
import socket, struct, time, sys, subprocess

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
    assert hdr[0] == 0x20
    return s

def subscribe(s, topic):
    body = struct.pack(">H", 1) + struct.pack(">H", len(topic)) + topic.encode() + b"\x00"
    s.sendall(b"\x82" + enc_rem(len(body)) + body)
    time.sleep(0.02)

def rss_kb(procname):
    out = subprocess.run(["pgrep", "-f", procname], capture_output=True, text=True).stdout.split()
    rss = 0
    for pid in out:
        try:
            with open(f"/proc/{pid}/status") as f:
                for l in f:
                    if l.startswith("VmRSS"):
                        rss += int(l.split()[1])
                        break
        except FileNotFoundError:
            pass
    return rss

def vmsize_kb(procname):
    out = subprocess.run(["pgrep", "-f", procname], capture_output=True, text=True).stdout.split()
    vm = 0
    for pid in out:
        try:
            with open(f"/proc/{pid}/status") as f:
                for l in f:
                    if l.startswith("VmSize"):
                        vm += int(l.split()[1])
                        break
        except FileNotFoundError:
            pass
    return vm

port = int(sys.argv[1]); name = sys.argv[2]; n = int(sys.argv[3])
conns = []
for i in range(n):
    c = connect(f"idle-{i}", port)
    subscribe(c, "mem/#")
    conns.append(c)
time.sleep(1.0)  # let broker settle
rss = rss_kb(name)
vm = vmsize_kb(name)
per_conn = rss / n
print(f"[{name}] {n} idle connections: RSS {rss/1024:.1f} MB ({per_conn:.1f} kB/conn), VmSize {vm/1024:.1f} MB")
