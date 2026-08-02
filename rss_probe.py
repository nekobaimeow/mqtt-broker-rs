#!/usr/bin/env python3
"""RSS decomposition: baseline vs N connections. Usage: rss_probe.py <port> <name> <n>"""
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
    s.recv(4)
    return s

def subscribe(s):
    t = b"mem/#"
    body = struct.pack(">H", 1) + struct.pack(">H", len(t)) + t + b"\x00"
    s.sendall(b"\x82" + enc_rem(len(body)) + body)
    time.sleep(0.02)

def proc_stats(procname):
    pids = subprocess.run(["pgrep", "-f", procname], capture_output=True, text=True).stdout.split()
    st = {"VmRSS": 0, "RssAnon": 0, "RssFile": 0, "ShmemPss": 0, "VmSize": 0}
    for pid in pids:
        try:
            with open(f"/proc/{pid}/status") as f:
                for l in f:
                    k = l.split(":")[0]
                    if k in st:
                        st[k] += int(l.split()[1])
        except FileNotFoundError:
            pass
    return st

port = int(sys.argv[1]); name = sys.argv[2]; n = int(sys.argv[3])
base = proc_stats(name)
conns = []
for i in range(n):
    c = connect(f"p-{i}", port)
    subscribe(c)
    conns.append(c)
time.sleep(1.0)
peak = proc_stats(name)
print(f"[{name}] baseline: RSS {base['VmRSS']/1024:.1f}MB (anon {base['RssAnon']/1024:.1f}MB, file {base['RssFile']/1024:.1f}MB, shmem {base['ShmemPss']/1024:.1f}MB) VmSize {base['VmSize']/1024:.0f}MB")
print(f"[{name}] +{n} conns: RSS {peak['VmRSS']/1024:.1f}MB (anon {peak['RssAnon']/1024:.1f}MB, file {peak['RssFile']/1024:.1f}MB, shmem {peak['ShmemPss']/1024:.1f}MB) VmSize {peak['VmSize']/1024:.0f}MB")
d = {k: peak[k]-base[k] for k in base}
print(f"[{name}] delta: RSS {d['VmRSS']/1024:.1f}MB ({d['VmRSS']/n:.1f}kB/conn) anon {d['RssAnon']/n:.1f}kB/conn shmem {d['ShmemPss']/n:.1f}kB/conn")
