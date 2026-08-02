#!/usr/bin/env python3
"""Keep N conns alive, probe broker smaps for stack vs heap RSS breakdown."""
import socket, struct, time, sys, subprocess, re

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

port = int(sys.argv[1]); name = sys.argv[2]; n = int(sys.argv[3])
conns = []
for i in range(n):
    c = connect(f"p-{i}", port)
    subscribe(c)
    conns.append(c)
time.sleep(1.0)

pid = subprocess.run(["pgrep", "-x", name], capture_output=True, text=True).stdout.split()[0]
threads = 0
with open(f"/proc/{pid}/status") as f:
    for l in f:
        if l.startswith("Threads"): threads = int(l.split()[1])
        if l.startswith("VmRSS"): rss = int(l.split()[1])
        if l.startswith("RssAnon"): anon = int(l.split()[1])

stack_rss = 0; heap_rss = 0; stack_maps = 0
with open(f"/proc/{pid}/smaps") as f:
    cur_name = None
    for l in f:
        if re.match(r'^[0-9a-f]+-[0-9a-f]+', l):
            cur_name = l.split()[-1] if len(l.split()) > 5 else None
        elif l.startswith("Rss:") and cur_name:
            v = int(l.split()[1])
            if "stack" in cur_name:
                stack_rss += v; stack_maps += 1
            elif "heap" in cur_name:
                heap_rss += v
print(f"[{name}] {n} conns: RSS {rss/1024:.1f}MB, Threads {threads}, anon {anon/1024:.1f}MB")
print(f"  stacks: {stack_maps} maps, {stack_rss/1024:.2f}MB RSS ({stack_rss/n:.1f}kB/conn)")
print(f"  heap:   {heap_rss/1024:.2f}MB RSS ({heap_rss/n:.1f}kB/conn)")
print(f"  stack+heap = {stack_rss+heap_rss:.0f}kB, anon = {anon}kB, 其他匿名 = {(anon-stack_rss-heap_rss)/n:.1f}kB/conn")
time.sleep(30)  # keep conns alive
