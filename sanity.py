#!/usr/bin/env python3
"""Quick sanity: subscribe on one conn, publish 5 msgs on another, verify delivery."""
import socket, struct, threading, time

def enc_rem(n):
    out = b""
    while True:
        b = n % 128
        n //= 128
        if n: b |= 0x80
        out += bytes([b])
        if not n: return out

def connect(cid):
    s = socket.create_connection(("127.0.0.1", 11883))
    body = b"\x00\x04MQTT\x04\x02\x00\x3c" + struct.pack(">H", len(cid)) + cid.encode()
    s.sendall(b"\x10" + enc_rem(len(body)) + body)
    # CONNACK
    hdr = s.recv(4)
    assert hdr[0] == 0x20, f"bad connack: {hdr.hex()}"
    return s

def subscribe(s, topic):
    body = struct.pack(">H", 1) + struct.pack(">H", len(topic)) + topic.encode() + b"\x00"
    s.sendall(b"\x82" + enc_rem(len(body)) + body)
    time.sleep(0.2)
    # drain suback
    s.settimeout(0.5)
    try:
        d = s.recv(100)
        print(f"  suback recv: {d.hex()}")
    except socket.timeout:
        print("  !! no suback")
    s.settimeout(None)

def publish(s, topic, payload):
    body = bytes([len(topic)]) + topic.encode() + payload
    s.sendall(b"\x30" + enc_rem(len(body)) + body)

sub = connect("py-sub")
subscribe(sub, "bench/#")

pub = connect("py-pub")
for i in range(5):
    publish(pub, "bench/x", b"hello-%d" % i)
print("  published 5 msgs")

sub.settimeout(2.0)
got = 0
end = time.time() + 2
while time.time() < end:
    try:
        d = sub.recv(4096)
        if not d: break
        got += 1
        print(f"  sub received packet {got}: {d[:20].hex()}...")
    except socket.timeout:
        break
print(f"RESULT: received {got}/5")
