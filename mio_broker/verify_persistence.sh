#!/bin/bash
# disk-persistence 专项验证: 落盘 -> kill -9 -> 重启 -> 数据还在
set -u
cd /home/trade/mqtt_lab/mio_broker
BIN=./target/release/mqtt_mio_broker
STATE=/tmp/mqtt_state_verify.bin
PORT=11899
ADDR=127.0.0.1:$PORT
BLOG=/tmp/mqtt_verify_broker.log

echo "[1/6] 启动 broker (state=$STATE)"
pkill -f "mqtt_mio_broker 127.0.0.1:$PORT" 2>/dev/null
rm -f $STATE $BLOG
MQTT_STATE_FILE=$STATE $BIN $ADDR > $BLOG 2>&1 &
BPID=$!
sleep 1
grep -q "listening" $BLOG && echo "  broker up pid=$BPID" || { echo "  BROKER FAILED"; cat $BLOG; exit 1; }

echo "[2/6] 发布 retain 消息 + 建立持久会话"
python3 - "$ADDR" <<'EOF'
import socket, sys, time
addr = sys.argv[1]; host, port = addr.split(':'); port = int(port)
def pkt(b):
    s = socket.create_connection((host, port)); s.sendall(b); return s
# CONNECT c1 clean_session=0 (持久会话) \x04\x00
c = pkt(b'\x10\x0e\x00\x04MQTT\x04\x00\x00\x3c\x00\x02c1')
s = c.recv(4); assert s[0] == 0x20, f"no CONNACK: {s!r}"
# SUBSCRIBE a/b QoS1 (pid=1)
c.sendall(b'\x82\x08\x00\x01\x00\x03a/b\x01'); time.sleep(0.3); c.recv(100)
# PUBLISH retain a/b "hi"
c.sendall(b'\x31\x07\x00\x03a/bhi'); time.sleep(0.3)
c.close()
print("  retain a/b + session c1 set up (clean=0)")
EOF

echo "[3/6] kill -9 broker"
kill -9 $BPID 2>/dev/null; sleep 0.5
echo "  killed pid=$BPID"

echo "[4/6] 重启 broker (同一 state 文件)"
MQTT_STATE_FILE=$STATE $BIN $ADDR > $BLOG 2>&1 &
BPID2=$!
sleep 1
grep -q "listening" $BLOG && echo "  broker2 up pid=$BPID2" || { echo "  RESTART FAILED"; cat $BLOG; exit 1; }
grep -i "loaded\|state" $BLOG | tail -2

echo "[5/6] 验证 retain 恢复"
python3 - "$ADDR" <<'EOF'
import socket, sys, time
addr = sys.argv[1]; host, port = addr.split(':'); port = int(port)
s = socket.create_connection((host, port))
s.sendall(b'\x10\x0e\x00\x04MQTT\x04\x02\x00\x3c\x00\x02x1')
s.recv(4)
s.sendall(b'\x82\x08\x00\x01\x00\x03a/b\x00')
time.sleep(0.5)
s.settimeout(1)
data = b''
try:
    while True:
        chunk = s.recv(200)
        if not chunk: break
        data += chunk
        if len(data) > 300: break
except socket.timeout:
    pass
assert b'\x31' in data and b'a/b' in data and b'hi' in data, f"retained NOT delivered: {data!r}"
print("  retained delivered after restart OK")
EOF

echo "[6/6] 清理"
kill -9 $BPID2 2>/dev/null
rm -f $STATE $BLOG
echo "ALL PERSISTENCE CHECKS PASSED"
