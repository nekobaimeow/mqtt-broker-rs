#!/bin/bash
# 全量回归: 5 测试套件, broker 已跑在 11883 (QOS1_RETRY_MS 由 broker 启动时决定)
cd /home/trade/mqtt_lab/mio_broker
PASS=0; FAIL=0
for t in mqtt_qos1_test mqtt_qos2_retain_test mqtt_lwt_test mqtt_session_test mqtt_subsidx_test; do
  if [ ! -x target/release/$t ]; then
    rustc --edition 2021 -O $t.rs -o target/release/$t 2>&1 | head -5
  fi
  echo "=== $t ==="
  OUT=$(./target/release/$t 127.0.0.1:11883 2>&1)
  RC=$?
  if [ $RC -eq 0 ]; then
    echo "PASS ($RC)"; PASS=$((PASS+1))
  else
    echo "FAIL rc=$RC"; FAIL=$((FAIL+1))
  fi
  echo "$OUT" | tail -6
done
echo "=============================="
echo "TOTAL: PASS=$PASS FAIL=$FAIL"
