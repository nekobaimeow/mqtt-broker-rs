# mqtt-broker-rs

A from-scratch MQTT 3.1.1 broker in Rust, benchmarked head-to-head against Mosquitto 2.0.18.

Two implementations live in this repo:

| Version | Architecture | Dependencies | Notes |
|---|---|---|---|
| `mqtt_broker.rs` | threaded (2 threads/connection) | **zero** (pure std, `rustc -O`) | reference implementation |
| `mio_broker/` | single-threaded event loop (mio) | mio 1.x only | **recommended**, fastest & leanest |

## Features (mio version)

- MQTT 3.1.1: CONNECT / SUBSCRIBE (wildcards `#` `+`) / PUBLISH / UNSUBSCRIBE / PINGREQ / DISCONNECT
- **QoS 0 / 1 / 2** full delivery semantics:
  - QoS1: PUBACK, bounded in-flight window (256), DUP retransmit with timeout, give-up after 3 retries
  - QoS2: full PUBLISH→PUBREC→PUBREL→PUBCOMP handshake both directions, inbound dedup
- **Retained messages**: store/overwrite/clear, wildcard delivery on subscribe, QoS downgrade
- Bounded per-subscriber queues: slow QoS0 subscribers get dropped messages (at-most-once, legal), slow QoS1/2 subscribers get disconnected rather than blocking the publisher or exhausting memory
- Keepalive ×1.5 timeout, zombie connection reaping
- TCP_NODELAY, batched (32KB coalesced) writes, adaptive poll timeout (1ms active / 50ms idle)

## Performance vs Mosquitto 2.0.18 (WSL2, same host)

Throughput (msg/s, QoS0, 32B payload, 1 publisher → N subscribers):

| Scenario | this broker (mio) | mosquitto | ratio |
|---|---|---|---|
| 1→1, 100k | 158k | 87k | 1.8x |
| 1→10, 100k | 956k | 34k | **28x** |
| 1→100, 10k | 1.88M | 34k | **55x** |

Latency (ping-pong, 10k samples): p50 **160μs** vs mosquitto 182μs, p99 323μs vs 386μs.

Memory (300 idle connections): RSS **28.7MB** vs mosquitto 32.0MB, VmSize 38.8MB vs 47.6MB.

## Layout

```
mqtt_broker.rs          threaded zero-dep broker
mio_broker/             event-loop broker (cargo, mio 1.x)
mqtt_test.rs            8-scenario protocol integration test (zero-dep client)
mqtt_qos1_test.rs       12-scenario QoS1 test (in mio_broker/)
mqtt_qos2_retain_test.rs 18-scenario QoS2 + retain test (in mio_broker/)
mqtt_bench.rs           throughput + latency benchmark
repro.rs                minimal reproduction client
sanity.py / oom_test.py / mixed_test.py / rss_probe.py   Python aux tests
TEST.md                 full test report & benchmarking methodology
```

## Build & run

```bash
# threaded zero-dep version
rustc -O mqtt_broker.rs -o mqtt_broker
./mqtt_broker 0.0.0.0:1883

# event-loop version
cd mio_broker && cargo build --release
MALLOC_ARENA_MAX=2 ./target/release/mqtt_mio_broker 0.0.0.0:1883
```

`MALLOC_ARENA_MAX=2` matters: it cuts VmSize from 9.4GB to 180MB on glibc (malloc arena preallocation).

## Tests

```bash
cd mio_broker && cargo build --release
rustc -O mqtt_test.rs -o mqtt_test && ./mqtt_test 127.0.0.1:11883          # QoS0, 8 scenarios
rustc -O mqtt_qos1_test.rs -o qos1_test && ./qos1_test 127.0.0.1:11883    # QoS1, 12 scenarios
rustc -O mqtt_qos2_retain_test.rs -o qos2_test && ./qos2_test 127.0.0.1:11883  # QoS2+retain, 18 scenarios
```

Run the broker on 11883 with `QOS1_RETRY_MS=500` to make the DUP retransmit tests fast.

## Design notes

- Single event loop, no locks, `HashMap` per-connection state. Fan-out is a memcpy + one write per subscriber — which is why 1→100 throughput dwarfs mosquitto.
- Per-subscriber bounded queue = perfect isolation: one slow client can never stall the publisher (mosquitto's TCP backpressure can).
- Retransmission uses a per-connection in-flight window (256) to bound memory; PID reuse after ack.
- Known missing features vs mosquitto: LWT, persistent sessions, $SYS, TLS/WebSocket, auth/ACL, bridging, shared subscriptions.

## Benchmark methodology (fairness notes)

- Mosquitto run with `-v` logging **off** (logging halves its throughput: 43k→87k).
- Both brokers on same host, loopback, identical workload (QoS0, 32B payload).
- WSL2: epoll edge cases exist (README in TEST.md §8), absolute numbers are WSL-relative; ratios are the meaningful metric.
