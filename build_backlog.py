#!/usr/bin/env python3
"""jcode 功能 backlog 生成器 — 每个功能生成一个任务 cmd（单行、apply_patch 注入、走 zen_proxy）。

用法: python3 build_backlog.py  → 生成 jcode_backlog.json（WSL 侧状态文件）
"""
import json, os

# ---- 任务 prompt 模板（与已验证的 $SYS/订阅索引/TEST.md 同款纪律） ----
PREFIX = """Work in C:\\Users\\trade\\mqtt-broker-rs\\mio_broker. Edit ONLY src\\main.rs using the apply_patch tool (never the write tool - it overwrites the whole file). After each patch run: cd /d C:\\Users\\trade\\mqtt-broker-rs\\mio_broker && cargo build --release 2>&1 | findstr /i error - if any error line appears, fix and rebuild until clean. Report patches applied at the end.

apply_patch format (MANDATORY): start with a line exactly `*** Begin Patch`, then `*** Update File: src\\main.rs`, then `@@` context lines (space-prefixed context, - removed, + added), end with `*** End Patch`. If apply_patch reports no valid directives, re-emit with the exact header. Do ONE logical change per patch. Keep existing behavior and the MQTT wire protocol unchanged. No new dependencies - std only.
"""

TASKS = [
    {
        "id": "trie-subindex",
        "desc": "订阅索引 trie 化：精确+通配分层 hash 升级为前缀树",
        "prompt": PREFIX + """TASK: upgrade the two-tier subscription index to a topic-level trie so wildcard matching also skips non-matching branches. Today: Broker has subs_exact: HashMap<String,Vec<usize>> and subs_wild: Vec<usize>, both rebuilt by rebuild_index(); publish() does O(1) exact lookup then scans every wildcard index with topic_matches(). With many wildcard subscribers the wildcard scan is still O(W) per message.

IMPLEMENTATION:
1. Replace the flat subs_wild Vec with a trie over topic levels. Node children keyed by level string; store at each node a Vec<usize> of sub indices whose filter ends exactly there, plus a Vec<usize> of indices whose filter has a # continuation (matches subtree) and a Vec<usize> for + (single-level wildcard) continuation.
2. Keep subs_exact as-is (exact filters). Wildcard filters (containing + or #) go into the trie instead of subs_wild.
3. Insert: split filter on /, walk/create trie nodes; a filter ending in # registers at its parent level as subtree-match; a + level registers as single-level wildcard child; an exact level registers at that node.
4. Query in publish(): collect candidates = subs_exact.get(topic) indices + trie walk of the topic levels (following exact child, + child, and # subtree matches at each level). Then filter candidates with the existing topic_matches() to preserve exact semantics (candidates may be supersets, e.g. a # subtree match must still respect level count constraints - actually # matches everything below, but keep topic_matches() as the final authority so semantics are identical).
5. Rebuild: rebuild_index() must rebuild the trie too. All mutation sites already call rebuild_index() after structural changes; add trie rebuild there.
6. Performance goal: with many unrelated wildcard filters (e.g. 1000 subscribers on distinct prefixes), a publish to one topic must NOT scan all 1000 - the trie walk only visits matching branches. A regression test at the end: temporarily print (via eprintln) the number of topic_matches() calls per publish when 100 subscribers on distinct prefixes + 1 subscriber on the target exist; the count must be far below 100 (trie prunes). Then remove the eprintln.

Deliverables: report the trie design, patch count, and the measured topic_matches-call reduction."""
    },
    {
        "id": "disk-persistence",
        "desc": "消息持久化：retain + 持久会话离线队列落盘，重启不丢",
        "prompt": PREFIX + """TASK: persist retained messages and persistent-session offline queues to disk so a broker restart does not lose them. Today retained: HashMap<String,(Vec<u8>,u8)> and SessionState.offline: VecDeque live only in memory.

IMPLEMENTATION:
1. On startup, if file broker_state.bin exists, load it; on clean shutdown (SIGINT/SIGTERM handling or a simple save after every mutation - choose the simpler correct option), serialize and write it.
2. Use a minimal hand-rolled binary format (no serde, no new deps): magic bytes MQTTSTATE\\0, version u8=1, then counts; retained entries as (u16 topic len, topic, u16 payload len, payload, u8 qos); sessions as (u16 client_id len, client_id, u16 sub_count, (u16 filter len, filter, u8 qos)*, u16 offline_count, (u16 topic len, topic, u16 payload len, payload, u8 qos)*). Read must be tolerant of truncation (return Err and start empty rather than panic).
3. Persist points: after retain insert/remove in publish(), after CONNECT clean=1 session drop, after remove_client() stores a session, after offline queue flush on reconnect. Simplest correct approach: a dirty flag + save() call at the end of handle_packets and remove_client. Do NOT persist QoS0 data, in-flight QoS1/2 retransmit state, or client connections.
4. Verify: write a small test scenario in your head and describe it - start broker, publish retained msg + create offline session, kill -9 broker (no clean shutdown - document that this loses the tail write, acceptable), restart, subscribe - retained msg must be there; reconnect with clean=0 - offline queued messages must be delivered.
5. Keep the file path configurable via env var MQTT_STATE_FILE with default broker_state.bin in cwd.

Deliverables: report the format, save points, and load-tolerance behavior."""
    },
    {
        "id": "shared-subs",
        "desc": "共享订阅 $share/g: 同组客户端轮询分发（MQTT 3.1.1 常见扩展）",
        "prompt": PREFIX + """TASK: implement shared subscriptions ($share/{group}/{filter}) - the de-facto MQTT 3.1.1 extension for load-balanced delivery. A PUBLISH matching a shared group is delivered to exactly ONE member of that group (round-robin), not all.

IMPLEMENTATION:
1. Parse: in parse_subscribe, when a filter starts with $share/, split into group name and real filter (format: $share/<group>/<real filter>). Store shared subs separately from normal subs: add field shared_subs: Vec<SharedSub> where SharedSub { group: String, filter: String (real filter), token: usize, qos: u8 }.
2. Delivery in publish(): first collect matching normal subs and deliver to all (existing path). Then for each matching shared group, deliver to exactly one member using a per-group round-robin cursor: add field share_rr: HashMap<String,usize> mapping group -> next member index into the collected matching members of that group; advance on each delivery. If the chosen member is dead (deliver_to Err), try the next member in the group until one succeeds or all fail (then prune dead ones).
3. SUBACK grants: return the requested QoS for the $share/... subscription as usual.
4. UNSUBSCRIBE with a $share/... filter must remove the matching shared sub (compare by group+filter+token).
5. Session restore: shared subs are per-connection, NOT stored in persistent sessions (drop them on disconnect; do not persist).
6. add_sub()/rebuild_index() must stay correct: $share subs do not go into subs_exact/subs_wild; they only live in shared_subs. The two-tier index remains for normal subs.
7. QoS semantics: per-subscriber min(src_qos, sub.qos) as normal. Retained delivery to $share groups: deliver retained messages to shared subs too (one member).

Deliverables: report data structures, round-robin logic, and edge cases handled."""
    },
    {
        "id": "sys-more",
        "desc": "$SYS 扩展：bytes sent/received、heap、subscriptions 细分、clients maximum",
        "prompt": PREFIX + """TASK: extend $SYS topics (mosquitto-style) with more counters, following the same pattern as the existing publish_sys_topics() (10s retained publish, existing counters: version/uptime/clients/messages/publish/subscriptions).

ADD these topics to publish_sys_topics():
- $SYS/broker/bytes/received: total bytes read from clients (accumulate in handle_packets where read_buf grows, and in the initial read)
- $SYS/broker/bytes/sent: total bytes written to clients (accumulate in flush_writes where packets are written)
- $SYS/broker/clients/maximum: high-water mark of concurrent clients (update in add_client path when clients.len() exceeds it)
- $SYS/broker/messages/stored: count of retained messages currently held
- $SYS/broker/subscriptions/count already exists; keep it

Keep payload format identical to existing $SYS topics (plain decimal strings, QoS0 retained). All counters u64, init 0 in Broker::new, update at the same places the existing counters are updated. Keep the 10s timer.

Deliverables: report counters added, update points, and confirm all existing tests still pass."""
    },
    {
        "id": "conn-rate-limit",
        "desc": "连接限流：每 IP 并发上限 + 全局连接速率限制（防滥用）",
        "prompt": PREFIX + """TASK: add connection abuse protection. Today any client can connect without limit; a flood of CONNECTs (possibly on many sockets) is only bounded by fd limits.

IMPLEMENTATION:
1. Per-IP concurrent connection cap: field ip_conns: HashMap<IpAddr,u32> + max_per_ip: u32 = 32 (const). On accept(), look up peer ip; if count >= max, send CONNACK 0x03 (server unavailable) then close immediately; else increment. On client removal (remove_client), decrement.
2. Global connect rate limit: token-bucket-ish: field connect_budget: u32 = 200, refilled by 50 per second (accumulate in the main loop using elapsed time); a new CONNECT consumes 1 token; if budget is 0, respond CONNACK 0x03 and close. Do not count failed non-CONNECT sockets.
3. Only apply to the accept path in main() where TcpListener accepts; the CONNACK reject must be a valid MQTT CONNACK (type 2, flags 0, return code 3).
4. Keep behavior identical for legitimate clients: unlimited distinct IPs, and a single client reconnecting repeatedly is fine (rate window is 200/50-per-sec which is generous).
5. Add counters to Broker: conn_rejected_ip: u64, conn_rejected_rate: u64 (increment on each rejection type).

Deliverables: report the design, and the CONNACK reject path."""
    },
]

def main():
    # 生成 backlog 状态文件（WSL 侧管理用）
    backlog = []
    for t in TASKS:
        backlog.append({
            "id": t["id"],
            "desc": t["desc"],
            "status": "pending",   # pending / in_progress / done / failed
            "commit": "",
            "started_at": "",
            "finished_at": "",
            "note": "",
        })
    with open("/home/trade/mqtt_lab/jcode_backlog.json", "w") as f:
        json.dump(backlog, f, ensure_ascii=False, indent=2)
    # 生成每个任务的 cmd 文件
    for t in TASKS:
        single = " ".join(line.strip() for line in t["prompt"].splitlines() if line.strip())
        cmd = f"""@echo off
set JCODE_NO_TELEMETRY=1
set DO_NOT_TRACK=1
C:\\Users\\trade\\.local\\bin\\jcode.exe --provider-profile zen-free --model deepseek-v4-flash-free --tools read,grep,apply_patch,bash,todo run "{single}"
"""
        path = f"/tmp/jcode_task_{t['id']}.cmd"
        with open(path, "w", newline="\r\n") as f:
            f.write(cmd)
        print(f"{t['id']}: {os.path.getsize(path)}B cmd -> {path}")
    print("backlog: /home/trade/mqtt_lab/jcode_backlog.json")
    print("TOTAL:", len(TASKS), "tasks")

if __name__ == "__main__":
    main()
