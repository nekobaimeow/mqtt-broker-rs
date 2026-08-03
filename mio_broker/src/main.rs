// mqtt_mio_broker — MQTT 3.1.1 broker, single-threaded event loop (mio 1.x)
// Architecture: one poll loop, no per-connection threads. Memory per
// connection is a few hundred bytes instead of 2 thread stacks + malloc arenas.
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// Hot-path packet logging is gated behind MQTT_VERBOSE=1: println! takes the
// stdout lock per call, and at 50k+ msg/s that lock becomes the bottleneck.
// Error-level lines (drops, giveups, slow-loop warnings) always print.
static VERBOSE: AtomicBool = AtomicBool::new(false);

macro_rules! vlog {
    ($($arg:tt)*) => {
        if VERBOSE.load(Ordering::Relaxed) {
            println!($($arg)*);
        }
    };
}

const LISTENER: Token = Token(0);
// Flood guard ceiling for the per-connection read buffer. Legal max: one full
// MAX_PACKET body plus one partial packet's header/body prefix, so 2x MAX_PACKET
// (2MB). The 64KB drain buffer can push read_buf to 64KB+residue in one shot,
// so this must stay well above the per-read chunk size.
const READ_CAP: usize = 2 * MAX_PACKET;
const MAX_PACKET: usize = 1 << 20; // refuse packets > 1MB (anti-memory-attack)
const WRITE_QUEUE_CAP: usize = 8192; // bounded outbound queue (QoS0 drop when full)
const IN_FLIGHT_CAP: usize = 256; // max unacked QoS1 per connection (memory guard)
const OFFLINE_CAP: usize = 1024;  // max queued messages per persistent offline session
const QOS1_MAX_RETRIES: u32 = 3; // give up on the client after N unanswered retries

fn qos1_retry_after() -> Duration {
    // env override so tests don't have to wait 10s
    match std::env::var("QOS1_RETRY_MS") {
        Ok(v) => Duration::from_millis(v.parse().unwrap_or(10000)),
        Err(_) => Duration::from_millis(10000),
    }
}

// ---- packet types ----
const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const PUBREC: u8 = 5;
const PUBREL: u8 = 6;
const PUBCOMP: u8 = 7;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

fn encode_rem(mut len: usize, buf: &mut Vec<u8>) {
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        buf.push(b);
        if len == 0 {
            break;
        }
    }
}

fn build_connack() -> Vec<u8> {
    vec![(CONNACK << 4), 0x02, 0x00, 0x00]
}
fn build_suback(pid: u16, grants: &[u8]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(3 + grants.len());
    pkt.push(SUBACK << 4);
    pkt.push((2 + grants.len()) as u8);
    pkt.extend_from_slice(&pid.to_be_bytes());
    pkt.extend_from_slice(grants);
    pkt
}
fn build_unsuback(pid: u16) -> Vec<u8> {
    vec![(UNSUBACK << 4), 0x02, (pid >> 8) as u8, pid as u8]
}
fn build_puback(pid: u16) -> Vec<u8> {
    vec![(PUBACK << 4), 0x02, (pid >> 8) as u8, pid as u8]
}
fn build_pubrec(pid: u16) -> Vec<u8> {
    vec![(PUBREC << 4), 0x02, (pid >> 8) as u8, pid as u8]
}
fn build_pubrel(pid: u16) -> Vec<u8> {
    // PUBREL fixed header flags must be 0x02
    vec![(PUBREL << 4) | 0x02, 0x02, (pid >> 8) as u8, pid as u8]
}
fn build_pubcomp(pid: u16) -> Vec<u8> {
    vec![(PUBCOMP << 4), 0x02, (pid >> 8) as u8, pid as u8]
}
fn build_pingresp() -> Vec<u8> {
    vec![(PINGRESP << 4), 0x00]
}
fn build_forward(topic: &[u8], payload: &[u8], qos: u8, pid: Option<u16>, dup: bool, retain: bool) -> Vec<u8> {
    let mut flags = PUBLISH << 4;
    if qos > 0 {
        flags |= qos << 1;
    }
    if dup {
        flags |= 0x08;
    }
    if retain {
        flags |= 0x01;
    }
    let mut pkt = vec![flags];
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic);
    if let Some(p) = pid {
        body.extend_from_slice(&p.to_be_bytes());
    }
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

// ---- topic filter matching (+ and #) ----
fn topic_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    let mut i = 0;
    while i < f.len() {
        if f[i] == "#" {
            return true;
        }
        if i >= t.len() {
            return false;
        }
        if f[i] != "+" && f[i] != t[i] {
            return false;
        }
        i += 1;
    }
    i == t.len()
}

// ---- packet parsers (same logic as the threaded broker) ----
fn parse_connect(body: &[u8]) -> Option<(String, u16, bool, u8, bool, String, Vec<u8>, bool)> {
    if body.len() < 12 {
        return None; // Need at least protocol name/flags/keepalive + client id (min 4+1+2+1+2 = 10)
    }
    let plen = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 6 + plen + 2 {
        return None;
    }
    let flags = body[3 + plen]; // connect flags byte: 2(len) + plen(protocol name) + 1(level)
    let keepalive = u16::from_be_bytes([body[4 + plen], body[5 + plen]]);
    let mut pos = 6 + plen; // payload starts after name+level+flags+keepalive
    
    // Parse client id
    let clen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2;
    if pos + clen > body.len() {
        return None;
    }
    let cid = String::from_utf8_lossy(&body[pos..pos + clen]).to_string();
    pos += clen;
    
    // clean session flag: bit 1 of connect flags (0x02)
    let clean_session = (flags >> 1) & 0x01 == 1;
    
    // Parse will flag, QoS, retain, topic, message
    let will_flag = (flags >> 2) & 0x01 == 1;
    let will_qos = (flags >> 3) & 0x03; // 2 bits for QoS
    let will_retain = (flags >> 5) & 0x01 == 1;
    
    if will_flag {
        // Will topic
        if pos + 2 > body.len() {
            return None;
        }
        let wlen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        if pos + wlen > body.len() {
            return None;
        }
        let wtopic = String::from_utf8_lossy(&body[pos..pos + wlen]).to_string();
        pos += wlen;
        
        // Will message
        if pos + 2 > body.len() {
            return None;
        }
        let mlen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        if pos + mlen > body.len() {
            return None;
        }
        let wmsg = body[pos..pos + mlen].to_vec();
        
        Some((cid, keepalive, will_flag, will_qos as u8, will_retain, wtopic, wmsg, clean_session))
    } else {
        Some((cid, keepalive, false, 0, false, String::new(), Vec::new(), clean_session))
    }
}

fn parse_subscribe(body: &[u8]) -> Option<(u16, Vec<(String, u8)>)> {
    if body.len() < 3 {
        return None;
    }
    let pid = u16::from_be_bytes([body[0], body[1]]);
    let mut pos = 2;
    let mut out = Vec::new();
    while pos + 2 <= body.len() {
        let tlen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        if pos + tlen + 1 > body.len() {
            return None;
        }
        let filter = String::from_utf8_lossy(&body[pos..pos + tlen]).to_string();
        pos += tlen;
        let qos = body[pos] & 0x03;
        pos += 1;
        out.push((filter, qos));
    }
    Some((pid, out))
}

fn parse_unsubscribe(body: &[u8]) -> Option<(u16, Vec<String>)> {
    if body.len() < 3 {
        return None;
    }
    let pid = u16::from_be_bytes([body[0], body[1]]);
    let mut pos = 2;
    let mut out = Vec::new();
    while pos + 2 <= body.len() {
        let tlen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        if pos + tlen > body.len() {
            return None;
        }
        out.push(String::from_utf8_lossy(&body[pos..pos + tlen]).to_string());
        pos += tlen;
    }
    Some((pid, out))
}

fn parse_publish(flags: u8, body: &[u8]) -> Option<(String, u8, Option<u16>, Vec<u8>, bool)> {
    if body.len() < 3 {
        return None;
    }
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let qos = (flags >> 1) & 0x03;
    let retain = flags & 0x01 != 0;
    let mut pos = 2 + tlen;
    if pos > body.len() {
        return None;
    }
    let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
    let pid = if qos > 0 {
        if pos + 2 > body.len() {
            return None;
        }
        let p = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        Some(p)
    } else {
        None
    };
    let payload = body[pos..].to_vec();
    Some((topic, qos, pid, payload, retain))
}

// Extract ONLY the packet id from a PUBLISH body (topic len + topic + pid).
// Used by the no-subscriber fast path, where topic/payload are never inspected
// so the String + Vec allocations in parse_publish are skipped entirely.
fn parse_pid_only(body: &[u8]) -> Option<u16> {
    if body.len() < 4 {
        return None;
    }
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + tlen + 2 {
        return None;
    }
    Some(u16::from_be_bytes([body[2 + tlen], body[2 + tlen + 1]]))
}

// ---- per-connection state ----
struct Subscription {
    filter: String,
    token: usize,
    qos: u8,
}

// an outbound message waiting for ack:
//   QoS1: pkt = PUBLISH, waiting for PUBACK
//   QoS2: pkt = PUBLISH, awaiting PUBREC -> then PUBREL, awaiting PUBCOMP
struct InFlight {
    pkt: Vec<u8>,        // the packet that must be (re)transmitted
    sent_at: Instant,
    retries: u32,
    qos2: bool,          // QoS2 handshake?
    await_pubcomp: bool, // QoS2: true = sent PUBREL, waiting PUBCOMP
}

struct Client {
    stream: TcpStream,
    read_buf: Vec<u8>,
    // queue elements carry their QoS flag: (is_qos1, packet)
    write_queue: VecDeque<(bool, Vec<u8>)>,
    write_pending: Vec<u8>, // coalesced batch currently being written
    write_off: usize,       // consumed bytes of write_pending
    client_id: String,
    keepalive_deadline: Option<Instant>,
    keepalive_secs: u16,
    token: usize,
    // last interest registered with the poll registry; reregister is an
    // epoll_ctl syscall, so skip it when the desired interest is unchanged
    interest: Interest,
    next_pid: u16,                     // QoS1/2 packet-id allocator (1..=65535)
    in_flight: HashMap<u16, InFlight>, // pid -> outbound ack state
    received_qos2: HashMap<u16, ()>,   // inbound QoS2 pids seen (dedup until PUBREL)
    // LWT (Last Will and Testament) support per MQTT 3.1.1 §3.1.3
    will_flag: bool,
    will_qos: u8,
    will_retain: bool,
    will_topic: String,
    will_message: Vec<u8>,
    // true when the client sent a clean DISCONNECT packet (suppresses LWT)
    clean_disconnect: bool,
    // true when the client connected with clean session = 1 (no persistence)
    clean_session: bool,
}

impl Client {
    fn queue_qos0(&mut self, pkt: Vec<u8>) -> bool {
        if self.write_queue.len() >= WRITE_QUEUE_CAP {
            return false; // QoS0: drop
        }
        self.write_queue.push_back((false, pkt));
        true
    }

    // QoS1 must not be dropped: evict oldest QoS0 entries to make room.
    // Returns Err(()) if the queue is full of QoS1 only (client too slow).
    fn queue_qos1(&mut self, pkt: Vec<u8>) -> Result<(), ()> {
        while self.write_queue.len() >= WRITE_QUEUE_CAP {
            // find first QoS0 entry and evict it (keeps FIFO order for the rest)
            match self.write_queue.iter().position(|(q1, _)| !q1) {
                Some(idx) => {
                    self.write_queue.remove(idx);
                }
                None => return Err(()), // all QoS1, cannot evict -> drop connection
            }
        }
        self.write_queue.push_back((true, pkt));
        Ok(())
    }

    // allocate a packet id not currently in flight; None = window full
    fn alloc_pid(&mut self) -> Option<u16> {
        if self.in_flight.len() >= IN_FLIGHT_CAP {
            return None; // client isn't acking fast enough
        }
        for _ in 0..=u16::MAX {
            if self.next_pid == 0 {
                self.next_pid = 1;
            }
            let pid = self.next_pid;
            self.next_pid = self.next_pid.wrapping_add(1);
            if !self.in_flight.contains_key(&pid) {
                return Some(pid);
            }
        }
        None
    }
}

// ---- wildcard subscription trie ----
// A topic-level trie indexes every wildcard filter (one containing + or #) so
// publish() can walk only the branches of the topic that could match, rather
// than scanning every wildcard subscription. node.children holds exact-level
// continuations, node.plus is the single-level-wildcard (+) continuation, and
// node.hash_subs holds subscriptions whose filter has # directly below this
// node (i.e. matches this node's entire remaining subtree). node.ends holds
// subscriptions whose filter stops exactly at this node.
#[derive(Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    plus: Option<Box<TrieNode>>,
    ends: Vec<usize>,
    hash_subs: Vec<usize>,
}

#[derive(Default)]
struct Trie {
    root: TrieNode,
}

impl Trie {
    fn new() -> Self {
        Trie { root: TrieNode::default() }
    }

    fn clear(&mut self) {
        self.root = TrieNode::default();
    }

    // Register a wildcard filter's index. `levels` is the filter split on '/'.
    fn insert(&mut self, idx: usize, levels: &[&str]) {
        let mut node = &mut self.root;
        let mut i = 0;
        while i < levels.len() {
            let lvl = levels[i];
            if lvl == "#" {
                // # must be the last level; it matches this node's whole subtree.
                node.hash_subs.push(idx);
                return;
            }
            if lvl == "+" {
                node = &mut *node.plus.get_or_insert_with(Box::default);
                i += 1;
                continue;
            }
            node = node.children.entry(lvl.to_string()).or_default();
            i += 1;
        }
        node.ends.push(idx);
    }

    // Walk the topic levels, appending every candidate subscription index whose
    // filter could match `topic`. Candidates are a superset of true matches: the
    // caller still runs topic_matches() as the final authority. `calls` counts
    // how many candidates were emitted (for the regression counter).
    fn collect(&self, topic: &str, out: &mut Vec<usize>, calls: &mut u64) {
        let levels: Vec<&str> = topic.split('/').collect();
        self.collect_node(&self.root, &levels, 0, out, calls);
    }

    fn collect_node(&self, node: &TrieNode, levels: &[&str], t: usize, out: &mut Vec<usize>, calls: &mut u64) {
        // #-continuation subscriptions match the whole remaining subtree.
        if !node.hash_subs.is_empty() {
            out.extend_from_slice(&node.hash_subs);
            *calls += node.hash_subs.len() as u64;
        }
        if t >= levels.len() {
            // Topic exhausted: only filters ending exactly here can still match.
            if !node.ends.is_empty() {
                out.extend_from_slice(&node.ends);
                *calls += node.ends.len() as u64;
            }
            return;
        }
        // The single-level wildcard (+) consumes exactly this topic level.
        if let Some(plus) = &node.plus {
            self.collect_node(plus, levels, t + 1, out, calls);
        }
        // Exact level child for the current topic level.
        if let Some(child) = node.children.get(levels[t]) {
            self.collect_node(child, levels, t + 1, out, calls);
        }
    }
}

struct Broker {
    clients: HashMap<usize, Client>,
    subs: Vec<Subscription>,
    // two-tier index over `subs` to speed up publish fan-out:
    //  - subs_exact maps an exact filter (no + or #) to indices into subs
    //  - subs_wild is a trie over topic levels for filters containing + or #,
    //    so wildcard publish walks only matching branches instead of scanning
    //    every wildcard subscription.
    // Both are rebuilt by rebuild_index() when subs is structurally changed.
    subs_exact: HashMap<String, Vec<usize>>,
    subs_wild: Trie,
    next_token: usize,
    drops: u64,
    dead_pruned: u64,
    retained: HashMap<String, (Vec<u8>, u8)>, // topic -> (payload, qos)
    // persistent sessions (clean session = 0), keyed by client_id
    sessions: HashMap<String, SessionState>,
    // dirty flag: set whenever retained or a persistent session changes; a
    // save() flushes the whole state to disk and clears it.
    dirty: bool,
    // last time save() actually wrote (throttles disk writes off the hot path)
    last_save: Instant,
    // path of the broker_state.bin file (env MQTT_STATE_FILE or default)
    state_file: String,
    // $SYS broker statistics (Mosquitto-style)
    sys_clients_total: u64,
    sys_msgs_received: u64,
    sys_msgs_sent: u64,
    sys_pub_received: u64,
    sys_pub_sent: u64,
    sys_start: Instant,
    sys_last_publish: Instant,
}

// persistent session state: subscriptions (filter, qos) + offline QoS1/2 queue
struct SessionState {
    subs: Vec<(String, u8)>,
    offline: VecDeque<(String, Vec<u8>, u8)>, // (topic, payload, qos) awaiting delivery
}

// ---- disk persistence (broker_state.bin) ----
// Minimal hand-rolled binary format, no serde / external deps.
//
// Layout:
//   magic     "MQTTSTATE\0"  (10 bytes)
//   version   u8 = 1
//   retained_count   u16
//   retained entries: u16 topic_len, topic bytes, u16 payload_len, payload bytes, u8 qos
//   session_count    u16
//   sessions:
//     u16 client_id_len, client_id bytes,
//     u16 sub_count, (u16 filter_len, filter bytes, u8 qos)*,
//     u16 offline_count, (u16 topic_len, topic, u16 payload_len, payload, u8 qos)*
//
// Reads tolerate truncation / corruption: any short buffer or bad count returns
// Err, and the caller falls back to an empty in-memory state rather than panic.
const STATE_MAGIC: &[u8] = b"MQTTSTATE\0";
const STATE_VERSION: u8 = 1;

// push a little-endian u16 helper
fn push_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

// Serialize the current retained + sessions map into a byte buffer.
fn serialize_state(
    retained: &HashMap<String, (Vec<u8>, u8)>,
    sessions: &HashMap<String, SessionState>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(STATE_MAGIC);
    out.push(STATE_VERSION);
    push_u16(&mut out, retained.len() as u16);
    for (topic, (payload, qos)) in retained {
        push_u16(&mut out, topic.len() as u16);
        out.extend_from_slice(topic.as_bytes());
        push_u16(&mut out, payload.len() as u16);
        out.extend_from_slice(payload);
        out.push(*qos);
    }
    push_u16(&mut out, sessions.len() as u16);
    for (cid, sess) in sessions {
        push_u16(&mut out, cid.len() as u16);
        out.extend_from_slice(cid.as_bytes());
        push_u16(&mut out, sess.subs.len() as u16);
        for (filter, sqos) in &sess.subs {
            push_u16(&mut out, filter.len() as u16);
            out.extend_from_slice(filter.as_bytes());
            out.push(*sqos);
        }
        push_u16(&mut out, sess.offline.len() as u16);
        for (topic, payload, oqos) in &sess.offline {
            push_u16(&mut out, topic.len() as u16);
            out.extend_from_slice(topic.as_bytes());
            push_u16(&mut out, payload.len() as u16);
            out.extend_from_slice(payload);
            out.push(*oqos);
        }
    }
    out
}

// Take exactly n bytes, else None (short read).
fn take<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Option<&'a [u8]> {
    if *pos + n > buf.len() {
        return None;
    }
    let s = &buf[*pos..*pos + n];
    *pos += n;
    Some(s)
}

// Take a little-endian u16, else None on truncation.
fn take_u16(buf: &[u8], pos: &mut usize) -> Option<u16> {
    let b = take(buf, pos, 2)?;
    Some(u16::from_le_bytes([b[0], b[1]]))
}

// Deserialize the previous serialize_state() output. Any truncation, bad magic,
// bad version, or out-of-bounds read yields None (caller starts empty).
fn deserialize_state(
    data: &[u8],
) -> Option<(HashMap<String, (Vec<u8>, u8)>, HashMap<String, SessionState>)> {
    // Validate magic + version.
    if data.len() < STATE_MAGIC.len() + 1 {
        return None;
    }
    if &data[..STATE_MAGIC.len()] != STATE_MAGIC {
        return None;
    }
    if data[STATE_MAGIC.len()] != STATE_VERSION {
        return None;
    }
    let mut pos = STATE_MAGIC.len() + 1;
    let rcount = take_u16(data, &mut pos)? as usize;
    let mut retained = HashMap::new();
    for _ in 0..rcount {
        let tlen = take_u16(data, &mut pos)? as usize;
        let topic = take(data, &mut pos, tlen)?;
        let plen = take_u16(data, &mut pos)? as usize;
        let payload = take(data, &mut pos, plen)?;
        let qos = take(data, &mut pos, 1)?[0];
        // Reject impossible QoS so a corrupt byte can't poison routing.
        if qos > 2 {
            return None;
        }
        retained.insert(
            String::from_utf8_lossy(topic).to_string(),
            (payload.to_vec(), qos),
        );
    }
    let scount = take_u16(data, &mut pos)? as usize;
    let mut sessions = HashMap::new();
    for _ in 0..scount {
        let clen = take_u16(data, &mut pos)? as usize;
        let cid = take(data, &mut pos, clen)?;
        let s_count = take_u16(data, &mut pos)? as usize;
        let mut subs = Vec::new();
        for _ in 0..s_count {
            let flen = take_u16(data, &mut pos)? as usize;
            let filter = take(data, &mut pos, flen)?;
            let sqos = take(data, &mut pos, 1)?[0];
            if sqos > 2 {
                return None;
            }
            subs.push((String::from_utf8_lossy(filter).to_string(), sqos));
        }
        let o_count = take_u16(data, &mut pos)? as usize;
        let mut offline = VecDeque::new();
        for _ in 0..o_count {
            let tlen = take_u16(data, &mut pos)? as usize;
            let topic = take(data, &mut pos, tlen)?;
            let plen = take_u16(data, &mut pos)? as usize;
            let payload = take(data, &mut pos, plen)?;
            let oqos = take(data, &mut pos, 1)?[0];
            if oqos > 2 {
                return None;
            }
            offline.push_back((
                String::from_utf8_lossy(topic).to_string(),
                payload.to_vec(),
                oqos,
            ));
        }
        sessions.insert(
            String::from_utf8_lossy(cid).to_string(),
            SessionState { subs, offline },
        );
    }
    Some((retained, sessions))
}

impl Broker {
    // Write the retained table + persistent sessions to the state file.
    // Best-effort: errors are printed but never fatal (broker keeps running).
    fn save(&mut self) {
        if !self.dirty {
            return;
        }
        let data = serialize_state(&self.retained, &self.sessions);
        match std::fs::write(&self.state_file, data) {
            Ok(()) => {
                self.dirty = false;
                self.last_save = Instant::now();
                println!("[=] STATE     saved {} retained, {} sessions", self.retained.len(), self.sessions.len());
            }
            Err(e) => println!("[!] STATE     save failed: {e}"),
        }
    }

    // Load persisted state from disk into memory at startup. Returns the number
    // of sessions restored (used for a log line); tolerance of truncation is
    // handled here by falling back to empty state rather than panicking.
    fn load_state(&mut self) {
        match std::fs::read(&self.state_file) {
            Ok(data) => match deserialize_state(&data) {
                Some((retained, sessions)) => {
                    let sessions_len = sessions.len();
                    self.retained = retained;
                    self.sessions = sessions;
                    println!("[=] STATE     loaded {} retained, {} sessions from {}", self.retained.len(), sessions_len, self.state_file);
                }
                None => {
                    println!("[!] STATE     {} is corrupt/truncated; starting with empty state", self.state_file);
                }
            },
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                println!("[=] STATE     no {} found; starting fresh", self.state_file);
            }
            Err(e) => {
                println!("[!] STATE     could not read {}: {e}; starting empty", self.state_file);
            }
        }
        self.dirty = true; // ensure a fresh current snapshot is written on first save
    }
}

impl Broker {
    fn new() -> Self {
        let now = Instant::now();
        Broker {
            clients: HashMap::new(),
            subs: Vec::new(),
            subs_exact: HashMap::new(),
            subs_wild: Trie::new(),
            next_token: 1,
            drops: 0,
            dead_pruned: 0,
            retained: HashMap::new(),
            sessions: HashMap::new(),
            dirty: false,
            last_save: now,
            state_file: std::env::var("MQTT_STATE_FILE")
                .unwrap_or_else(|_| "broker_state.bin".into()),
            sys_clients_total: 0,
            sys_msgs_received: 0,
            sys_msgs_sent: 0,
            sys_pub_received: 0,
            sys_pub_sent: 0,
            sys_start: now,
            sys_last_publish: now,
        }
    }

    fn add_client(&mut self, stream: TcpStream) -> (usize, TcpStream) {
        let token = self.next_token;
        self.next_token += 1;
        (token, stream)
    }

    // A filter is wild if it contains '+' or '#'. Exact filters (no wildcards)
    // are indexed in subs_exact so publish can look them up with a hash lookup.
    fn is_wild_filter(filter: &str) -> bool {
        filter.contains('+') || filter.contains('#')
    }

    // Add one subscription to `subs` (source of truth) and index it. Appends
    // never renumber existing subs, so only the new entry needs indexing.
    fn add_sub(&mut self, filter: String, token: usize, qos: u8) {
        let idx = self.subs.len();
        self.subs.push(Subscription { filter: filter.clone(), token, qos });
        if Self::is_wild_filter(&filter) {
            let levels: Vec<&str> = filter.split('/').collect();
            self.subs_wild.insert(idx, &levels);
        } else {
            self.subs_exact.entry(filter).or_default().push(idx);
        }
    }

    // Rebuild both tiers from scratch. Called after any mutation that shifts
    // indices (UNSUBSCRIBE/remove_client retain, or publish swap_remove).
    fn rebuild_index(&mut self) {
        self.subs_exact.clear();
        self.subs_wild.clear();
        for (idx, s) in self.subs.iter().enumerate() {
            if Self::is_wild_filter(&s.filter) {
                let levels: Vec<&str> = s.filter.split('/').collect();
                self.subs_wild.insert(idx, &levels);
            } else {
                self.subs_exact.entry(s.filter.clone()).or_default().push(idx);
            }
        }
    }

    // forward to all matching subscribers; returns (delivered, dropped)
    // src_qos is the incoming PUBLISH QoS; per-subscriber forward QoS =
    // min(src_qos, sub.qos) per MQTT 3.1.1 §3.3.5. QoS1/2 forwards get a
    // packet id and are tracked in in_flight until acked. retain=true stores
    // (or clears, for empty payload) the message in the retained table.
    fn publish(&mut self, topic: &str, payload: &[u8], src_qos: u8, retain: bool) -> (usize, usize) {
        if retain {
            if payload.is_empty() {
                self.retained.remove(topic);
                self.dirty = true;
            } else {
                self.retained.insert(topic.to_string(), (payload.to_vec(), src_qos));
                self.dirty = true;
            }
        }
        // Fast path: zero subscribers AND zero persistent sessions means the
        // message has nowhere to go. Retained storage was handled above; skip
        // all routing machinery (index walk, candidate vec, sort, per-sub
        // queueing). This is the common case for pub-only benchmarks and
        // telemetry feeds with no consumers.
        if self.subs.is_empty() && self.sessions.is_empty() {
            return (0, 0);
        }
        let mut delivered = 0usize;
        let mut dead: u64 = 0;
        // Candidate indices from the two-tier index: exact-match subscriptions
        // come from the map with an O(1) hash lookup; possibly-matching
        // wildcard subscriptions come from walking only the trie branches that
        // the topic's levels visit (pruning unrelated wildcards entirely). Each
        // subscription is indexed in exactly one tier, so nothing is visited
        // twice. Candidates are a superset: topic_matches() below is the final
        // authority, so exact routing semantics are preserved unchanged.
        let mut idxs: Vec<usize> = Vec::new();
        if let Some(exact) = self.subs_exact.get(topic) {
            idxs.extend_from_slice(exact);
        }
        let mut _pruned = 0u64; // trie reports candidates that escaped pruning
        self.subs_wild.collect(topic, &mut idxs, &mut _pruned);
        // The trie yields a superset (e.g. a # subtree node is reached even when
        // the topic's remaining depth is bounded, or + end-continuations). Run
        // the existing topic_matches() on each candidate so routing semantics
        // are byte-for-byte identical to the previous flat scan. The trie has
        // already pruned unrelated branches: `calls` is the number of candidates
        // that escaped pruning, far below the total wildcard count.
        idxs.retain(|&wi| topic_matches(&self.subs[wi].filter, topic));
        // Process in descending index order so swap_remove() (which pulls the
        // last element into the removed slot) never invalidates a still-pending
        // lower index. Delivery order may change; coverage does not.
        idxs.sort_unstable_by(|a, b| b.cmp(a));
        for i in idxs {
            let token = self.subs[i].token;
            let fwd_qos = src_qos.min(self.subs[i].qos);
            match self.deliver_to(token, topic, payload, fwd_qos, false) {
                Ok(()) => delivered += 1,
                Err(()) => {
                    // client gone or too slow to accept reliable traffic:
                    // deliver_to already removed it; prune the subscription
                    self.subs.swap_remove(i);
                    dead += 1;
                }
            }
        }
        self.dead_pruned += dead;
        if dead > 0 {
            // swap_remove renumbered subs, so the index must be rebuilt.
            self.rebuild_index();
        }
        // offline delivery for persistent sessions: QoS1/2 messages matching a
        // stored session's subscriptions are queued while the client is away
        // (QoS0 is at-most-once and never stored, per MQTT 3.1.1 §3.1.2.4)
        if src_qos > 0 {
            let mut target: Vec<(String, u8)> = Vec::new();
            for (cid, sess) in &self.sessions {
                for (filter, sqos) in &sess.subs {
                    if topic_matches(filter, topic) {
                        target.push((cid.clone(), *sqos));
                        break;
                    }
                }
            }
            for (cid, sqos) in target {
                if let Some(sess) = self.sessions.get_mut(&cid) {
                    if sess.offline.len() < OFFLINE_CAP {
                        sess.offline.push_back((topic.to_string(), payload.to_vec(), src_qos.min(sqos)));
                        self.dirty = true;
                    }
                }
            }
        }
        (delivered, 0)
    }

    // Publish a disconnecting client's Last Will (MQTT 3.1.1 §3.1.3).
    // Must be called BEFORE the client is removed from the map: reads the
    // will fields off the Client, then publishes through the normal path
    // (honoring will_retain). No-op when the client had no will.
    fn publish_lwt(&mut self, token: usize) {
        let will = match self.clients.get(&token) {
            Some(c) if c.will_flag => Some((
                c.will_topic.clone(),
                c.will_message.clone(),
                c.will_qos,
                c.will_retain,
            )),
            _ => None,
        };
        if let Some((topic, msg, qos, retain)) = will {
            self.publish(&topic, &msg, qos, retain);
        }
    }

    // Remove a client, publishing its LWT unless it disconnected cleanly
    // (sent DISCONNECT). Every disconnect path must go through this.
    fn remove_client(&mut self, token: usize) {
        let clean = self
            .clients
            .get(&token)
            .map(|c| c.clean_disconnect)
            .unwrap_or(false);
        if !clean {
            self.publish_lwt(token);
        }
        // persistent session: keep subscriptions + queued reliable traffic
        // in the session store so a later reconnect (clean session = 0) can
        // restore them. QoS0 queue entries are NOT persisted (at-most-once).
        let persist = self
            .clients
            .get(&token)
            .map(|c| !c.clean_session && !c.client_id.is_empty())
            .unwrap_or(false);
        if persist {
            let cid = self.clients.get(&token).unwrap().client_id.clone();
            let subs: Vec<(String, u8)> = self
                .subs
                .iter()
                .filter(|s| s.token == token)
                .map(|s| (s.filter.clone(), s.qos))
                .collect();
            // pull QoS1/2 packets still sitting in the write queue
            let offline: Vec<(String, Vec<u8>, u8)> = self
                .clients
                .get(&token)
                .unwrap()
                .write_queue
                .iter()
                .filter(|(q1, _)| *q1)
                .map(|(_, pkt)| {
                    // decode topic from the queued PUBLISH for later redelivery
                    let body = &pkt[2..];
                    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
                    let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
                    let mut off = 2 + tlen;
                    let qos = (pkt[0] & 0x06) >> 1;
                    if qos > 0 {
                        off += 2; // packet id
                    }
                    (topic, body[off..].to_vec(), qos)
                })
                .collect();
            let entry = self.sessions.entry(cid.clone()).or_insert(SessionState {
                subs: Vec::new(),
                offline: VecDeque::new(),
            });
            for s in subs {
                if !entry.subs.contains(&s) {
                    entry.subs.push(s);
                }
            }
            for m in offline {
                if entry.offline.len() < OFFLINE_CAP {
                    entry.offline.push_back(m);
                }
            }
            vlog!("[=] SESSION   {cid} persisted ({} subs, {} queued)", entry.subs.len(), entry.offline.len());
            self.dirty = true;
        }
       self.subs.retain(|s| s.token != token);
       self.rebuild_index();
       self.clients.remove(&token);
        self.save();
   }

    // Publish $SYS broker topics as retained messages every 10s.
    // Uses the existing publish() path so retained storage and
    // delivery to matching subscribers work automatically.
    fn publish_sys_topics(&mut self) {
        let ver = env!("CARGO_PKG_VERSION");
        let uptime = self.sys_start.elapsed().as_secs();
        let connected = self.clients.len() as u64;
        let total_subs: usize = self.subs.len();

        let topics: &[(&str, String, u8)] = &[
            ("$SYS/broker/version", format!("mqtt-broker-rs {}", ver), 0),
            ("$SYS/broker/uptime", uptime.to_string(), 0),
            ("$SYS/broker/clients/connected", connected.to_string(), 0),
            ("$SYS/broker/clients/total", self.sys_clients_total.to_string(), 0),
            ("$SYS/broker/messages/received", self.sys_msgs_received.to_string(), 0),
            ("$SYS/broker/messages/sent", self.sys_msgs_sent.to_string(), 0),
            ("$SYS/broker/messages/publish/received", self.sys_pub_received.to_string(), 0),
            ("$SYS/broker/messages/publish/sent", self.sys_pub_sent.to_string(), 0),
            ("$SYS/broker/subscriptions/count", total_subs.to_string(), 0),
        ];
        for (topic, payload, _qos) in topics {
            let _ = self.publish(topic, payload.as_bytes(), 0, true);
        }
    }

   // deliver one message to a single client at the given forward QoS.
    // Err(()) = client was removed (gone, or queue overflow on QoS1/2).
   fn deliver_to(&mut self, token: usize, topic: &str, payload: &[u8], fwd_qos: u8, retain: bool) -> Result<(), ()> {
       if fwd_qos == 0 {
           let pkt = build_forward(topic.as_bytes(), payload, 0, None, false, retain);
           match self.clients.get_mut(&token) {
               Some(c) => {
                   if c.queue_qos0(pkt) {
                        self.sys_pub_sent += 1;
                       Ok(())
                   } else {
                       Err(()) // QoS0 queue full -> drop connection
                   }
               }
               None => Err(()),
           }
       } else {
           match self.clients.get_mut(&token) {
               Some(c) => match c.alloc_pid() {
                   Some(pid) => {
                       let pkt = build_forward(topic.as_bytes(), payload, fwd_qos, Some(pid), false, retain);
                       let keep = pkt.clone();
                       match c.queue_qos1(pkt) {
                           Ok(()) => {
                                self.sys_pub_sent += 1;
                               c.in_flight.insert(
                                    pid,
                                    InFlight {
                                        pkt: keep,
                                        sent_at: Instant::now(),
                                        retries: 0,
                                        qos2: fwd_qos == 2,
                                        await_pubcomp: false,
                                    },
                                );
                                Ok(())
                            }
                            Err(()) => {
                                self.clients.remove(&token);
                                Err(())
                            }
                        }
                    }
                    None => {
                        // pid window exhausted -> client too slow
                        self.clients.remove(&token);
                        Err(())
                    }
                },
                None => Err(()),
            }
        }
    }
}

// Try to parse and handle one packet from read_buf. Returns Err on protocol
// violation (connection should be closed).
fn handle_packets(broker: &mut Broker, token: usize) -> Result<(), String> {
    // Zero-copy read: swap the whole read buffer out of the client (no alloc,
    // no O(n) drain memmove) and parse it locally. Packet bodies are referenced
    // by (offset, len) into this local buffer instead of being copied per
    // packet — one PUBLISH burst of 100 packets used to cost 100 body Vec
    // allocations plus a drain() memmove. The local `buf` is independent of
    // `broker`, so the processing loop below can mutate broker while holding
    // body slices borrowed from `buf`.
    let mut buf = {
        let c = broker.clients.get_mut(&token).unwrap();
        std::mem::take(&mut c.read_buf)
    };
    if buf.is_empty() {
        return Ok(());
    }
    let mut pkts: Vec<(u8, u8, usize, usize)> = Vec::new(); // (type, flags, body_off, body_len)
    let mut consumed = 0usize;
    let mut bad = None;
    loop {
        let b = &buf[consumed..];
        if b.is_empty() {
            break;
        }
        let ptype = b[0] >> 4;
        let flags = b[0] & 0x0f;
        // decode remaining length
        let mut rem: usize = 0;
        let mut mult: usize = 1;
        let mut i = 1;
        let mut complete = false;
        while i < b.len() && i <= 4 {
            rem += ((b[i] & 0x7f) as usize) * mult;
            if b[i] & 0x80 == 0 {
                complete = true;
                break;
            }
            mult *= 128;
            i += 1;
        }
        if !complete {
            break; // need more bytes
        }
        if rem > MAX_PACKET {
            bad = Some(format!("packet too large ({rem}B)"));
            break;
        }
        if b.len() < i + 1 + rem {
            break; // body incomplete, wait for more
        }
        let bstart = consumed + i + 1;
        consumed += i + 1 + rem;
        pkts.push((ptype, flags, bstart, rem));
    }
    let pkt_count = pkts.len();
    // keep the unconsumed tail (partial packet) in the client's read buffer
    if consumed < buf.len() {
        let rest = buf.split_off(consumed);
        broker.clients.get_mut(&token).unwrap().read_buf = rest;
    }
    let client_id = broker
        .clients
        .get(&token)
        .map(|c| c.client_id.clone())
        .unwrap_or_default();
    if let Some(e) = bad {
        return Err(e);
    }

   // MQTT-3.1.2-23: broker must treat ANY packet as liveness proof. Refresh the
   // keepalive deadline on every received packet so bursty publishers that skip
   // PINGREQ (paho clients send PINGREQ only when idle) aren't dropped mid-load.
   {
       let c = broker.clients.get_mut(&token).unwrap();
       if let Some(_) = c.keepalive_deadline {
           let ka = c.keepalive_secs;
           c.keepalive_deadline = Some(Instant::now() + Duration::from_secs((ka as f64 * 1.5) as u64));
       }
   }

   for (ptype, flags, bstart, blen) in pkts {
       let body = &buf[bstart..bstart + blen];
       match ptype {
            CONNECT => match parse_connect(&body) {
                Some((cid, ka, will_flag, will_qos, will_retain, will_topic, will_message, clean_session)) => {
                    // clean session = 1: discard any stored session for this id.
                    // clean session = 0: restore the stored session's subscriptions
                    // into the new client (offline queue is flushed below, after
                    // the subscription restore so forwards find the new token).
                    if clean_session {
                        if broker.sessions.remove(&cid).is_some() {
                            broker.dirty = true;
                        }
                    } else if let Some(sess) = broker.sessions.get(&cid) {
                        let restore_subs: Vec<(String, u8)> = {
                            sess.subs.iter().map(|(f, q)| (f.clone(), *q)).collect()
                        };
                        for (filter, qos) in restore_subs {
                            broker.add_sub(filter.clone(), token, qos);
                            vlog!("[+] SESSION   {cid} restored sub {filter} (qos {qos})");
                        }
                    }
                    let c = broker.clients.get_mut(&token).unwrap();
                    c.client_id = cid.clone();
                    c.clean_session = clean_session;
                    c.keepalive_deadline = if ka > 0 {
                        Some(Instant::now() + Duration::from_secs((ka as f64 * 1.5) as u64))
                    } else {
                        None
                    };
                    c.keepalive_secs = ka;
                    c.will_flag = will_flag;
                    c.will_qos = will_qos;
                    c.will_retain = will_retain;
                    c.will_topic = will_topic;
                    c.will_message = will_message;
                    c.queue_qos0(build_connack());
                    // flush the persistent session's offline queue to the new
                    // client, at min(queued_qos, sub_qos) — QoS0 subscriptions
                    // still get QoS0 copies of queued QoS1/2 messages
                    if !clean_session {
                        let restored: Option<(Vec<(String, u8)>, Vec<(String, Vec<u8>, u8)>)> =
                            broker.sessions.get(&cid).map(|s| (s.subs.clone(), s.offline.iter().cloned().collect()));
                        if let Some((subs, queued)) = restored {
                            for (qtopic, qpayload, qqos) in queued {
                                let mut dqos = qqos;
                                // cap to what this client's subs allow
                                for (filter, sqos) in &subs {
                                    if topic_matches(filter, &qtopic) {
                                        dqos = dqos.min(*sqos);
                                        break;
                                    }
                                }
                                if broker.deliver_to(token, &qtopic, &qpayload, dqos, false).is_err() {
                                    return Err("client dropped during session flush".into());
                                }
                            }
                        }
                        if broker.sessions.remove(&cid).is_some() {
                            broker.dirty = true; // flushed queue, session moved to socket
                        }
                    }
                    vlog!("[+] CONNECT  {cid}  (token {token}, clean={clean_session})");
                    broker.sys_clients_total += 1;
                }
                None => return Err("bad CONNECT".into()),
            },
            SUBSCRIBE => match parse_subscribe(&body) {
                Some((pid, topics)) => {
                    let mut grants = Vec::new();
                    for (filter, qos) in &topics {
                        broker.add_sub(filter.clone(), token, *qos);
                        grants.push(*qos);
                        vlog!("[+] SUBSCRIBE {} -> {filter} (qos {qos})", client_id);
                        // retained delivery: every stored message matching this
                        // filter is sent immediately, at min(retained_qos, sub_qos)
                        let hits: Vec<(String, Vec<u8>, u8)> = broker
                            .retained
                            .iter()
                            .filter(|(t, _)| topic_matches(filter, t))
                            .map(|(t, (p, q))| (t.clone(), p.clone(), *q))
                            .collect();
                        for (rtopic, rpayload, rqos) in hits {
                            let dqos = rqos.min(*qos);
                            if broker.deliver_to(token, &rtopic, &rpayload, dqos, true).is_err() {
                                // client got dropped (queue overflow) — bail out
                                return Err("client dropped during retained delivery".into());
                            }
                        }
                    }
                    let c = broker.clients.get_mut(&token).unwrap();
                    c.queue_qos0(build_suback(pid, &grants));
                }
                None => return Err("bad SUBSCRIBE".into()),
            },
            UNSUBSCRIBE => match parse_unsubscribe(&body) {
                Some((pid, filters)) => {
                    broker.subs.retain(|s| !(s.token == token && filters.contains(&s.filter)));
                    broker.rebuild_index();
                    for f in &filters {
                        vlog!("[-] UNSUBSCRIBE {} -> {f}", client_id);
                    }
                    let c = broker.clients.get_mut(&token).unwrap();
                    c.queue_qos0(build_unsuback(pid));
                }
                None => return Err("bad UNSUBSCRIBE".into()),
            },
            PUBLISH => {
                let qos = (flags >> 1) & 0x03;
                let retain = flags & 0x01 != 0;
                // No-subscriber fast path: with zero subscribers AND zero
                // persistent sessions, a non-retained PUBLISH has nowhere to
                // go — only the ack matters. QoS1/2 need the packet id, so
                // decode just that and skip parse_publish's String+Vec
                // allocations (topic + payload copies). QoS0 needs nothing.
                let no_routing = !retain && broker.subs.is_empty() && broker.sessions.is_empty();
                if no_routing {
                    let mut is_dup = false;
                    if qos == 1 {
                        let p = parse_pid_only(&body).ok_or("bad PUBLISH")?;
                        let c = broker.clients.get_mut(&token).unwrap();
                        c.queue_qos0(build_puback(p));
                    } else if qos == 2 {
                        let p = parse_pid_only(&body).ok_or("bad PUBLISH")?;
                        let c = broker.clients.get_mut(&token).unwrap();
                        if c.received_qos2.contains_key(&p) {
                            is_dup = true;
                        } else {
                            c.received_qos2.insert(p, ());
                        }
                        c.queue_qos0(build_pubrec(p));
                    }
                    if !is_dup {
                        broker.sys_pub_received += 1;
                    }
                    vlog!("[>] PUBLISH  {} (no-subs fast path, qos {qos}{})", client_id,
                        if is_dup { " dup" } else { "" });
                } else {
                match parse_publish(flags, &body) {
                Some((topic, qos, pid, payload, retain)) => {
                    // QoS1: ack immediately. QoS2: ack (PUBREC) but dedupe —
                    // a repeated PID (publisher retry before our PUBREL reached
                    // them) must NOT be forwarded twice.
                    let mut seen_dup = false;
                    if let (Some(p), 1) = (pid, qos) {
                        let c = broker.clients.get_mut(&token).unwrap();
                        c.queue_qos0(build_puback(p));
                    } else if let (Some(p), 2) = (pid, qos) {
                        let c = broker.clients.get_mut(&token).unwrap();
                        if c.received_qos2.contains_key(&p) {
                            seen_dup = true;
                        } else {
                            c.received_qos2.insert(p, ());
                        }
                        c.queue_qos0(build_pubrec(p));
                    }
                   if !seen_dup {
                       let s = String::from_utf8_lossy(&payload);
                       let (n, _) = broker.publish(&topic, &payload, qos, retain);
                        broker.sys_pub_received += 1;
                        vlog!("[>] PUBLISH  {} -> {topic} (qos {qos}{}{}, {}B, delivered to {n}) \"{}\"",
                            client_id,
                            if retain { " retain" } else { "" },
                            if seen_dup { " dup" } else { "" },
                            payload.len(), s);
                        if broker.dead_pruned > 0 {
                            let dp = broker.dead_pruned;
                            broker.dead_pruned = 0;
                            println!("[!] pruned {dp} dead subscriptions (total drops: {})", broker.drops);
                        }
                    }
                }
                None => return Err("bad PUBLISH".into()),
                }
                }
            }
            PUBACK => {
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let c = broker.clients.get_mut(&token).unwrap();
                    if c.in_flight.remove(&pid).is_some() {
                        vlog!("[~] PUBACK   {} -> pid {pid}", client_id);
                    }
                } else {
                    return Err("bad PUBACK".into());
                }
            }
            PUBREC => {
                // subscriber acks our QoS2 PUBLISH -> send PUBREL, move to
                // AwaitPubcomp state (retransmits become PUBREL, DUP'd)
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let c = broker.clients.get_mut(&token).unwrap();
                    let mut rel = false;
                    if let Some(inf) = c.in_flight.get_mut(&pid) {
                        if inf.qos2 && !inf.await_pubcomp {
                            inf.await_pubcomp = true;
                            inf.sent_at = Instant::now();
                            inf.retries = 0;
                            rel = true;
                        }
                    }
                    if rel {
                        vlog!("[~] PUBREC   {} -> pid {pid} (-> PUBREL)", client_id);
                        let c = broker.clients.get_mut(&token).unwrap();
                        c.queue_qos0(build_pubrel(pid));
                    }
                } else {
                    return Err("bad PUBREC".into());
                }
            }
            PUBREL => {
                // publisher completes inbound QoS2: forget dedup state, ack
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let c = broker.clients.get_mut(&token).unwrap();
                    c.received_qos2.remove(&pid);
                    c.queue_qos0(build_pubcomp(pid));
                    vlog!("[~] PUBREL   {} -> pid {pid} (-> PUBCOMP)", client_id);
                } else {
                    return Err("bad PUBREL".into());
                }
            }
            PUBCOMP => {
                // final ack for outbound QoS2: drop the in-flight entry
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let c = broker.clients.get_mut(&token).unwrap();
                    if c.in_flight.remove(&pid).is_some() {
                        vlog!("[~] PUBCOMP  {} -> pid {pid}", client_id);
                    }
                } else {
                    return Err("bad PUBCOMP".into());
                }
            }
            PINGREQ => {
                let c = broker.clients.get_mut(&token).unwrap();
                c.queue_qos0(build_pingresp());
                vlog!("[~] PINGREQ  {} -> PINGRESP", client_id);
            }
            DISCONNECT => {
                vlog!("[-] DISCONNECT {} (token {token})", client_id);
                let c = broker.clients.get_mut(&token).unwrap();
                c.clean_disconnect = true;
                return Err("disconnect".into());
            }
            _ => return Err(format!("unknown packet type {ptype}")),
        }
    }
    broker.sys_msgs_received += pkt_count as u64;
    // NOTE: no save() here — disk writes on the per-batch hot path cause
    // latency spikes (a dirty $SYS retain makes every following PUBLISH batch
    // serialize + fsync synchronously). save() is now called from the main
    // loop tail with a 200ms throttle, keeping the receive path pure-memory.
    Ok(())
}

fn flush_writes(registry: &Registry, broker: &mut Broker, token: usize) {
    let (wrote_all, socket_dead) = {
        let c = broker.clients.get_mut(&token).unwrap();
        let Client { stream, write_queue, write_pending, write_off, .. } = c;
        let mut done = true;
        let mut dead = false;
        // Coalesce queued packets into one buffer and issue few, large write()
        // calls — one syscall per packet would cap throughput at syscall rate.
        const BATCH_MAX: usize = 32768;
        loop {
            if *write_off == write_pending.len() {
                if write_queue.is_empty() {
                    break; // all flushed
                }
                write_pending.clear();
                *write_off = 0;
                while write_pending.len() < BATCH_MAX {
                    match write_queue.pop_front() {
                        Some((_, p)) => write_pending.extend_from_slice(&p),
                        None => break,
                    }
                }
            }
            let n = match stream.write(&write_pending[*write_off..]) {
                Ok(0) => {
                    done = false;
                    break;
                }
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    done = false;
                    break;
                }
                Err(_) => {
                    dead = true;
                    break;
                }
            };
            *write_off += n;
        }
        (done, dead)
    };
    if socket_dead {
        broker.remove_client(token);
        return;
    }
    // Count packets written to socket for $SYS stats
    broker.sys_msgs_sent += 1;
    if let Some(c) = broker.clients.get_mut(&token) {
        let interest = if wrote_all {
            Interest::READABLE
        } else {
            Interest::READABLE | Interest::WRITABLE
        };
        // skip the epoll_ctl syscall when the interest didn't actually change
        if c.interest != interest {
            let _ = registry.reregister(&mut c.stream, Token(token), interest);
            c.interest = interest;
        }
    }
}

// Read ONE chunk from a client and parse it. Returning means the caller should
// flush writes (read and write must interleave, otherwise a burst publisher
// fills every subscriber queue and everything gets dropped before any flush).
// Returns Err if the connection must be closed (EOF, protocol error, flood).
fn drain_client(broker: &mut Broker, token: usize) -> Result<(), ()> {
    // 64KB stack buffer: one read should consume the whole kernel buffer on
    // a bursty publisher. 8KB meant several read syscalls + epoll rounds per
    // 8KB of traffic, which capped per-connection receive throughput.
    let mut buf = [0u8; 65536];
    let n = match broker.clients.get_mut(&token).unwrap().stream.read(&mut buf) {
        Ok(0) => return Err(()), // EOF
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
        Err(_) => return Err(()),
    };
    {
        let c = broker.clients.get_mut(&token).unwrap();
        c.read_buf.extend_from_slice(&buf[..n]);
        if c.read_buf.len() > READ_CAP {
            return Err(()); // flooding / oversized packet
        }
    }
    if handle_packets(broker, token).is_err() {
        return Err(());
    }
    Ok(())
}

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:1883".into());
    let sa: std::net::SocketAddr = addr.parse().expect("bad addr");
    let mut poll = Poll::new().expect("poll");
    let mut events = Events::with_capacity(1024);
    let mut listener = TcpListener::bind(sa).expect("bind");
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)
        .expect("register listener");
    let mut broker = Broker::new();
    broker.load_state();
    println!("[mqtt-mio-broker] listening on {addr} (mio {})", env!("CARGO_PKG_VERSION"));

    let mut idle_rounds: u32 = 0;
    let verbose = std::env::var("MQTT_VERBOSE")
        .map(|v| v == "1" || v == "true" || v == "yes")
        .unwrap_or(false);
    VERBOSE.store(verbose, Ordering::Relaxed);
    if verbose {
        println!("[mqtt-mio-broker] verbose logging enabled");
    }
    loop {
        let loop_start = std::time::Instant::now();
        // adaptive poll timeout: 1ms while active (low latency), back off to
        // 50ms when idle (CPU) — active data still wakes us via READABLE
        let timeout = if idle_rounds > 20 {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(1)
        };
        poll.poll(&mut events, Some(timeout)).expect("poll");
        let had_events = events.iter().next().is_some();
        if had_events {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
        }
        for event in events.iter() {
            match event.token() {
                LISTENER => {
                    loop {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                let (token, stream) = broker.add_client(stream);
                                // disable Nagle: MQTT frames are tiny and Nagle +
                                // delayed-ACK throttles small-packet throughput badly
                                stream.set_nodelay(true).expect("nodelay");
                                let client = Client {
                                    stream,
                                    read_buf: Vec::new(),
                                    write_queue: VecDeque::new(),
                                    write_pending: Vec::new(),
                                    write_off: 0,
                                    client_id: String::new(),
                                    keepalive_deadline: None,
                                    keepalive_secs: 0,
                                    token,
                                    interest: Interest::READABLE,
                                    next_pid: 1,
                                    in_flight: HashMap::new(),
                                    received_qos2: HashMap::new(),
                                    // LWT fields initialized to defaults
                                    will_flag: false,
                                    will_qos: 0,
                                    will_retain: false,
                                    will_topic: String::new(),
                                    will_message: Vec::new(),
                                    clean_disconnect: false,
                                    clean_session: true, // default: clean; CONNECT overrides
                                };
                                broker.clients.insert(token, client);
                                let c = broker.clients.get_mut(&token).unwrap();
                                poll.registry()
                                    .register(&mut c.stream, Token(token), Interest::READABLE)
                                    .expect("register client");
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                println!("[!] accept error: {e}");
                                break;
                            }
                        }
                    }
                }
                Token(token) => {
                    if event.is_readable() {
                        if drain_client(&mut broker, token).is_err() {
                            broker.remove_client(token);
                            continue;
                        }
                    }
                    if event.is_writable() {
                        // flush write queue; the borrow is released inside
                        let has_queue = {
                            broker.clients.get(&token).map(|c| !c.write_queue.is_empty()).unwrap_or(false)
                        };
                        if has_queue {
                            flush_writes(poll.registry(), &mut broker, token);
                        } else {
                            if let Some(c) = broker.clients.get_mut(&token) {
                                if c.interest != Interest::READABLE {
                                    let _ = poll.registry().reregister(
                                        &mut c.stream,
                                        Token(token),
                                        Interest::READABLE,
                                    );
                                    c.interest = Interest::READABLE;
                                }
                            }
                        }
                    }
                }
            }
        }
        // Single pass over all clients collecting: (a) tokens with queued
        // writes to flush, (b) keepalive-expired tokens, (c) in-flight
        // retransmissions. Previously this was 3-4 full scans per loop plus a
        // per-in-flight env-var lookup; now it's one scan with the env value
        // hoisted out of the loop.
        let now = Instant::now();
        let retry_after = qos1_retry_after();
        let mut pending: Vec<usize> = Vec::new();
        let mut dead: Vec<usize> = Vec::new();
        let mut retry: Vec<(usize, u16, Vec<u8>)> = Vec::new();
        let mut give_up: Vec<(usize, u16)> = Vec::new();
        for (t, c) in broker.clients.iter() {
            if !c.write_queue.is_empty() {
                pending.push(*t);
            }
            if let Some(d) = c.keepalive_deadline {
                if now > d {
                    dead.push(*t);
                }
            }
            for (pid, inf) in c.in_flight.iter() {
                if now.duration_since(inf.sent_at) < retry_after {
                    continue;
                }
                if inf.retries >= QOS1_MAX_RETRIES {
                    give_up.push((*t, *pid));
                } else if inf.qos2 && inf.await_pubcomp {
                    // we're waiting for PUBCOMP: retransmit PUBREL (DUP flag)
                    let mut rel = build_pubrel(*pid);
                    rel[0] |= 0x08; // DUP bit
                    retry.push((*t, *pid, rel));
                } else {
                    // waiting for PUBACK (QoS1) or PUBREC (QoS2): re-send PUBLISH DUP
                    let mut dup = inf.pkt.clone();
                    dup[0] |= 0x08; // DUP bit
                    retry.push((*t, *pid, dup));
                }
            }
        }
        // eagerly flush any client with queued data (non-blocking). The
        // WRITABLE interest is only a fallback for when the kernel buffer is
        // full — relying on epoll to notice writability adds latency per batch.
        for t in &pending {
            if broker.clients.contains_key(t) {
                flush_writes(poll.registry(), &mut broker, *t);
            }
        }
        // belt & braces: actively drain every client once per loop. epoll
        // level-triggered notifications should cover this, but the full sweep
        // guarantees no socket is starved even if a READABLE edge is missed.
        let all_tokens: Vec<usize> = broker.clients.keys().cloned().collect();
        for t in all_tokens {
            if !broker.clients.contains_key(&t) {
                continue;
            }
            if drain_client(&mut broker, t).is_err() {
                broker.remove_client(t);
            }
        }
        // drain queued new forwards from this round of reads
        let pending2: Vec<usize> = broker
            .clients
            .iter()
            .filter(|(_, c)| !c.write_queue.is_empty())
            .map(|(t, _)| *t)
            .collect();
        for t in pending2 {
            if broker.clients.contains_key(&t) {
                flush_writes(poll.registry(), &mut broker, t);
            }
        }
       // publish $SYS topics every 10 seconds
       if broker.sys_last_publish.elapsed() >= Duration::from_secs(10) {
           broker.publish_sys_topics();
           broker.sys_last_publish = Instant::now();
       }
        for t in dead {
            broker.remove_client(t);
        }
        for (t, pid, dup) in retry {
            let Some(c) = broker.clients.get_mut(&t) else { continue };
            let res = c.queue_qos1(dup);
            match res {
                Ok(()) => {
                    if let Some(entry) = c.in_flight.get_mut(&pid) {
                        entry.sent_at = now; // reset timer
                        entry.retries += 1;  // count retry
                        println!("[R] RETRY    pid {pid} -> {} (try {})", c.client_id, entry.retries);
                    }
                }
                Err(()) => {
                    // queue full of reliable traffic: cannot retry, drop client
                    println!("[!] DROP     {} (QoS1/2 queue full during retry)", c.client_id);
                    broker.remove_client(t);
                }
            }
        }
        for (t, pid) in give_up {
            if let Some(c) = broker.clients.get_mut(&t) {
                println!("[!] GIVEUP   {} pid {pid} after {QOS1_MAX_RETRIES} retries", c.client_id);
                broker.remove_client(t);
            }
        }
        // throttled state flush: persist dirty retained/session changes at most
        // every 200ms, off the packet hot path (see handle_packets note).
        if broker.dirty && broker.last_save.elapsed() >= Duration::from_millis(200) {
            broker.save();
        }
        let loop_ms = loop_start.elapsed().as_micros();
        if loop_ms > 1000 {
            eprintln!("[loop] {loop_ms}us, {} clients", broker.clients.len());
        }
    }
}
