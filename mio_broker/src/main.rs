// mqtt_mio_broker — MQTT 3.1.1 broker, single-threaded event loop (mio 1.x)
// Architecture: one poll loop, no per-connection threads. Memory per
// connection is a few hundred bytes instead of 2 thread stacks + malloc arenas.
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

const LISTENER: Token = Token(0);
const READ_CAP: usize = 65536; // per-connection read buffer cap
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
    let mut pkt = vec![(SUBACK << 4)];
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(grants);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn build_unsuback(pid: u16) -> Vec<u8> {
    let mut pkt = vec![(UNSUBACK << 4)];
    let mut body = pid.to_be_bytes().to_vec();
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn build_puback(pid: u16) -> Vec<u8> {
    let mut pkt = vec![(PUBACK << 4)];
    let mut body = pid.to_be_bytes().to_vec();
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn build_pubrec(pid: u16) -> Vec<u8> {
    let mut pkt = vec![(PUBREC << 4)];
    let mut body = pid.to_be_bytes().to_vec();
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn build_pubrel(pid: u16) -> Vec<u8> {
    // PUBREL fixed header flags must be 0x02
    let mut pkt = vec![(PUBREL << 4) | 0x02];
    let mut body = pid.to_be_bytes().to_vec();
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn build_pubcomp(pid: u16) -> Vec<u8> {
    let mut pkt = vec![(PUBCOMP << 4)];
    let mut body = pid.to_be_bytes().to_vec();
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
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
    token: usize,
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

struct Broker {
    clients: HashMap<usize, Client>,
    subs: Vec<Subscription>,
    next_token: usize,
    drops: u64,
    dead_pruned: u64,
    retained: HashMap<String, (Vec<u8>, u8)>, // topic -> (payload, qos)
    // persistent sessions (clean session = 0), keyed by client_id
    sessions: HashMap<String, SessionState>,
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

impl Broker {
    fn new() -> Self {
        let now = Instant::now();
        Broker {
            clients: HashMap::new(),
            subs: Vec::new(),
            next_token: 1,
            drops: 0,
            dead_pruned: 0,
            retained: HashMap::new(),
            sessions: HashMap::new(),
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

    // forward to all matching subscribers; returns (delivered, dropped)
    // src_qos is the incoming PUBLISH QoS; per-subscriber forward QoS =
    // min(src_qos, sub.qos) per MQTT 3.1.1 §3.3.5. QoS1/2 forwards get a
    // packet id and are tracked in in_flight until acked. retain=true stores
    // (or clears, for empty payload) the message in the retained table.
    fn publish(&mut self, topic: &str, payload: &[u8], src_qos: u8, retain: bool) -> (usize, usize) {
        if retain {
            if payload.is_empty() {
                self.retained.remove(topic);
            } else {
                self.retained.insert(topic.to_string(), (payload.to_vec(), src_qos));
            }
        }
        let mut delivered = 0usize;
        let mut dead: u64 = 0;
        let mut i = 0;
        while i < self.subs.len() {
            if !topic_matches(&self.subs[i].filter, topic) {
                i += 1;
                continue;
            }
            let token = self.subs[i].token;
            let fwd_qos = src_qos.min(self.subs[i].qos);
            match self.deliver_to(token, topic, payload, fwd_qos, false) {
                Ok(()) => {
                    delivered += 1;
                    i += 1;
                }
                Err(()) => {
                    // client gone or too slow to accept reliable traffic:
                    // deliver_to already removed it; prune the subscription
                    self.subs.swap_remove(i);
                    dead += 1;
                }
            }
        }
        self.dead_pruned += dead;
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
            println!("[=] SESSION   {cid} persisted ({} subs, {} queued)", entry.subs.len(), entry.offline.len());
        }
       self.subs.retain(|s| s.token != token);
       self.clients.remove(&token);
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
    let (pkts, client_id, keepalive) = {
        let c = broker.clients.get_mut(&token).unwrap();
        let mut pkts = Vec::new();
        let mut consumed = 0usize;
        let mut bad = None;
        loop {
            let buf = &c.read_buf[consumed..];
            if buf.is_empty() {
                break;
            }
            let ptype = buf[0] >> 4;
            let flags = buf[0] & 0x0f;
            // decode remaining length
            let mut rem: usize = 0;
            let mut mult: usize = 1;
            let mut i = 1;
            let mut complete = false;
            while i < buf.len() && i <= 4 {
                rem += ((buf[i] & 0x7f) as usize) * mult;
                if buf[i] & 0x80 == 0 {
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
            if buf.len() < i + 1 + rem {
                break; // body incomplete, wait for more
            }
            let body = buf[i + 1..i + 1 + rem].to_vec();
            consumed += i + 1 + rem;
            pkts.push((ptype, flags, body));
        }
        if let Some(e) = bad {
            return Err(e);
        }
        // drain consumed bytes
        if consumed > 0 {
            c.read_buf.drain(..consumed);
        }
        (pkts, c.client_id.clone(), c.keepalive_deadline)
   };
   let pkt_count = pkts.len();

   for (ptype, flags, body) in pkts {
       match ptype {
            CONNECT => match parse_connect(&body) {
                Some((cid, ka, will_flag, will_qos, will_retain, will_topic, will_message, clean_session)) => {
                    // clean session = 1: discard any stored session for this id.
                    // clean session = 0: restore the stored session's subscriptions
                    // into the new client (offline queue is flushed below, after
                    // the subscription restore so forwards find the new token).
                    if clean_session {
                        broker.sessions.remove(&cid);
                    } else if let Some(sess) = broker.sessions.get(&cid) {
                        for (filter, qos) in &sess.subs {
                            broker.subs.push(Subscription { filter: filter.clone(), token, qos: *qos });
                            println!("[+] SESSION   {cid} restored sub {filter} (qos {qos})");
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
                        broker.sessions.remove(&cid); // delivered, session now lives on the socket
                    }
                    println!("[+] CONNECT  {cid}  (token {token}, clean={clean_session})");
                    broker.sys_clients_total += 1;
                }
                None => return Err("bad CONNECT".into()),
            },
            SUBSCRIBE => match parse_subscribe(&body) {
                Some((pid, topics)) => {
                    let mut grants = Vec::new();
                    for (filter, qos) in &topics {
                        broker.subs.push(Subscription { filter: filter.clone(), token, qos: *qos });
                        grants.push(*qos);
                        println!("[+] SUBSCRIBE {} -> {filter} (qos {qos})", client_id);
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
                    for f in &filters {
                        println!("[-] UNSUBSCRIBE {} -> {f}", client_id);
                    }
                    let c = broker.clients.get_mut(&token).unwrap();
                    c.queue_qos0(build_unsuback(pid));
                }
                None => return Err("bad UNSUBSCRIBE".into()),
            },
            PUBLISH => match parse_publish(flags, &body) {
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
                        println!("[>] PUBLISH  {} -> {topic} (qos {qos}{}{}, {}B, delivered to {n}) \"{}\"",
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
            },
            PUBACK => {
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let c = broker.clients.get_mut(&token).unwrap();
                    if c.in_flight.remove(&pid).is_some() {
                        println!("[~] PUBACK   {} -> pid {pid}", client_id);
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
                        println!("[~] PUBREC   {} -> pid {pid} (-> PUBREL)", client_id);
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
                    println!("[~] PUBREL   {} -> pid {pid} (-> PUBCOMP)", client_id);
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
                        println!("[~] PUBCOMP  {} -> pid {pid}", client_id);
                    }
                } else {
                    return Err("bad PUBCOMP".into());
                }
            }
            PINGREQ => {
                let c = broker.clients.get_mut(&token).unwrap();
                c.queue_qos0(build_pingresp());
                println!("[~] PINGREQ  {} -> PINGRESP", client_id);
            }
            DISCONNECT => {
                println!("[-] DISCONNECT {} (token {token})", client_id);
                let c = broker.clients.get_mut(&token).unwrap();
                c.clean_disconnect = true;
                return Err("disconnect".into());
            }
            _ => return Err(format!("unknown packet type {ptype}")),
        }
    }
   let _ = keepalive;
    broker.sys_msgs_received += pkt_count as u64;
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
        let _ = registry.reregister(&mut c.stream, Token(token), interest);
    }
}

// Read ONE chunk from a client and parse it. Returning means the caller should
// flush writes (read and write must interleave, otherwise a burst publisher
// fills every subscriber queue and everything gets dropped before any flush).
// Returns Err if the connection must be closed (EOF, protocol error, flood).
fn drain_client(broker: &mut Broker, token: usize) -> Result<(), ()> {
    let mut buf = [0u8; 8192];
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
    println!("[mqtt-mio-broker] listening on {addr} (mio {})", env!("CARGO_PKG_VERSION"));

    let mut idle_rounds: u32 = 0;
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
                                    token,
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
                                let _ = poll.registry().reregister(
                                    &mut c.stream,
                                    Token(token),
                                    Interest::READABLE,
                                );
                            }
                        }
                    }
                }
            }
        }
        // eagerly flush any client with queued data (non-blocking). The
        // WRITABLE interest is only a fallback for when the kernel buffer is
        // full — relying on epoll to notice writability adds latency per batch.
        let pending: Vec<usize> = broker
            .clients
            .iter()
            .filter(|(_, c)| !c.write_queue.is_empty())
            .map(|(t, _)| *t)
            .collect();
        for t in pending {
            if broker.clients.contains_key(&t) {
                flush_writes(poll.registry(), &mut broker, t);
            }
        }
        // belt & braces: poll timeout also wakes us to actively drain every
        // client, in case an epoll READABLE notification is missed
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
      // keepalive sweep
      let now = Instant::now();
       // publish $SYS topics every 10 seconds
       if broker.sys_last_publish.elapsed() >= Duration::from_secs(10) {
           broker.publish_sys_topics();
           broker.sys_last_publish = Instant::now();
       }
        let dead: Vec<usize> = broker
            .clients
            .iter()
            .filter(|(_, c)| match c.keepalive_deadline {
                Some(d) => now > d,
                None => false,
            })
            .map(|(t, _)| *t)
            .collect();
        for t in dead {
            broker.remove_client(t);
        }
        // retransmission sweep: any in-flight message older than
        // QOS1_RETRY_AFTER gets re-queued with DUP=1 (QoS1: PUBLISH;
        // QoS2 awaiting PUBREC: PUBLISH; QoS2 awaiting PUBCOMP: PUBREL).
        // After QOS1_MAX_RETRIES unanswered retries the client is dropped.
        let mut retry: Vec<(usize, u16, Vec<u8>)> = Vec::new();
        let mut give_up: Vec<(usize, u16)> = Vec::new();
        for (t, c) in broker.clients.iter() {
            for (pid, inf) in c.in_flight.iter() {
                if now.duration_since(inf.sent_at) < qos1_retry_after() {
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
        let loop_ms = loop_start.elapsed().as_micros();
        if loop_ms > 1000 {
            eprintln!("[loop] {loop_ms}us, {} clients", broker.clients.len());
        }
    }
}
