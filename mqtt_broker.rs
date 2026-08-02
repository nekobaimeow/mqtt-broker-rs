// mqtt_broker.rs — 0-dependency MQTT 3.1.1 broker (pure std)
// Usage: mqtt_broker [addr]   (default 0.0.0.0:1883)
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

static NEXT_TOKEN: AtomicUsize = AtomicUsize::new(1);

// ---- packet types ----
const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

// ---- MQTT variable-length integer (remaining length) ----
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

fn decode_rem(stream: &mut TcpStream) -> Option<usize> {
    let mut rem: usize = 0;
    let mut mult: usize = 1;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).ok()?;
        rem += ((b[0] & 0x7f) as usize) * mult;
        if b[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    Some(rem)
}

fn read_packet(stream: &mut TcpStream) -> Option<(u8, u8, Vec<u8>)> {
    let mut hdr = [0u8; 1];
    stream.read_exact(&mut hdr).ok()?;
    let ptype = hdr[0] >> 4;
    let flags = hdr[0] & 0x0f;
    let len = decode_rem(stream)?;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).ok()?;
    Some((ptype, flags, body))
}

fn write_all(stream: &mut TcpStream, pkt: &[u8]) {
    let _ = stream.write_all(pkt);
}

fn build_connack() -> Vec<u8> {
    vec![(CONNACK << 4), 0x02, 0x00, 0x00] // session-present=0, rc=0
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
    vec![(UNSUBACK << 4), 0x02, pid.to_be_bytes()[0], pid.to_be_bytes()[1]]
}

fn build_puback(pid: u16) -> Vec<u8> {
    vec![(PUBACK << 4), 0x02, pid.to_be_bytes()[0], pid.to_be_bytes()[1]]
}

fn build_pingresp() -> Vec<u8> {
    vec![(PINGRESP << 4), 0x00]
}

// forward a PUBLISH to a subscriber, always downgraded to QoS 0
fn build_forward(topic: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![(PUBLISH << 4)]; // qos=0, dup=0, retain=0
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic);
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

// ---- topic filter matching (supports + and #) ----
fn topic_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    let mut i = 0;
    while i < f.len() {
        if f[i] == "#" {
            return true; // matches parent level and everything below
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

struct Subscription {
    filter: String,
    qos: u8,
    token: usize,
    tx: mpsc::SyncSender<Arc<Vec<u8>>>,
}

/// Bounded queue per subscriber (in packets). QoS0 delivery is at-most-once,
/// so when the queue is full we may legally drop the message — this is what
/// keeps memory bounded no matter how slow a subscriber is.
/// Sized to roughly match a TCP send buffer (~400KB @ 50B/msg) so bursts are
/// absorbed by the queue + kernel buffer, same effective buffering as
/// mosquitto, but strictly bounded instead of unbounded.
const SUB_QUEUE_CAP: usize = 8192;

// Thread stacks: reader/writer loops are shallow — std's default 2MB stacks
// waste virtual memory per connection (2 threads/conn). 256KB/64KB is plenty.
const READER_STACK: usize = 256 * 1024;
const WRITER_STACK: usize = 64 * 1024;

struct Broker {
    subs: Vec<Subscription>,
    client_ids: Vec<String>,
    drops: u64,
    dead_subs: u64,
}

impl Broker {
    fn new() -> Self {
        Broker { subs: Vec::new(), client_ids: Vec::new(), drops: 0, dead_subs: 0 }
    }

    fn subscribe(&mut self, filter: String, qos: u8, token: usize, tx: mpsc::SyncSender<Arc<Vec<u8>>>) {
        self.subs.push(Subscription { filter, qos, token, tx });
    }

    fn unsubscribe(&mut self, filter: &str, token: usize) {
        self.subs.retain(|s| !(s.filter == filter && s.token == token));
    }

    fn disconnect_client(&mut self, token: usize) {
        self.subs.retain(|s| s.token != token);
    }

    // returns (delivered, dropped). Never blocks: bounded queue + try_send.
    // Disconnected senders (writer thread died = client gone) are pruned here.
    // The forward packet is built ONCE and shared via Arc across all matching
    // subscribers — a 100-way fan-out stores 1 copy of the data, not 100.
    fn publish(&mut self, topic: &str, payload: &[u8]) -> (usize, usize) {
        let pkt = Arc::new(build_forward(topic.as_bytes(), payload));
        let mut delivered = 0usize;
        let mut dropped: u64 = 0;
        let mut dead: u64 = 0;
        self.subs.retain(|s| {
            if !topic_matches(&s.filter, topic) {
                return true;
            }
            match s.tx.try_send(Arc::clone(&pkt)) {
                Ok(()) => {
                    delivered += 1;
                    true
                }
                Err(mpsc::TrySendError::Full(_)) => {
                    dropped += 1;
                    true // queue full: legal QoS0 drop, keep subscriber
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    dead += 1;
                    false // writer thread gone: prune dead subscription
                }
            }
        });
        self.drops += dropped;
        self.dead_subs += dead;
        (delivered, dropped as usize)
    }
}

// ---- CONNECT parsing (returns client id + keepalive) ----
fn parse_connect(body: &[u8]) -> Option<(String, u16)> {
    if body.len() < 10 {
        return None;
    }
    let plen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let level = body[2 + plen];
    let _flags = body[3 + plen];
    let keepalive = u16::from_be_bytes([body[4 + plen], body[5 + plen]]);
    let mut pos = 6 + plen;
    // client id (protocol name + level + flags + keepalive = 6 bytes after name)
    let _ = level;
    if pos + 2 > body.len() {
        return None;
    }
    let clen = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2;
    if pos + clen > body.len() {
        return None;
    }
    let cid = String::from_utf8_lossy(&body[pos..pos + clen]).to_string();
    Some((cid, keepalive))
}

// parse SUBSCRIBE payload -> (packet_id, Vec<(filter, qos)>)
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

// parse UNSUBSCRIBE payload -> (packet_id, Vec<filter>)
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

// parse PUBLISH body -> (topic, qos, packet_id, payload)
fn parse_publish(flags: u8, body: &[u8]) -> Option<(String, u8, Option<u16>, Vec<u8>)> {
    if body.len() < 3 {
        return None;
    }
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let qos = (flags >> 1) & 0x03;
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
    Some((topic, qos, pid, payload))
}

fn handle_client(stream: TcpStream, broker: Arc<Mutex<Broker>>) {
    let mut stream = stream;
    let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    // Shrink kernel socket buffers: MQTT frames are small and bursts are
    // absorbed by the bounded user-space queue, so the default multi-hundred-KB
    // SO_RCVBUF/SO_SNDBUF just pin RSS per connection.
    let _ = stream.set_recv_buffer_size(16 * 1024);
    let _ = stream.set_send_buffer_size(16 * 1024);
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".into());

    // writer thread: owns a clone of the socket, drains the bounded queue.
    // On write error (client gone) it exits; rx drops and every queued
    // SyncSender turns Disconnected, so publish() prunes the subscription.
    let (tx, rx) = mpsc::sync_channel::<Arc<Vec<u8>>>(SUB_QUEUE_CAP);
    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    thread::Builder::new()
        .stack_size(WRITER_STACK)
        .spawn(move || {
            for pkt in rx {
                if writer.write_all(&pkt[..]).is_err() {
                    break;
                }
            }
        })
        .expect("spawn writer");

    let mut client_id = String::new();
    let mut keepalive_s: u64 = 0;
    loop {
        // MQTT keepalive: client must send something within keepalive secs.
        // Enforce it with a read timeout (1.5x grace) so zombie connections
        // can't pin memory forever.
        if keepalive_s > 0 {
            let _ = stream.set_read_timeout(Some(Duration::from_secs((keepalive_s as f64 * 1.5) as u64)));
        }
        let (ptype, flags, body) = match read_packet(&mut stream) {
            Some(x) => x,
            None => break,
        };
        match ptype {
            CONNECT => {
                match parse_connect(&body) {
                    Some((cid, ka)) => {
                        client_id = cid.clone();
                        keepalive_s = ka as u64;
                        let mut b = broker.lock().unwrap();
                        b.client_ids.push(cid.clone());
                        let n = b.client_ids.len();
                        drop(b);
                        tx.send(Arc::new(build_connack())).ok();
                        println!("[+] CONNECT  {}  ({peer})  [{n} clients]", cid);
                    }
                    None => {
                        println!("[!] bad CONNECT from {peer}");
                        break;
                    }
                }
            }
            SUBSCRIBE => {
                match parse_subscribe(&body) {
                    Some((pid, topics)) => {
                        let mut grants = Vec::new();
                        let mut b = broker.lock().unwrap();
                        for (filter, qos) in &topics {
                            b.subscribe(filter.clone(), *qos, token, tx.clone());
                            grants.push(*qos);
                            println!("[+] SUBSCRIBE {} -> {filter} (qos {qos})", client_id);
                        }
                        drop(b);
                        tx.send(Arc::new(build_suback(pid, &grants))).ok();
                    }
                    None => {
                        println!("[!] bad SUBSCRIBE from {peer}");
                        break;
                    }
                }
            }
            UNSUBSCRIBE => {
                match parse_unsubscribe(&body) {
                    Some((pid, filters)) => {
                        let mut b = broker.lock().unwrap();
                        for f in &filters {
                            b.unsubscribe(f, token);
                            println!("[-] UNSUBSCRIBE {} -> {f}", client_id);
                        }
                        drop(b);
                        tx.send(Arc::new(build_unsuback(pid))).ok();
                    }
                    None => break,
                }
            }
            PUBLISH => {
                match parse_publish(flags, &body) {
                    Some((topic, qos, pid, payload)) => {
                        // ack QoS1 inbound
                        if let Some(p) = pid {
                            if qos == 1 {
                                tx.send(Arc::new(build_puback(p))).ok();
                            }
                        }
                        let s = String::from_utf8_lossy(&payload);
                        let mut b = broker.lock().unwrap();
                        let (n, dropped) = b.publish(&topic, &payload);
                        let drops_total = b.drops;
                        let dead_total = b.dead_subs;
                        drop(b);
                        if dropped > 0 {
                            println!(
                                "[>] PUBLISH  {} -> {topic} (qos {qos}, {}B, delivered {n}, dropped {dropped}) \"{}\"",
                                client_id,
                                payload.len(),
                                s
                            );
                        } else {
                            println!(
                                "[>] PUBLISH  {} -> {topic} (qos {qos}, {}B, delivered to {n}) \"{}\"",
                                client_id,
                                payload.len(),
                                s
                            );
                        }
                        if dead_total > 0 {
                            println!("[!] pruned {dead_total} dead subscriptions (total drops: {drops_total})");
                        }
                    }
                    None => {
                        println!("[!] bad PUBLISH from {peer}");
                        break;
                    }
                }
            }
            PINGREQ => {
                tx.send(Arc::new(build_pingresp())).ok();
                println!("[~] PINGREQ  {} -> PINGRESP", client_id);
            }
            DISCONNECT => {
                println!("[-] DISCONNECT {} ({peer})", client_id);
                break;
            }
            _ => {
                println!("[?] unknown packet type {ptype} from {peer}");
                break;
            }
        }
    }
    let mut b = broker.lock().unwrap();
    b.disconnect_client(token);
    b.client_ids.retain(|c| *c != client_id);
    drop(b);
}

fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:1883".into());
    let listener = TcpListener::bind(&addr).expect("bind failed");
    let broker = Arc::new(Mutex::new(Broker::new()));
    println!("[mqtt-broker] listening on {addr} (0-dependency, pure std)");
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let b = Arc::clone(&broker);
                thread::Builder::new()
                    .stack_size(READER_STACK)
                    .spawn(move || handle_client(stream, b))
                    .expect("spawn reader");
            }
            Err(e) => println!("[!] accept error: {e}"),
        }
    }
}
