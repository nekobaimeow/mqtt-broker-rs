// mqtt_session_test.rs — persistent session (clean session = 0) integration tests
// Scenarios:
//  1. offline queue: sub(qos1, clean=0) -> disconnect -> pub(qos1) -> reconnect(clean=0) -> receive
//  2. QoS0 not stored: sub(qos1, clean=0) -> disconnect -> pub(qos0) -> reconnect -> nothing
//  3. clean=1 wipes session: sub(clean=0) -> disconnect -> reconnect(clean=1) -> pub -> reconnect(clean=0) -> nothing
//  4. subscription restore: sub(clean=0) -> disconnect -> reconnect(clean=0, no re-sub) -> pub -> receive
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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

fn pkt_connect(cid: &str, clean: bool) -> Vec<u8> {
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(if clean { 0x02 } else { 0x00 });
    body.extend_from_slice(&60u16.to_be_bytes());
    let c = cid.as_bytes();
    body.extend_from_slice(&(c.len() as u16).to_be_bytes());
    body.extend_from_slice(c);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_subscribe(pid: u16, filter: &str, qos: u8) -> Vec<u8> {
    let mut pkt = vec![0x82];
    let mut body = pid.to_be_bytes().to_vec();
    let f = filter.as_bytes();
    body.extend_from_slice(&(f.len() as u16).to_be_bytes());
    body.extend_from_slice(f);
    body.push(qos);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_publish(qos: u8, pid: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![0x30 | (qos << 1)];
    let mut body = Vec::new();
    let t = topic.as_bytes();
    body.extend_from_slice(&(t.len() as u16).to_be_bytes());
    body.extend_from_slice(t);
    if qos > 0 {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn read_pkt(s: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut head = [0u8; 1];
    if s.read_exact(&mut head).is_err() {
        return None;
    }
    let mut len = 0usize;
    let mut mult = 1usize;
    loop {
        let mut b = [0u8; 1];
        if s.read_exact(&mut b).is_err() {
            return None;
        }
        len += (b[0] & 0x7f) as usize * mult;
        if b[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut body = vec![0u8; len];
    if len > 0 && s.read_exact(&mut body).is_err() {
        return None;
    }
    Some((head[0], body))
}

fn connect(addr: &str, cid: &str, clean: bool) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_nodelay(true).unwrap();
    s.write_all(&pkt_connect(cid, clean)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if let Some((ptype, _)) = read_pkt(&mut s) {
            if ptype == 0x20 {
                return s;
            }
        }
    }
    panic!("no CONNACK for {cid}");
}

fn expect_publish(s: &mut TcpStream, topic: &str, payload: &[u8], ms: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    s.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    while std::time::Instant::now() < deadline {
        if let Some((ptype, body)) = read_pkt(s) {
            if ptype >> 4 == 3 {
                let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
                let t = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
                let mut off = 2 + tlen;
                if (ptype & 0x06) >> 1 > 0 {
                    off += 2;
                }
                if t == topic && &body[off..] == payload {
                    return true;
                }
            }
        }
    }
    false
}

fn expect_nothing(s: &mut TcpStream, ms: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    s.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    while std::time::Instant::now() < deadline {
        if let Some((ptype, _)) = read_pkt(s) {
            // CONNACK/SUBACK ok; anything else (esp. PUBLISH type 3) is a failure
            if ptype >> 4 == 3 {
                return false;
            }
        }
    }
    true
}

fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:11883".into());
    let mut pass = 0;
    let mut fail = 0;
    let mut check = |name: &str, ok: bool| {
        if ok {
            pass += 1;
            println!("PASS  {name}");
        } else {
            fail += 1;
            println!("FAIL  {name}");
        }
    };

    // ---- 1. offline QoS1 queue delivered on reconnect ----
    {
        let mut a = connect(&addr, "sess1", false);
        a.write_all(&pkt_subscribe(1, "off/topic", 1)).unwrap();
        let _ = read_pkt(&mut a); // SUBACK
        drop(a); // abnormal disconnect, session persisted

        // publisher sends QoS1 while sess1 is away
        let mut p = connect(&addr, "pub1", true);
        p.write_all(&pkt_publish(1, 10, "off/topic", b"queued-msg")).unwrap();
        let _ = read_pkt(&mut p); // PUBACK
        drop(p);

        // reconnect with clean=0 -> should get queued message
        let mut a2 = connect(&addr, "sess1", false);
        let got = expect_publish(&mut a2, "off/topic", b"queued-msg", 2000);
        check("offline QoS1 queued and delivered on reconnect", got);
        drop(a2);
    }

    // ---- 2. QoS0 not stored offline ----
    {
        let mut a = connect(&addr, "sess2", false);
        a.write_all(&pkt_subscribe(2, "q0/topic", 1)).unwrap();
        let _ = read_pkt(&mut a);
        drop(a);

        let mut p = connect(&addr, "pub2", true);
        p.write_all(&pkt_publish(0, 0, "q0/topic", b"fire-and-forget")).unwrap();
        drop(p);

        let mut a2 = connect(&addr, "sess2", false);
        let silent = expect_nothing(&mut a2, 800);
        check("QoS0 not stored for offline session", silent);
        drop(a2);
    }

    // ---- 3. clean=1 wipes the session ----
    {
        let mut a = connect(&addr, "sess3", false);
        a.write_all(&pkt_subscribe(3, "wipe/topic", 1)).unwrap();
        let _ = read_pkt(&mut a);
        drop(a);

        // reconnect with clean=1: session must be discarded
        let mut a2 = connect(&addr, "sess3", true);
        drop(a2);

        let mut p = connect(&addr, "pub3", true);
        p.write_all(&pkt_publish(1, 11, "wipe/topic", b"gone")).unwrap();
        let _ = read_pkt(&mut p);
        drop(p);

        // reconnect clean=0: no stored session, nothing queued
        let mut a3 = connect(&addr, "sess3", false);
        let silent = expect_nothing(&mut a3, 800);
        check("clean=1 wipes stored session", silent);
        drop(a3);
    }

    // ---- 4. subscription restored on reconnect ----
    {
        let mut a = connect(&addr, "sess4", false);
        a.write_all(&pkt_subscribe(4, "restore/topic", 1)).unwrap();
        let _ = read_pkt(&mut a);
        drop(a);

        // reconnect clean=0 WITHOUT re-subscribing, then a publisher fires
        let mut a2 = connect(&addr, "sess4", false);
        let mut p = connect(&addr, "pub4", true);
        p.write_all(&pkt_publish(1, 12, "restore/topic", b"restored-sub")).unwrap();
        let _ = read_pkt(&mut p);
        let got = expect_publish(&mut a2, "restore/topic", b"restored-sub", 2000);
        check("subscriptions restored on reconnect", got);
        drop(a2);
        drop(p);
    }

    println!("----");
    println!("{pass} passed, {fail} failed");
    if fail > 0 {
        std::process::exit(1);
    }
}
