// mqtt_qos2_retain_test.rs — QoS2 handshake + retained-message tests
// Scenarios:
//  1. QoS2 outbound handshake: sub(qos2) <- pub(qos2): PUBLISH->PUBREC->PUBREL->PUBCOMP
//  2. inbound QoS2 dedup: publisher re-sends same PID -> broker forwards once
//  3. QoS2 retransmit: no PUBREC -> DUP PUBLISH; no PUBCOMP -> DUP PUBREL
//  4. retain: stored, delivered to new subscriber with RETAIN flag
//  5. retain overwrite: new value wins
//  6. retain clear: empty payload removes
//  7. retain wildcard: '#' / '+/#' filters match
//  8. retain QoS downgrade: retained at qos2, sub qos0 -> QoS0 delivery
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn enc(mut len: usize, b: &mut Vec<u8>) {
    loop {
        let mut x = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            x |= 0x80;
        }
        b.push(x);
        if len == 0 {
            break;
        }
    }
}
fn conn(addr: &str, cid: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(0x02);
    body.extend_from_slice(&60u16.to_be_bytes());
    body.extend_from_slice(&(cid.len() as u16).to_be_bytes());
    body.extend_from_slice(cid.as_bytes());
    enc(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    s.write_all(&pkt).unwrap();
    let mut b = [0u8; 4];
    s.read_exact(&mut b).unwrap();
    s
}
fn sub(s: &mut TcpStream, pid: u16, filter: &str, qos: u8) {
    let mut pkt = vec![0x82];
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    body.push(qos);
    enc(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    s.write_all(&pkt).unwrap();
}
fn pub_pkt(pid: u16, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Vec<u8> {
    let mut flags = 0x30 | ((qos & 3) << 1);
    if retain {
        flags |= 0x01;
    }
    let mut pkt = vec![flags];
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    enc(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}
fn ack_pkt(ptype: u8, pid: u16) -> Vec<u8> {
    let flags = if ptype == 6 { 0x02 } else { 0x00 }; // PUBREL needs flags=2
    vec![(ptype << 4) | flags, 0x02, (pid >> 8) as u8, (pid & 0xff) as u8]
}
// read one packet; returns (first_byte, body)
fn recv(s: &mut TcpStream, timeout: Duration) -> Option<(u8, Vec<u8>)> {
    s.set_read_timeout(Some(timeout)).ok();
    let mut h = [0u8; 1];
    match s.read_exact(&mut h) {
        Ok(_) => {}
        Err(_) => return None,
    }
    let b0 = h[0];
    let mut rem: usize = 0;
    let mut mult = 1usize;
    let mut lb = [0u8; 1];
    loop {
        s.read_exact(&mut lb).unwrap();
        rem += ((lb[0] & 0x7f) as usize) * mult;
        if lb[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut body = vec![0u8; rem];
    s.read_exact(&mut body).unwrap();
    Some((b0, body))
}
fn ptype(b0: u8) -> u8 {
    b0 >> 4
}
// parse forwarded PUBLISH: returns (topic, qos, pid, payload, retain)
fn parse_pub(body: &[u8], qos: u8) -> (String, Option<u16>, Vec<u8>) {
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
    let mut pos = 2 + tlen;
    let pid = if qos > 0 {
        let p = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        Some(p)
    } else {
        None
    };
    (topic, pid, body[pos..].to_vec())
}


// subscribe then read until SUBACK; return any PUBLISH packets seen first
// (broker queues retained deliveries BEFORE the SUBACK)
fn sub_until_ack(s: &mut TcpStream, pid: u16, filter: &str, qos: u8) -> Vec<(u8, Vec<u8>)> {
    sub(s, pid, filter, qos);
    let mut pubs = Vec::new();
    loop {
        match recv(s, Duration::from_secs(2)) {
            Some((b0, body)) if ptype(b0) == 9 => break, // SUBACK
            Some(p) => pubs.push(p),
            None => break,
        }
    }
    pubs
}

fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:11883".into());
    let mut pass = 0;
    let mut fail = 0;
    let mut check = |name: &str, cond: bool| {
        if cond {
            pass += 1;
            println!("  [PASS] {name}");
        } else {
            fail += 1;
            println!("  [FAIL] {name}");
        }
    };

    println!("[1] QoS2 outbound handshake");
    {
        let mut s = conn(&addr, "q2-sub");
        let mut p = conn(&addr, "q2-pub");
        sub(&mut s, 1, "q2/t", 2);
        recv(&mut s, Duration::from_secs(2)); // SUBACK
        p.write_all(&pub_pkt(50, "q2/t", b"exactly-once", 2, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC to publisher
        let fwd = recv(&mut s, Duration::from_secs(2));
        let (topic, fpid, payload) = match fwd {
            Some((b0, body)) if ptype(b0) == 3 => parse_pub(&body, (b0 >> 1) & 3),
            _ => (String::new(), None, Vec::new()),
        };
        check("subscriber got QoS2 PUBLISH", topic == "q2/t" && fpid.is_some());
        check("payload intact", payload == b"exactly-once");
        if let Some(pid) = fpid {
            s.write_all(&ack_pkt(5, pid)).unwrap(); // PUBREC
            let rel = recv(&mut s, Duration::from_secs(2));
            check("broker sends PUBREL", rel.as_ref().map(|(b0, b)| ptype(*b0) == 6 && b.len() == 2).unwrap_or(false));
            if let Some((b0, b)) = rel {
                if ptype(b0) == 6 {
                    let rpid = u16::from_be_bytes([b[0], b[1]]);
                    s.write_all(&ack_pkt(7, rpid)).unwrap(); // PUBCOMP
                }
            }
        }
        // publisher side: PUBREL -> PUBCOMP
        p.write_all(&ack_pkt(6, 50)).unwrap();
        let comp = recv(&mut p, Duration::from_secs(2));
        check("publisher completes with PUBCOMP", comp.map(|(b0, _)| ptype(b0) == 7).unwrap_or(false));
    }

    println!("[2] inbound QoS2 dedup (publisher retry)");
    {
        let mut s = conn(&addr, "dd-sub");
        let mut p = conn(&addr, "dd-pub");
        sub(&mut s, 2, "dd/t", 0);
        recv(&mut s, Duration::from_secs(2));
        // send QoS2 pid=70, then re-send SAME pid (simulating lost PUBREC retry)
        p.write_all(&pub_pkt(70, "dd/t", b"once", 2, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC
        p.write_all(&pub_pkt(70, "dd/t", b"once", 2, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC again
        p.write_all(&ack_pkt(6, 70)).unwrap(); // PUBREL
        recv(&mut p, Duration::from_secs(2)); // PUBCOMP
        // subscriber must see exactly ONE delivery
        let a = recv(&mut s, Duration::from_secs(2));
        let b = recv(&mut s, Duration::from_millis(300));
        check("dedup: exactly one delivery", a.is_some() && b.is_none());
        // now reuse pid 70 for a fresh message -> must be delivered
        p.write_all(&pub_pkt(70, "dd/t", b"fresh", 2, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC
        p.write_all(&ack_pkt(6, 70)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBCOMP
        let c = recv(&mut s, Duration::from_secs(2));
        check("pid reusable after PUBREL", c.is_some());
    }

    println!("[3] QoS2 retransmit states");
    {
        let mut s = conn(&addr, "rt-sub");
        let mut p = conn(&addr, "rt-pub");
        sub(&mut s, 3, "rt/t", 2);
        recv(&mut s, Duration::from_secs(2));
        p.write_all(&pub_pkt(80, "rt/t", b"retry", 2, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC
        let first = recv(&mut s, Duration::from_secs(2));
        check("first QoS2 delivery", first.map(|(b0, _)| ptype(b0) == 3).unwrap_or(false));
        // don't PUBREC: broker must re-send PUBLISH with DUP
        let dup1 = recv(&mut s, Duration::from_secs(5));
        check("DUP PUBLISH retransmit", dup1.as_ref().map(|(b0, _)| ptype(*b0) == 3 && (*b0 & 0x08) != 0).unwrap_or(false));
        // now ack with PUBREC, then withhold PUBCOMP: broker sends DUP PUBREL
        if let Some((b0, body)) = &dup1 {
            if ptype(*b0) == 3 {
                let (_, pid, _) = parse_pub(&body, 2);
                if let Some(pid) = pid {
                    s.write_all(&ack_pkt(5, pid)).unwrap();
                    let rel = recv(&mut s, Duration::from_secs(2));
                    check("PUBREL sent after PUBREC", rel.map(|(b0, _)| ptype(b0) == 6).unwrap_or(false));
                    let rel2 = recv(&mut s, Duration::from_secs(5));
                    check("DUP PUBREL retransmit", rel2.as_ref().map(|(b0, _)| ptype(*b0) == 6 && (*b0 & 0x08) != 0).unwrap_or(false));
                    if let Some((rb0, rbody)) = &rel2 {
                        if ptype(*rb0) == 6 {
                            let rpid = u16::from_be_bytes([rbody[0], rbody[1]]);
                            s.write_all(&ack_pkt(7, rpid)).unwrap();
                        }
                    }
                }
            }
        }
    }

    println!("[4] retain: stored + delivered on subscribe");
    {
        let mut p = conn(&addr, "r1-pub");
        p.write_all(&pub_pkt(0, "rtn/status", b"online", 1, true)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBACK
        let mut s = conn(&addr, "r1-sub");
        let pubs = sub_until_ack(&mut s, 4, "rtn/status", 0);
        match pubs.first() {
            Some((b0, body)) if ptype(*b0) == 3 => {
                let retain_flag = *b0 & 0x01 != 0;
                let (_, _, payload) = parse_pub(body, (b0 >> 1) & 3);
                check("retain delivered on subscribe", payload == b"online");
                check("RETAIN flag set", retain_flag);
            }
            _ => {
                check("retain delivered on subscribe", false);
                check("RETAIN flag set", false);
            }
        }
    }

    println!("[5] retain overwrite");
    {
        let mut p = conn(&addr, "r2-pub");
        p.write_all(&pub_pkt(0, "rtn/status", b"offline", 1, true)).unwrap();
        recv(&mut p, Duration::from_secs(2));
        let mut s = conn(&addr, "r2-sub");
        let pubs = sub_until_ack(&mut s, 5, "rtn/status", 0);
        let payload = match pubs.first() {
            Some((b0, body)) if ptype(*b0) == 3 => parse_pub(body, 0).2,
            _ => Vec::new(),
        };
        check("overwritten retain wins", payload == b"offline");
    }

    println!("[6] retain clear (empty payload)");
    {
        let mut p = conn(&addr, "r3-pub");
        p.write_all(&pub_pkt(0, "rtn/status", b"", 1, true)).unwrap();
        recv(&mut p, Duration::from_secs(2));
        let mut s = conn(&addr, "r3-sub");
        let pubs = sub_until_ack(&mut s, 6, "rtn/status", 0);
        check("cleared retain not delivered", pubs.is_empty());
    }

    println!("[7] retain wildcard subscribe");
    {
        let mut p = conn(&addr, "r4-pub");
        p.write_all(&pub_pkt(0, "sensor/room1/temp", b"23.5", 1, true)).unwrap();
        recv(&mut p, Duration::from_secs(2));
        let mut s = conn(&addr, "r4-sub");
        let pubs = sub_until_ack(&mut s, 7, "sensor/+/temp", 0);
        let ok = match pubs.first() {
            Some((b0, body)) if ptype(*b0) == 3 => {
                let (t, _, payload) = parse_pub(body, 0);
                t == "sensor/room1/temp" && payload == b"23.5"
            }
            _ => false,
        };
        check("wildcard '+' matches retained", ok);
    }

    println!("[8] retain QoS downgrade (retained qos2 -> sub qos0)");
    {
        let mut p = conn(&addr, "r5-pub");
        p.write_all(&pub_pkt(0, "rtn/q", b"q2stored", 2, true)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBREC
        p.write_all(&ack_pkt(6, 0)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBCOMP
        let mut s = conn(&addr, "r5-sub");
        let pubs = sub_until_ack(&mut s, 8, "rtn/q", 0);
        match pubs.first() {
            Some((b0, body)) if ptype(*b0) == 3 => {
                let q = (*b0 >> 1) & 3;
                let (_, _, payload) = parse_pub(body, q);
                check("delivered as QoS0", q == 0);
                check("payload intact", payload == b"q2stored");
            }
            _ => {
                check("delivered as QoS0", false);
                check("payload intact", false);
            }
        }
    }

    println!("[9] QoS0/1 regression");
    {
        let mut s = conn(&addr, "rg-sub");
        let mut p = conn(&addr, "rg-pub");
        sub(&mut s, 9, "rg/t", 1);
        recv(&mut s, Duration::from_secs(2));
        p.write_all(&pub_pkt(1, "rg/t", b"q1", 1, false)).unwrap();
        recv(&mut p, Duration::from_secs(2)); // PUBACK
        let m = recv(&mut s, Duration::from_secs(2));
        match m {
            Some((b0, body)) if ptype(b0) == 3 => {
                let q = (b0 >> 1) & 3;
                let (_, pid, payload) = parse_pub(&body, q);
                check("QoS1 forward", q == 1 && pid.is_some() && payload == b"q1");
            }
            _ => check("QoS1 forward", false),
        }
    }

    println!("\n==== QoS2+retain test result: {pass} pass, {fail} fail ====");
    if fail > 0 {
        std::process::exit(1);
    }
}
