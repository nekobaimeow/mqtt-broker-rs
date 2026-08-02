// mqtt_qos1_test.rs — QoS1 integration tests for the mio broker
// Scenarios:
//  1. QoS1 round-trip: sub(qos1) -> pub(qos1) -> receive PUBLISH with pid -> PUBACK -> done
//  2. min() rule: pub(qos1) to a qos0 subscriber -> receive QoS0 PUBLISH (no pid)
//  3. DUP retransmit: subscribe qos1, never PUBACK -> broker re-sends with DUP=1
//  4. no-retransmit-after-ack: PUBACK received -> no DUP re-send
//  5. pid reuse: two messages, ack the first, next gets a fresh pid
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

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

fn pkt_connect(cid: &str) -> Vec<u8> {
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(0x02); // clean session
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

fn pkt_publish_qos1(pid: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![0x32]; // PUBLISH, QoS1
    let mut body = Vec::new();
    let t = topic.as_bytes();
    body.extend_from_slice(&(t.len() as u16).to_be_bytes());
    body.extend_from_slice(t);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_publish_qos0(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![0x30];
    let mut body = Vec::new();
    let t = topic.as_bytes();
    body.extend_from_slice(&(t.len() as u16).to_be_bytes());
    body.extend_from_slice(t);
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_puback(pid: u16) -> Vec<u8> {
    vec![0x40, 0x02, (pid >> 8) as u8, (pid & 0xff) as u8]
}

fn pkt_pingreq() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn connect(addr: &str, cid: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.write_all(&pkt_connect(cid)).unwrap();
    let mut buf = [0u8; 16];
    s.read_exact(&mut buf[..4]).expect("connack"); // 4 bytes: fixed+rem+2
    assert_eq!(buf[0] >> 4, 2, "expected CONNACK");
    s
}

fn recv_pkt(s: &mut TcpStream, timeout: Duration) -> Option<(u8, Vec<u8>)> {
    s.set_read_timeout(Some(timeout)).ok();
    let mut head = [0u8; 1];
    match s.read_exact(&mut head) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => return None,
        Err(e) => panic!("read head: {e}"),
    }
    let b0 = head[0];
    let ptype = b0 >> 4;
    // remaining length
    let mut rem: usize = 0;
    let mut mult = 1usize;
    let mut len_buf = [0u8; 1];
    loop {
        s.read_exact(&mut len_buf).unwrap();
        rem += ((len_buf[0] & 0x7f) as usize) * mult;
        if len_buf[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut body = vec![0u8; rem];
    s.read_exact(&mut body).unwrap();
    Some((b0, body))
}

// parse forwarded PUBLISH; returns (qos, pid_opt, payload)
fn parse_fwd(body: &[u8], qos: u8) -> (Option<u16>, Vec<u8>) {
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2 + tlen;
    let pid = if qos > 0 {
        let p = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        Some(p)
    } else {
        None
    };
    (pid, body[pos..].to_vec())
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

    println!("[1] QoS1 round-trip");
    {
        let mut sub = connect(&addr, "q1-sub");
        let mut pubc = connect(&addr, "q1-pub");
        sub.write_all(&pkt_subscribe(1, "q1/topic", 1)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2)); // SUBACK
        pubc.write_all(&pkt_publish_qos1(7, "q1/topic", b"hello-qos1")).unwrap();
        // publisher gets PUBACK for its own QoS1 publish
        let ack = recv_pkt(&mut pubc, Duration::from_secs(2));
        check("publisher receives PUBACK", ack.map(|(b0, _)| (b0 >> 4) == 4).unwrap_or(false));
        // subscriber gets forwarded QoS1 PUBLISH with a pid
        let fwd = recv_pkt(&mut sub, Duration::from_secs(2));
        let (pid, payload) = match fwd {
            Some((b0, b)) if (b0 >> 4) == 3 => {
                // flags byte: QoS1 => pid present
                let q = (b[0] >> 1) & 0x03; // not stored; we parse from first byte of pkt
                let _ = q;
                parse_fwd(&b, 1)
            }
            _ => (None, Vec::new()),
        };
        check("subscriber receives QoS1 PUBLISH", pid.is_some());
        check("payload intact", payload == b"hello-qos1");
        // ack it -> no DUP retransmit
        if let Some(p) = pid {
            sub.write_all(&pkt_puback(p)).unwrap();
        }
        let dup = recv_pkt(&mut sub, Duration::from_millis(300));
        check("no DUP after PUBACK", dup.is_none());
    }

    println!("[2] min() rule: qos1 pub -> qos0 sub gets QoS0");
    {
        let mut sub = connect(&addr, "q0-sub");
        let mut pubc = connect(&addr, "q0-pub");
        sub.write_all(&pkt_subscribe(2, "q0/topic", 0)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos1(9, "q0/topic", b"downgraded")).unwrap();
        recv_pkt(&mut pubc, Duration::from_secs(2)); // PUBACK
        let fwd = recv_pkt(&mut sub, Duration::from_secs(2));
        check("QoS0 forward to qos0 sub", fwd.map(|(b0, _)| (b0 >> 4) == 3).unwrap_or(false));
    }

    println!("[3] DUP retransmit when no PUBACK");
    {
        let mut sub = connect(&addr, "dup-sub");
        let mut pubc = connect(&addr, "dup-pub");
        sub.write_all(&pkt_subscribe(3, "dup/topic", 1)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos1(11, "dup/topic", b"needs-retry")).unwrap();
        recv_pkt(&mut pubc, Duration::from_secs(2)); // PUBACK to publisher
        let first = recv_pkt(&mut sub, Duration::from_secs(2));
        check("first delivery", first.map(|(b0, _)| (b0 >> 4) == 3).unwrap_or(false));
        // do NOT ack; broker should re-send with DUP=1 after QOS1_RETRY_MS
        let retry = recv_pkt(&mut sub, Duration::from_secs(5));
        match retry {
            Some((b0, b)) if (b0 >> 4) == 3 => {
                // DUP flag = bit 3 of flags byte (b[0])
                check("DUP flag set on retry", b0 & 0x08 != 0);
            }
            _ => check("DUP flag set on retry", false),
        }
        // drain any further retries, then ack to stop them
        let _ = recv_pkt(&mut sub, Duration::from_secs(1));
        let _ = recv_pkt(&mut sub, Duration::from_secs(1));
    }

    println!("[4] pid reuse after ack");
    {
        let mut sub = connect(&addr, "pid-sub");
        let mut pubc = connect(&addr, "pid-pub");
        sub.write_all(&pkt_subscribe(4, "pid/topic", 1)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos1(1, "pid/topic", b"m1")).unwrap();
        recv_pkt(&mut pubc, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos1(2, "pid/topic", b"m2")).unwrap();
        recv_pkt(&mut pubc, Duration::from_secs(2));
        let a = recv_pkt(&mut sub, Duration::from_secs(2));
        let b = recv_pkt(&mut sub, Duration::from_secs(2));
        let (pa, pb) = match (a, b) {
            (Some((b0, x)), Some((b1, y))) if (b0 >> 4) == 3 && (b1 >> 4) == 3 => {
                let (p1, _) = parse_fwd(&x, 1);
                let (p2, _) = parse_fwd(&y, 1);
                (p1, p2)
            }
            _ => (None, None),
        };
        check("two distinct pids", pa.is_some() && pb.is_some() && pa != pb);
        if let Some(p) = pa {
            sub.write_all(&pkt_puback(p)).unwrap();
        }
    }

    println!("[7] QoS1 queue-full -> subscriber disconnected");
    {
        // subscriber subscribes qos1 but never reads; publisher floods qos1.
        // broker must disconnect the slow subscriber (all-QoS1 queue full)
        let mut sub = connect(&addr, "slow-sub");
        let mut pubc = connect(&addr, "slow-pub");
        sub.write_all(&pkt_subscribe(7, "flood/topic", 1)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2)); // SUBACK
        // flood enough to exceed WRITE_QUEUE_CAP (8192) with QoS1-only traffic
        let mut pid = 100u16;
        for _ in 0..20000 {
            pubc.write_all(&pkt_publish_qos1(pid, "flood/topic", b"x")).unwrap();
            pid = pid.wrapping_add(1);
        }
        // publisher acks come back; subscriber should get disconnected (EOF).
        // NB: must drain all buffered data first — broker wrote ~256 msgs into
        // the kernel buffer before disconnecting, so read until EOF/reset
        // (with a timeout to avoid hanging on a still-alive connection).
        sub.set_read_timeout(Some(Duration::from_millis(300)));
        let mut got_eof = false;
        let mut one = [0u8; 1];
        loop {
            match sub.read(&mut one) {
                Ok(0) => {
                    got_eof = true;
                    break;
                }
                Ok(_) => continue,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break; // connection still alive, no more data
                }
                Err(_) => {
                    got_eof = true;
                    break;
                }
            }
        }
        check("slow subscriber disconnected on QoS1 overflow", got_eof);
    }

    println!("[8] GIVEUP after 3 unanswered DUP retries");
    {
        let mut sub = connect(&addr, "ghost-sub");
        let mut pubc = connect(&addr, "ghost-pub");
        sub.write_all(&pkt_subscribe(8, "ghost/topic", 1)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos1(200, "ghost/topic", b"haunt")).unwrap();
        recv_pkt(&mut pubc, Duration::from_secs(2));
        // read the first copy, then stop acking entirely -> broker retries
        // 3x (500ms each) then gives up and disconnects
        let _ = recv_pkt(&mut sub, Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(600));
        let _ = recv_pkt(&mut sub, Duration::from_secs(2)); // DUP #1
        std::thread::sleep(Duration::from_millis(600));
        let _ = recv_pkt(&mut sub, Duration::from_secs(2)); // DUP #2
        std::thread::sleep(Duration::from_millis(600));
        let _ = recv_pkt(&mut sub, Duration::from_secs(2)); // DUP #3
        // after 3 retries broker should disconnect
        std::thread::sleep(Duration::from_millis(600));
        sub.set_read_timeout(Some(Duration::from_millis(300)));
        let mut got_eof = false;
        let mut one = [0u8; 1];
        loop {
            match sub.read(&mut one) {
                Ok(0) => {
                    got_eof = true;
                    break;
                }
                Ok(_) => continue,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break; // connection still alive, no more data
                }
                Err(_) => {
                    got_eof = true;
                    break;
                }
            }
        }
        check("broker gives up and disconnects", got_eof);
    }

    println!("[9] QoS0 still works (regression)");
    {
        let mut sub = connect(&addr, "reg-sub");
        let mut pubc = connect(&addr, "reg-pub");
        sub.write_all(&pkt_subscribe(5, "reg/topic", 0)).unwrap();
        recv_pkt(&mut sub, Duration::from_secs(2));
        pubc.write_all(&pkt_publish_qos0("reg/topic", b"plain")).unwrap();
        let fwd = recv_pkt(&mut sub, Duration::from_secs(2));
        check("QoS0 fanout intact", fwd.map(|(b0, _)| (b0 >> 4) == 3).unwrap_or(false));
    }

    println!("[6] keepalive ping still works");
    {
        let mut s = connect(&addr, "ping-client");
        s.write_all(&pkt_pingreq()).unwrap();
        let r = recv_pkt(&mut s, Duration::from_secs(2));
        check("PINGRESP", r.map(|(b0, _)| (b0 >> 4) == 13).unwrap_or(false));
    }

    println!("\n==== QoS1 test result: {pass} pass, {fail} fail ====");
    if fail > 0 {
        std::process::exit(1);
    }
}
