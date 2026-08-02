// mqtt_lwt_test.rs — LWT (Last Will and Testament) integration tests for the mio broker
// Scenarios:
//  1. will fires on abnormal disconnect: sub(will/topic) -> client A connects with
//     will flag -> A drops TCP without DISCONNECT -> subscriber receives will message
//  2. will does NOT fire on clean DISCONNECT: A sends DISCONNECT -> no will message
//  3. will retain: A connects with will retain=1 -> A drops -> will stored as retained,
//     late subscriber on same topic receives it immediately
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

fn pkt_connect_will(cid: &str, clean: bool, will_topic: &str, will_msg: &[u8], will_retain: bool, will_qos: u8) -> Vec<u8> {
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4);
    let mut flags = 0u8;
    if clean {
        flags |= 0x02;
    }
    flags |= 0x04; // will flag
    flags |= (will_qos & 0x03) << 3;
    if will_retain {
        flags |= 0x20;
    }
    body.push(flags);
    body.extend_from_slice(&60u16.to_be_bytes());
    let c = cid.as_bytes();
    body.extend_from_slice(&(c.len() as u16).to_be_bytes());
    body.extend_from_slice(c);
    let wt = will_topic.as_bytes();
    body.extend_from_slice(&(wt.len() as u16).to_be_bytes());
    body.extend_from_slice(wt);
    body.extend_from_slice(&(will_msg.len() as u16).to_be_bytes());
    body.extend_from_slice(will_msg);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
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

fn pkt_disconnect() -> Vec<u8> {
    vec![0xE0, 0x00]
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

fn expect_publish(s: &mut TcpStream, topic: &str, payload: &[u8]) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    s.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    while std::time::Instant::now() < deadline {
        if let Some((ptype, body)) = read_pkt(s) {
            if ptype >> 4 == 3 { // PUBLISH
                let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
                let t = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
                let mut off = 2 + tlen;
                if (ptype & 0x06) >> 1 > 0 {
                    off += 2; // skip pid for qos>0
                }
                let p = &body[off..];
                if t == topic && p == payload {
                    return true;
                }
            }
        }
    }
    false
}

fn connect_ok(port: u16) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_nodelay(true).unwrap();
    let _ = s.write_all(&pkt_connect("tester"));
    // wait CONNACK (0x20)
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    s.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    while std::time::Instant::now() < deadline {
        if let Some((ptype, _)) = read_pkt(&mut s) {
            if ptype == 0x20 {
                return s;
            }
        }
    }
    panic!("no CONNACK");
}

fn main() {
    let port: u16 = std::env::args().nth(1).map(|a| a.parse().unwrap()).unwrap_or(11883);
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

    // ---- Scenario 1: will fires on abnormal disconnect ----
    {
        // subscriber
        let mut sub = connect_ok(port);
        sub.write_all(&pkt_subscribe(1, "will/topic", 0)).unwrap();
        // swallow SUBACK
        let _ = read_pkt(&mut sub);

        // client A with will, then drop TCP hard
        let mut a = connect_ok(port);
        a.write_all(&pkt_connect_will("willA", true, "will/topic", b"gone fishin", false, 0))
            .unwrap();
        let _ = read_pkt(&mut a); // CONNACK
        drop(a); // abnormal disconnect (no DISCONNECT packet)

        let got = expect_publish(&mut sub, "will/topic", b"gone fishin");
        check("will fires on abnormal disconnect", got);
    }

    // ---- Scenario 2: clean DISCONNECT suppresses will ----
    {
        let mut sub = connect_ok(port);
        sub.write_all(&pkt_subscribe(2, "will/topic", 0)).unwrap();
        let _ = read_pkt(&mut sub);

        let mut a = connect_ok(port);
        a.write_all(&pkt_connect_will("willB", true, "will/topic", b"should not arrive", false, 0))
            .unwrap();
        let _ = read_pkt(&mut a);
        a.write_all(&pkt_disconnect()).unwrap();
        drop(a);

        let got = expect_publish(&mut sub, "will/topic", b"should not arrive");
        check("clean DISCONNECT suppresses will", !got);
    }

    // ---- Scenario 3: will retain stores retained message ----
    {
        let mut a = connect_ok(port);
        a.write_all(&pkt_connect_will("willC", true, "will/retained", b"sticky", true, 0))
            .unwrap();
        let _ = read_pkt(&mut a);
        drop(a); // abnormal drop -> will published with retain

        // late subscriber gets the retained will immediately
        let mut sub = connect_ok(port);
        sub.write_all(&pkt_subscribe(3, "will/retained", 0)).unwrap();
        let got = expect_publish(&mut sub, "will/retained", b"sticky");
        check("will retain stores retained message", got);
    }

    println!("----");
    println!("{pass} passed, {fail} failed");
    if fail > 0 {
        std::process::exit(1);
    }
}
