// mqtt_test.rs — 0-dependency MQTT test client (pure std)
// Simulates: 3 clients, subscribe with wildcards, publish, verify fan-out & isolation
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

fn pkt_connect(cid: &str) -> Vec<u8> {
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4); // 3.1.1
    body.push(0x02); // clean session
    body.extend_from_slice(&60u16.to_be_bytes()); // keepalive
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

fn pkt_unsubscribe(pid: u16, filter: &str) -> Vec<u8> {
    let mut pkt = vec![0xA2];
    let mut body = pid.to_be_bytes().to_vec();
    let f = filter.as_bytes();
    body.extend_from_slice(&(f.len() as u16).to_be_bytes());
    body.extend_from_slice(f);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_publish(topic: &str, payload: &[u8], qos: u8) -> Vec<u8> {
    let mut pkt = vec![0x30 | ((qos & 3) << 1)];
    let mut body = Vec::new();
    let t = topic.as_bytes();
    body.extend_from_slice(&(t.len() as u16).to_be_bytes());
    body.extend_from_slice(t);
    if qos > 0 {
        body.extend_from_slice(&1u16.to_be_bytes()); // pid=1
    }
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn pkt_ping() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn pkt_disconnect() -> Vec<u8> {
    vec![0xE0, 0x00]
}

fn send(stream: &mut TcpStream, pkt: &[u8]) {
    stream.write_all(pkt).expect("write");
}

fn read_packet(stream: &mut TcpStream, timeout_ms: u64) -> Option<(u8, u8, Vec<u8>)> {
    stream.set_read_timeout(Some(Duration::from_millis(timeout_ms))).ok();
    let mut hdr = [0u8; 1];
    if stream.read_exact(&mut hdr).is_err() {
        return None;
    }
    let ptype = hdr[0] >> 4;
    let flags = hdr[0] & 0x0f;
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
    let mut body = vec![0u8; rem];
    stream.read_exact(&mut body).ok()?;
    Some((ptype, flags, body))
}

fn expect(stream: &mut TcpStream, name: &str, ptype: u8, timeout_ms: u64) {
    match read_packet(stream, timeout_ms) {
        Some((t, _, body)) => {
            if t == ptype {
                println!("  OK   {name}: packet type {ptype} ({}B)", body.len());
            } else {
                println!("  FAIL {name}: expected type {ptype}, got {t}");
                std::process::exit(1);
            }
        }
        None => {
            println!("  FAIL {name}: timeout / no packet");
            std::process::exit(1);
        }
    }
}

fn expect_nothing(stream: &mut TcpStream, name: &str, timeout_ms: u64) {
    match read_packet(stream, timeout_ms) {
        Some((t, _, _)) => {
            println!("  FAIL {name}: expected silence, got type {t}");
            std::process::exit(1);
        }
        None => println!("  OK   {name}: silence (no packet within {timeout_ms}ms)"),
    }
}

// returns (topic, payload) of a received PUBLISH; handles QoS0/QoS1
// (QoS1 forwards carry a 2-byte packet id after the topic)
fn expect_publish(stream: &mut TcpStream, name: &str, timeout_ms: u64) -> (String, String) {
    match read_packet(stream, timeout_ms) {
        Some((3, flags, body)) => {
            let qos = (flags >> 1) & 0x03;
            let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
            let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
            let mut pos = 2 + tlen;
            if qos > 0 {
                pos += 2; // skip packet id
            }
            let payload = String::from_utf8_lossy(&body[pos..]).to_string();
            println!("  OK   {name}: PUBLISH qos{qos} topic={topic} payload={payload}");
            (topic, payload)
        }
        Some((t, _, _)) => {
            println!("  FAIL {name}: expected PUBLISH(3), got {t}");
            std::process::exit(1);
        }
        None => {
            println!("  FAIL {name}: timeout");
            std::process::exit(1);
        }
    }
}

fn connect(addr: &str, cid: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect");
    send(&mut s, &pkt_connect(cid));
    expect(&mut s, &format!("{cid} CONNACK"), 2, 2000);
    println!("  ... {cid} connected");
    s
}

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1883".into());
    println!("== MQTT integration test against {addr} ==");

    // ---- phase 1: 3 clients connect ----
    let mut a = connect(&addr, "client-A");
    let mut b = connect(&addr, "client-B");
    let mut c = connect(&addr, "client-C");

    // ---- phase 2: subscriptions ----
    send(&mut a, &pkt_subscribe(1, "test/#", 1));
    expect(&mut a, "A SUBACK", 9, 2000);
    send(&mut b, &pkt_subscribe(1, "test/+/foo", 1));
    expect(&mut b, "B SUBACK", 9, 2000);

    // ---- phase 3: publish, fan-out check ----
    println!("[test] C publishes to test/hello/foo");
    send(&mut c, &pkt_publish("test/hello/foo", b"msg1", 0));
    let (ta, pa) = expect_publish(&mut a, "A receives (test/#)", 2000);
    assert!(ta == "test/hello/foo" && pa == "msg1");
    let (tb, pb) = expect_publish(&mut b, "B receives (test/+/foo)", 2000);
    assert!(tb == "test/hello/foo" && pb == "msg1");

    // ---- phase 4: isolation — non-matching topic ----
    println!("[test] C publishes to test/other (B must NOT receive)");
    send(&mut c, &pkt_publish("test/other", b"msg2", 0));
    let (ta2, pa2) = expect_publish(&mut a, "A receives (test/#)", 2000);
    assert!(ta2 == "test/other" && pa2 == "msg2");
    expect_nothing(&mut b, "B silent (test/+/foo != test/other)", 500);

    // ---- phase 5: QoS1 publish -> PUBACK ----
    println!("[test] C publishes QoS1 -> expects PUBACK");
    send(&mut c, &pkt_publish("test/hello/foo", b"qos1msg", 1));
    expect(&mut c, "C PUBACK", 4, 2000);
    let (tc, pc) = expect_publish(&mut a, "A receives qos1msg", 2000);
    assert!(tc == "test/hello/foo" && pc == "qos1msg");

    // ---- phase 6: ping ----
    println!("[test] A pings");
    send(&mut a, &pkt_ping());
    expect(&mut a, "A PINGRESP", 13, 2000);

    // ---- phase 7: unsubscribe ----
    println!("[test] A unsubscribes test/#");
    send(&mut a, &pkt_unsubscribe(9, "test/#"));
    expect(&mut a, "A UNSUBACK", 11, 2000);
    send(&mut c, &pkt_publish("test/any", b"msg3", 0));
    expect_nothing(&mut a, "A silent after unsubscribe", 500);

    // ---- phase 8: disconnect ----
    send(&mut a, &pkt_disconnect());
    send(&mut b, &pkt_disconnect());
    send(&mut c, &pkt_disconnect());
    println!("== ALL TESTS PASSED ==");
}
