// minimal repro: single subscriber thread + publisher in main
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn encode_rem(mut len: usize, buf: &mut Vec<u8>) {
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 { b |= 0x80; }
        buf.push(b);
        if len == 0 { break; }
    }
}

fn pkt_connect(cid: &str) -> Vec<u8> {
    let mut pkt = vec![0x10];
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(0x02);
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

fn pkt_publish(topic: &str, payload: &[u8], qos: u8) -> Vec<u8> {
    let mut pkt = vec![0x30 | ((qos & 3) << 1)];
    let mut body = Vec::new();
    let t = topic.as_bytes();
    body.extend_from_slice(&(t.len() as u16).to_be_bytes());
    body.extend_from_slice(t);
    if qos > 0 { body.extend_from_slice(&1u16.to_be_bytes()); }
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn connect(addr: &str, cid: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_nodelay(true).ok();
    s.write_all(&pkt_connect(cid)).unwrap();
    let mut hdr = [0u8; 1];
    s.read_exact(&mut hdr).unwrap();
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).unwrap();
        if b[0] & 0x80 == 0 { break; }
    }
    let mut body = [0u8; 2];
    s.read_exact(&mut body).unwrap();
    s
}

fn read_packet(stream: &mut TcpStream, timeout_ms: u64) -> Option<(u8, u8, Vec<u8>)> {
    stream.set_read_timeout(Some(Duration::from_millis(timeout_ms))).ok();
    let mut hdr = [0u8; 1];
    stream.read_exact(&mut hdr).ok()?;
    let ptype = hdr[0] >> 4;
    let flags = hdr[0] & 0x0f;
    let mut rem: usize = 0;
    let mut mult: usize = 1;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).ok()?;
        rem += ((b[0] & 0x7f) as usize) * mult;
        if b[0] & 0x80 == 0 { break; }
        mult *= 128;
    }
    let mut body = vec![0u8; rem];
    stream.read_exact(&mut body).ok()?;
    Some((ptype, flags, body))
}

fn main() {
    let addr = "127.0.0.1:11883";
    let mut sub = connect(addr, "only-sub");
    sub.write_all(&pkt_subscribe(1, "bench/#", 0)).unwrap();
    println!("subscribed, waiting for SUBACK...");
    match read_packet(&mut sub, 2000) {
        Some((t, _, b)) => println!("SUBACK: type {t} body {:02x?}", b),
        None => { println!("NO SUBACK!"); std::process::exit(1); }
    }
    let mut pub_s = connect(addr, "only-pub");
    for i in 0..10 {
        let payload = format!("m{i}");
        pub_s.write_all(&pkt_publish("bench/x", payload.as_bytes(), 0)).unwrap();
        match read_packet(&mut sub, 2000) {
            Some((3, _, body)) => {
                let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
                let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
                let pl = String::from_utf8_lossy(&body[2 + tlen..]).to_string();
                println!("[{i}] got PUBLISH {topic} -> {pl}");
            }
            Some((t, _, b)) => println!("[{i}] unexpected type {t} body {:02x?}", b),
            None => println!("[{i}] TIMEOUT waiting for publish!"),
        }
    }
    println!("DONE");
}
