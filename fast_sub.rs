// fast_sub.rs — fast subscriber: subscribe bench/#, count N publishes, print rate.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn encode_rem(mut len: usize, buf: &mut Vec<u8>) {
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 { b |= 0x80; }
        buf.push(b);
        if len == 0 { break; }
    }
}

fn connect(cid: &str, port: u16) -> TcpStream {
    let addr = format!("127.0.0.1:{port}");
    let mut s = TcpStream::connect(&addr).expect("connect");
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
    s.write_all(&pkt).unwrap();
    let mut hdr = [0u8; 4];
    s.read_exact(&mut hdr).unwrap();
    s
}

fn subscribe(s: &mut TcpStream) {
    let mut pkt = vec![0x82];
    let mut body = 1u16.to_be_bytes().to_vec();
    let f = b"bench/#";
    body.extend_from_slice(&(f.len() as u16).to_be_bytes());
    body.extend_from_slice(f);
    body.push(0);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    s.write_all(&pkt).unwrap();
    let mut suback = [0u8; 5];
    s.read_exact(&mut suback).unwrap(); // SUBACK
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(11883);
    let n: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(100_000);
    let mut s = connect("rust-fast", port);
    subscribe(&mut s);
    println!("[fast_sub] subscribed, waiting for {n} msgs...");
    let t0 = Instant::now();
    let mut got = 0usize;
    s.set_read_timeout(Some(Duration::from_millis(3000))).ok();
    while got < n {
        // read one packet
        let mut hdr = [0u8; 1];
        if s.read_exact(&mut hdr).is_err() {
            println!("[fast_sub] stall: got {got}/{n} before EOF/timeout");
            break;
        }
        let ptype = hdr[0] >> 4;
        let mut rem: usize = 0;
        let mut mult: usize = 1;
        loop {
            let mut b = [0u8; 1];
            if s.read_exact(&mut b).is_err() { println!("[fast_sub] stall reading rem"); break; }
            rem += ((b[0] & 0x7f) as usize) * mult;
            if b[0] & 0x80 == 0 { break; }
            mult *= 128;
        }
        let mut body = vec![0u8; rem];
        if s.read_exact(&mut body).is_err() { println!("[fast_sub] stall reading body"); break; }
        if ptype == 3 {
            got += 1;
        }
    }
    let dt = t0.elapsed();
    println!(
        "[fast_sub] got {got}/{n} in {:.3}s -> {:.0} msg/s",
        dt.as_secs_f64(),
        got as f64 / dt.as_secs_f64()
    );
}
