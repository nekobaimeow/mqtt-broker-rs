// fanout throughput benchmark: 1 publisher -> N subscribers, exact filters
// (the case the subscription index optimizes). Run against the broker on 11883.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn enc(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut v = vec![(b.len() >> 8) as u8, (b.len() & 0xff) as u8];
    v.extend_from_slice(b);
    v
}
fn pkt(t: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![(t << 4) | flags];
    let mut n = body.len();
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 { b |= 0x80; }
        v.push(b);
        if n == 0 { break; }
    }
    v.extend_from_slice(body);
    v
}
fn connect(id: &str) -> TcpStream {
    let mut s = TcpStream::connect("127.0.0.1:11883").unwrap();
    s.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let mut body = enc("MQTT");
    body.push(0x04); body.push(0x02);
    body.extend_from_slice(&[0x00, 0x3c]);
    body.extend_from_slice(&enc(id));
    s.write_all(&pkt(1, 0, &body)).unwrap();
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).unwrap();
    s
}
fn sub(s: &mut TcpStream, pid: u16, filter: &str) {
    let mut body = vec![(pid >> 8) as u8, (pid & 0xff) as u8];
    body.extend_from_slice(&enc(filter));
    body.push(0);
    s.write_all(&pkt(8, 2, &body)).unwrap();
    let mut buf = [0u8; 64];
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(500) {
        match s.read(&mut buf) { Ok(0) | Err(_) => break, Ok(_) => {} }
    }
}
fn main() {
    let n: usize = std::env::args().nth(1).unwrap_or("10".into()).parse().unwrap();
    let msgs: usize = std::env::args().nth(2).unwrap_or("200000".into()).parse().unwrap();
    let topic = "perf/x";
    // exact-filter subscribers, each with its own connection
    let mut subs = Vec::new();
    for i in 0..n {
        let mut s = connect(&format!("perf-sub{i}"));
        sub(&mut s, 1, topic);
        subs.push(s);
    }
    let mut pubc = connect("perf-pub");
    // warmup 50k
    let payload = vec![b'a'; 32];
    for _ in 0..50_000 {
        let mut body = enc(topic);
        body.extend_from_slice(&payload);
        pubc.write_all(&pkt(3, 0, &body)).unwrap();
    }
    // drain subs
    let mut buf = [0u8; 4096];
    for s in subs.iter_mut() { while let Ok(n) = s.read(&mut buf) { if n == 0 { break; } } }
    // timed run
    let t0 = Instant::now();
    for _ in 0..msgs {
        let mut body = enc(topic);
        body.extend_from_slice(&payload);
        pubc.write_all(&pkt(3, 0, &body)).unwrap();
    }
    let el = t0.elapsed();
    // let the broker flush everything
    std::thread::sleep(Duration::from_millis(200));
    let mut received = 0usize;
    for s in subs.iter_mut() {
        while let Ok(n) = s.read(&mut buf) {
            if n == 0 { break; }
            received += n;
        }
    }
    let rate = msgs as f64 / el.as_secs_f64();
    println!("1->{n} exact filters: {msgs} msgs in {:.3}s = {:.0} msg/s (subs drained {received}B)",
        el.as_secs_f64(), rate);
}
