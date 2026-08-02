// mqtt_bench.rs — 0-dependency MQTT benchmark (pure std)
// Modes:
//   throughput  ADDR SUBS MSGS   — 1 publisher -> N subscribers, QoS0 fan-out
//   latency     ADDR N           — ping-pong RTT between two clients, N samples
use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// topic "bench/ping" / "bench/pong" are both 10 bytes -> payload starts at 2+10
const PAYLOAD_OFF: usize = 12;

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
    if qos > 0 {
        body.extend_from_slice(&1u16.to_be_bytes());
    }
    body.extend_from_slice(payload);
    encode_rem(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

fn connect(addr: &str, cid: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_nodelay(true).ok();
    s.write_all(&pkt_connect(cid)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(3000))).ok();
    let mut hdr = [0u8; 1];
    if s.read_exact(&mut hdr).is_err() {
        eprintln!("[dbg] connect({cid}): TIMEOUT waiting for CONNACK header");
        std::process::exit(2);
    }
    loop {
        let mut b = [0u8; 1];
        if s.read_exact(&mut b).is_err() {
            eprintln!("[dbg] connect({cid}): TIMEOUT waiting for CONNACK rem-len");
            std::process::exit(2);
        }
        if b[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = [0u8; 2];
    if s.read_exact(&mut body).is_err() {
        eprintln!("[dbg] connect({cid}): TIMEOUT waiting for CONNACK body");
        std::process::exit(2);
    }
    s.set_read_timeout(None).ok();
    s
}

fn read_packet(stream: &mut TcpStream) -> Option<(u8, u8, Vec<u8>)> {
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
        if b[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut body = vec![0u8; rem];
    stream.read_exact(&mut body).ok()?;
    Some((ptype, flags, body))
}

// ---- throughput: SUBS subscribers on bench/#, publisher blasts MSGS on bench/x ----
fn bench_throughput(addr: &str, subs: usize, msgs: usize) {
    let counter = Arc::new(AtomicUsize::new(0));
    let total = subs * msgs;
    let mut handles = Vec::new();

    let t0 = Instant::now();
    let mut spawn_count = 0;
    for i in 0..subs {
        let addr = addr.to_string();
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            eprintln!("[dbg] sub-{i} thread started, connecting...");
            let mut s = connect(&addr, &format!("sub-{i}"));
            eprintln!("[dbg] sub-{i} connected, subscribing...");
            s.write_all(&pkt_subscribe(1, "bench/#", 0)).unwrap();
            // read SUBACK
            let sa = read_packet(&mut s);
            eprintln!("[dbg] sub-{i} SUBACK result: {:?}", sa.as_ref().map(|(t, _, _)| *t));
            let mut got_any = false;
            let mut count = 0usize;
            // Each subscriber must stop after its own share (msgs).
            // Waiting on the global counter alone deadlocks: the last
            // subscriber blocks in read_packet for a message that never comes.
            while count < msgs {
                match read_packet(&mut s) {
                    Some((3, _, _)) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                        got_any = true;
                        count += 1;
                    }
                    Some((t, _, _)) => {
                        eprintln!("[dbg] sub-{i} unexpected type {t} (got {count} so far)");
                        break;
                    }
                    None => {
                        eprintln!("[dbg] sub-{i} EOF (got {count} so far)");
                        break;
                    }
                }
            }
            eprintln!(
                "[dbg] sub-{i} exit loop, got {count}, counter={}",
                counter.load(Ordering::Relaxed)
            );
            if !got_any {
                eprintln!("[dbg] sub-{i} NEVER received a single PUBLISH");
            }
        }));
        spawn_count += 1;
    }
    eprintln!("[dbg] spawned {spawn_count} subscriber threads");
    // give subscribers a moment to subscribe
    thread::sleep(Duration::from_millis(200));

    let mut pub_s = connect(addr, "publisher");
    let pkt = pkt_publish("bench/x", &[0u8; 32], 0);
    for _ in 0..msgs {
        pub_s.write_all(&pkt).unwrap();
    }

    // wait for all subscribers to finish
    for h in handles {
        h.join().unwrap();
    }
    let dt = t0.elapsed();
    let got = counter.load(Ordering::Relaxed);
    let rate = got as f64 / dt.as_secs_f64();
    println!(
        "  delivered {got}/{total} msgs in {:.3}s  ->  {:.0} msg/s (fan-out),  {:.0} msg/s (subscriber-side)",
        dt.as_secs_f64(),
        rate,
        rate / subs as f64
    );
}

// ---- latency: A publishes ts-tagged msg to bench/ping, B echoes to bench/pong ----
fn bench_latency(addr: &str, n: usize) {
    let (tx, rx) = mpsc::channel::<f64>();

    // echo client B
    let addr_b = addr.to_string();
    let b_handle = thread::spawn(move || {
        let mut s = connect(&addr_b, "echo-b");
        s.write_all(&pkt_subscribe(1, "bench/ping", 0)).unwrap();
        let _ = read_packet(&mut s);
        // echo exactly n times, then exit so join() in main can finish
        let mut echoed = 0usize;
        while echoed < n {
            match read_packet(&mut s) {
                Some((3, _, body)) => {
                    // echo back on bench/pong
                    s.write_all(&pkt_publish("bench/pong", &body[PAYLOAD_OFF..], 0)).unwrap();
                    echoed += 1;
                }
                _ => break,
            }
        }
    });

    // measurer client A
    let mut a = connect(addr, "measurer-a");
    a.write_all(&pkt_subscribe(1, "bench/pong", 0)).unwrap();
    let _ = read_packet(&mut a);
    thread::sleep(Duration::from_millis(100));

    let mut latencies = Vec::with_capacity(n);
    for i in 0..n {
        // payload: 8-byte ns timestamp
        let ts = Instant::now();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        a.write_all(&pkt_publish("bench/ping", &nanos.to_be_bytes(), 0)).unwrap();
        // wait for echo (with timeout so we can pinpoint a stall)
        a.set_read_timeout(Some(Duration::from_millis(3000))).ok();
        let t_start = Instant::now();
        let mut got_pong = false;
        loop {
            match read_packet(&mut a) {
                Some((3, _, body)) => {
                    let sent = u64::from_be_bytes(
                        body[PAYLOAD_OFF..PAYLOAD_OFF + 8].try_into().unwrap(),
                    );
                    let rtt = (Instant::now() - ts).as_secs_f64() * 1e6;
                    // verify the echoed payload matches (sanity)
                    assert_eq!(sent, nanos, "payload mismatch at sample {i}");
                    latencies.push(rtt);
                    got_pong = true;
                    break;
                }
                Some((t, _, body)) => {
                    eprintln!(
                        "[dbg] sample {i}: unexpected type {t} len {} (waiting for pong)",
                        body.len()
                    );
                    break;
                }
                None => {
                    eprintln!("[dbg] sample {i}: read timeout after {:.1}s (no pong)", t_start.elapsed().as_secs_f64());
                    break;
                }
            }
        }
        if !got_pong {
            eprintln!("[dbg] sample {i}: FAILED to get pong, aborting");
            std::process::exit(3);
        }
    }

    let _ = tx.send(0.0);
    let _ = b_handle.join();

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean: f64 = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let max = latencies.last().unwrap();
    println!(
        "  {n} samples: mean {mean:.2}us  p50 {p50:.2}us  p95 {p95:.2}us  p99 {p99:.2}us  max {max:.2}us"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("throughput") => {
            let addr = args.get(2).expect("addr").clone();
            let subs: usize = args.get(3).expect("subs").parse().unwrap();
            let msgs: usize = args.get(4).expect("msgs").parse().unwrap();
            println!("[throughput] {subs} subscribers, {msgs} msgs @ {addr}");
            bench_throughput(&addr, subs, msgs);
        }
        Some("latency") => {
            let addr = args.get(2).expect("addr").clone();
            let n: usize = args.get(3).expect("n").parse().unwrap();
            println!("[latency] {n} ping-pong samples @ {addr}");
            bench_latency(&addr, n);
        }
        _ => {
            eprintln!("usage: mqtt_bench <throughput ADDR SUBS MSGS | latency ADDR N>");
            std::process::exit(1);
        }
    }
}
