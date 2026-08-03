// subscription-index dedicated test:
// 1. N clients subscribe to the SAME exact filter -> subs_exact must fan out to all
// 2. exact + wildcard mixed fan-out on one publish
// 3. unsubscribe one of N -> others still get it
// 4. dead-client prune during publish -> index rebuilt, later publishes still correct
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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
fn connect(id: &str, clean: bool) -> TcpStream {
    let mut s = TcpStream::connect("127.0.0.1:11883").unwrap();
    s.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let flags: u8 = if clean { 0x02 } else { 0x00 };
    let mut body = enc("MQTT");
    body.push(0x04); body.push(flags);
    body.extend_from_slice(&[0x00, 0x3c]);
    body.extend_from_slice(&enc(id));
    s.write_all(&pkt(1, 0, &body)).unwrap();
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).unwrap();
    s
}
fn sub(s: &mut TcpStream, pid: u16, filter: &str, qos: u8) {
    let mut body = vec![(pid >> 8) as u8, (pid & 0xff) as u8];
    body.extend_from_slice(&enc(filter));
    body.push(qos);
    s.write_all(&pkt(8, 2, &body)).unwrap();
    // keep reading (discarding) until a SUBACK (type 9) packet byte shows up.
    // NOTE: any retained PUBLISH the broker delivers before/with the suback
    // is consumed here; tests needing retained payloads must not use sub().
    let mut buf = [0u8; 4096];
    let t0 = std::time::Instant::now();
    loop {
        if t0.elapsed() >= Duration::from_millis(1000) { break; }
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if buf[..n].iter().any(|&b| (b >> 4) == 9) { break; }
            }
        }
    }
}
fn drain(s: &mut TcpStream, ms: u64) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let t0 = std::time::Instant::now();
    while t0.elapsed() < Duration::from_millis(ms) {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.push(buf[..n].to_vec()),
            Err(_) => break,
        }
    }
    out
}
fn publish(s: &mut TcpStream, topic: &str, payload: &str, retain: bool) {
    let mut body = enc(topic);
    body.extend_from_slice(payload.as_bytes());
    s.write_all(&pkt(3, if retain { 0x01 } else { 0x00 }, &body)).unwrap();
}
fn count_pub(datagrams: &[Vec<u8>]) -> usize {
    datagrams.iter().filter(|d| !d.is_empty() && (d[0] >> 4) == 3).count()
}

fn main() {
    let mut pass = 0; let mut fail = 0;
    let mut check = |name: &str, ok: bool| {
        if ok { pass += 1; println!("PASS  {name}"); } else { fail += 1; println!("FAIL  {name}"); }
    };

    // [1] 5 clients subscribe to the same exact filter "sx/one"
    let addr = "127.0.0.1:11883";
    let mut subs: Vec<TcpStream> = Vec::new();
    for i in 0..5 {
        let mut s = connect(&format!("sx-sub{i}"), true);
        sub(&mut s, 1, "sx/one", 0);
        subs.push(s);
    }
    let mut pubc = connect("sx-pub", true);
    publish(&mut pubc, "sx/one", "hello", false);
    std::thread::sleep(Duration::from_millis(300));
    let mut got = 0;
    for s in subs.iter_mut() { got += count_pub(&drain(s, 200)); }
    check("[1] exact filter fans out to all 5", got == 5);

    // [2] mixed: exact "sx/one" (5) + wild "sx/#" (2) + unrelated "other/#" (1)
    let mut w1 = connect("sx-w1", true); sub(&mut w1, 1, "sx/#", 0);
    let mut w2 = connect("sx-w2", true); sub(&mut w2, 1, "sx/#", 0);
    let mut w3 = connect("sx-w3", true); sub(&mut w3, 1, "other/#", 0);
    publish(&mut pubc, "sx/one", "again", false);
    std::thread::sleep(Duration::from_millis(300));
    let mut got = 0;
    for s in subs.iter_mut() { got += count_pub(&drain(s, 200)); }
    got += count_pub(&drain(&mut w1, 200));
    got += count_pub(&drain(&mut w2, 200));
    let unrelated = count_pub(&drain(&mut w3, 200));
    check("[2] mixed fan-out 7 (5 exact + 2 wild), unrelated 0", got == 7 && unrelated == 0);

    // [3] unsubscribe sub0 -> fan-out becomes 6
    let mut body = vec![0x00, 0x01];
    body.extend_from_slice(&enc("sx/one"));
    subs[0].write_all(&pkt(10, 2, &body)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    publish(&mut pubc, "sx/one", "after-unsub", false);
    std::thread::sleep(Duration::from_millis(300));
    let mut got = 0;
    for s in subs.iter_mut().skip(1) { got += count_pub(&drain(s, 200)); }
    got += count_pub(&drain(&mut w1, 200)) + count_pub(&drain(&mut w2, 200));
    check("[3] after unsubscribe fan-out = 6", got == 6);

    // [4] kill sub1 (abrupt close) mid-flight, publish -> broker prunes, others still get it
    let _ = subs[1].shutdown(std::net::Shutdown::Both);
    drop(subs.remove(1));
    std::thread::sleep(Duration::from_millis(200));
    publish(&mut pubc, "sx/one", "after-dead", false);
    std::thread::sleep(Duration::from_millis(400));
    let mut got = 0;
    for s in subs.iter_mut() { got += count_pub(&drain(s, 200)); }
    got += count_pub(&drain(&mut w1, 200)) + count_pub(&drain(&mut w2, 200));
    check("[4] dead-client prune keeps others working (got 5)", got == 5);

    // [5] $SYS still alive after index churn (wait for the 10s publish timer)
    std::thread::sleep(Duration::from_millis(9500));
    let mut sys = connect("sx-sys", true);
    // raw subscribe (no sub() helper: it would consume retained payloads)
    let mut sbody = vec![0x00, 0x01];
    sbody.extend_from_slice(&enc("$SYS/#"));
    sbody.push(0);
    sys.write_all(&pkt(8, 2, &sbody)).unwrap();
    let raw: Vec<u8> = drain(&mut sys, 1500).into_iter().flatten().collect();
    let text = String::from_utf8_lossy(&raw).to_string();
    let ok5 = raw.windows(19).any(|w| w == b"$SYS/broker/version") && text.contains("mqtt-broker-rs");
    check("[5] $SYS retained topics still delivered", ok5);

    println!("----\nsubsidx test result: {pass} pass, {fail} fail");
    std::process::exit(if fail == 0 { 0 } else { 1 });
}
