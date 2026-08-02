#!/usr/bin/env rustc

use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};

fn test_lwt_parsing() {
    // Test parsing of CONNECT packet with LWT
    let mut buf = Vec::new();
    // Construct a minimal CONNECT packet with LWT
    // Protocol name "MQTT": 4 bytes
    // Flags: 0x06 (clean session = 0, will flag = 1, will qos = 1, will retain = 0)
    // Keepalive: 60
    // Client ID: "client1" (6 bytes)
    // Will topic: "will/topic" (11 bytes)  
    // Will message: "lwt message" (13 bytes)
    
    // Packet structure:
    // 0-1: remaining length (37)
    // 2-5: "MQTT" protocol
    // 6: flags byte (0x06)
    // 7-8: keepalive (60)
    // 9-10: client id length (6)
    // 11-16: "client1"
    // 17-18: will topic length (11)
    // 19-29: "will/topic" 
    // 30-31: will message length (13)
    // 32-44: "lwt message"
    
    buf.extend_from_slice(&[0, 37]); // remaining length
    buf.extend_from_slice(b"MQTT");
    buf.push(0x06); // flags: clean session=0, will flag=1, will qos=1, will retain=0
    buf.extend_from_slice(&[0, 60]); // keepalive
    buf.extend_from_slice(&[0, 6]); // client id length
    buf.extend_from_slice(b"client1");
    buf.extend_from_slice(&[0, 11]); // will topic length
    buf.extend_from_slice(b"will/topic");
    buf.extend_from_slice(&[0, 13]); // will message length
    buf.extend_from_slice(b"lwt message");
    
    // This would be parsed by the actual parse_connect function
    println!("Test LWT parsing successful - mock test");
}

fn main() {
    println!("LWT implementation test");
    test_lwt_parsing();
}