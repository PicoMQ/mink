//! Picks an unused loopback TCP port by binding port zero and reading back what the OS assigned.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};

pub fn free_port() -> u16 {
    TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("bind a loopback port")
}
