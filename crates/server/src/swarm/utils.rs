//! Network utilities for swarm configuration
//!
//! Provides network binding utilities for the distributed swarm infrastructure.

use anyhow::Result;
use libp2p::Multiaddr;
use std::net::Ipv4Addr;
use tracing::info;

/// Get the preferred listen address for the swarm
///
/// Returns a multiaddr that binds to all available network interfaces
/// for maximum connectivity in distributed environments.
pub fn get_preferred_listen_address(port: u16) -> Result<Multiaddr> {
    // Bind to all interfaces for maximum peer connectivity
    let addr = Ipv4Addr::UNSPECIFIED; // 0.0.0.0 - bind to all interfaces
    let multiaddr = format!("/ip4/{}/tcp/{}", addr, port).parse::<Multiaddr>()?;

    info!("🎧 Swarm listening on all interfaces: {}", multiaddr);
    info!("   📡 Ready for Kademlia DHT peer discovery");

    Ok(multiaddr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_preferred_listen_address() {
        let addr = get_preferred_listen_address(9580).unwrap();
        assert_eq!(addr.to_string(), "/ip4/0.0.0.0/tcp/9580");
    }
}
