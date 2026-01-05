//! Network utilities for swarm configuration
//!
//! Provides network binding utilities for the distributed swarm infrastructure.

use anyhow::Result;
use libp2p::Multiaddr;
use std::net::{IpAddr, Ipv4Addr};
use tracing::{info, warn};

/// Resolve the preferred listen address from cluster config.
///
/// Preference order:
/// 1) First valid IPv4 entry
/// 2) First valid DNS v4 entry
/// 3) First valid DNS (unspecified) entry
/// 4) First valid IPv6 entry
/// 5) First valid DNS v6 entry
/// If none are provided or valid, falls back to 0.0.0.0.
///
/// We prepend the explicit bind_address (if set) ahead of listen_addrs to ensure that
/// env/configured bind address is honored even when listen_addrs is empty.
pub fn resolve_listen_address(
    bind_address: &str,
    listen_addrs: &[String],
    port: u16,
) -> Result<Multiaddr> {
    // Helper to build multiaddr string
    let mut candidates = Vec::new();

    // Prepend the explicit bind address if provided
    if !bind_address.trim().is_empty() {
        candidates.extend(parse_listen_entry(bind_address, port));
    }

    // Then process any explicit listen_addrs entries
    for raw in listen_addrs {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }

        candidates.extend(parse_listen_entry(trimmed, port));
    }

    // Preference: IPv4 (including dns4/plain dns), then IPv6 (including dns6).
    let pick = candidates
        .iter()
        .find(|a| a.to_string().starts_with("/ip4/"))
        .or_else(|| {
            candidates
                .iter()
                .find(|a| a.to_string().starts_with("/dns4/"))
        })
        .or_else(|| {
            candidates
                .iter()
                .find(|a| a.to_string().starts_with("/dns/"))
        })
        .or_else(|| {
            candidates
                .iter()
                .find(|a| a.to_string().starts_with("/ip6/"))
        })
        .or_else(|| {
            candidates
                .iter()
                .find(|a| a.to_string().starts_with("/dns6/"))
        })
        .or_else(|| candidates.first())
        .cloned();

    if let Some(addr) = pick {
        info!("🎧 Using configured cluster listen address: {}", addr);
        return Ok(addr);
    }

    // Fallback to 0.0.0.0
    let addr = get_preferred_listen_address(port)?;
    warn!(
        "⚠️  No valid listen address provided; falling back to {}",
        addr
    );
    Ok(addr)
}

fn parse_listen_entry(entry: &str, port: u16) -> Vec<Multiaddr> {
    let mut out = Vec::new();
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return out;
    }

    // If caller already supplied a multiaddr, try to parse directly.
    if trimmed.starts_with("/ip4/")
        || trimmed.starts_with("/ip6/")
        || trimmed.starts_with("/dns/")
        || trimmed.starts_with("/dns4/")
        || trimmed.starts_with("/dns6/")
    {
        match trimmed.parse::<Multiaddr>() {
            Ok(addr) => {
                out.push(addr);
                return out;
            }
            Err(e) => {
                warn!("⚠️  Ignoring invalid listen multiaddr '{}': {}", trimmed, e);
                return out;
            }
        }
    }

    // Otherwise treat as host or host:port; prefer supplied port if no port present.
    let (host, port_num) = if let Some((h, p)) = trimmed.rsplit_once(':') {
        match p.parse::<u16>() {
            Ok(pn) => (h, pn),
            Err(e) => {
                warn!(
                    "⚠️  Ignoring listen address '{}': invalid port '{}': {}",
                    trimmed, p, e
                );
                return out;
            }
        }
    } else {
        (trimmed, port)
    };

    // Strip brackets for IPv6 literals
    let clean_host = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };

    let addr_str = match clean_host.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => format!("/ip4/{}/tcp/{}", clean_host, port_num),
        Ok(IpAddr::V6(_)) => format!("/ip6/{}/tcp/{}", clean_host, port_num),
        Err(_) => format!("/dns/{}/tcp/{}", clean_host, port_num),
    };

    match addr_str.parse::<Multiaddr>() {
        Ok(addr) => out.push(addr),
        Err(e) => warn!(
            "⚠️  Ignoring listen address '{}': failed to parse {} -> {}",
            trimmed, addr_str, e
        ),
    }

    out
}

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
