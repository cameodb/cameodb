//! Network utilities for swarm configuration
//!
//! Provides network binding utilities for the distributed swarm infrastructure.

use anyhow::Result;
use libp2p::Multiaddr;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use tracing::{debug, info, warn};

/// Resolve the preferred listen address from cluster config.
///
/// Preference order:
/// 1) First valid IPv4 entry
/// 2) First valid DNS v4 entry
/// 3) First valid DNS (unspecified) entry
/// 4) First valid IPv6 entry
/// 5) First valid DNS v6 entry
///
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

    // Preference for listening: bind only to IP addresses (dns is not supported for listen_on in our stack).
    debug!(
        "resolve_listen_address: bind_address='{}' port={} candidates={}",
        bind_address,
        port,
        candidates
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let pick = candidates
        .iter()
        .find(|a| a.to_string().starts_with("/ip4/"))
        .or_else(|| {
            candidates
                .iter()
                .find(|a| a.to_string().starts_with("/ip6/"))
        })
        .cloned();

    if let Some(addr) = pick {
        info!("🎧 Using configured cluster listen address: {}", addr);
        return Ok(addr);
    }

    // If no IP candidates, try resolving bind_address as DNS to an IP (prefer IPv4).
    if !bind_address.trim().is_empty()
        && let Some(addr) = resolve_hostname_to_ip(bind_address, port)
    {
        info!(
            "🎧 Resolved DNS bind address {} -> {} for cluster listen",
            bind_address, addr
        );
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

fn resolve_hostname_to_ip(host: &str, port: u16) -> Option<Multiaddr> {
    // Normalize host if caller passed a dns multiaddr form like /dns4/foo/tcp/1234
    let normalized_host = if host.starts_with("/dns/") || host.starts_with("/dns4/") {
        host.trim_start_matches("/dns/")
            .trim_start_matches("/dns4/")
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or(host)
    } else if host.starts_with("/dns6/") {
        host.trim_start_matches("/dns6/")
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or(host)
    } else {
        host
    };

    // Only attempt resolution if not already an IP literal or ip multiaddr.
    if normalized_host.parse::<IpAddr>().is_ok()
        || normalized_host.starts_with("/ip4/")
        || normalized_host.starts_with("/ip6/")
    {
        return None;
    }

    let mut first_v6 = None;
    match (normalized_host, port).to_socket_addrs() {
        Ok(iter) => {
            for sa in iter {
                match sa.ip() {
                    IpAddr::V4(v4) => {
                        let ma = format!("/ip4/{}/tcp/{}", v4, port).parse().ok()?;
                        return Some(ma);
                    }
                    IpAddr::V6(v6) => {
                        if first_v6.is_none() {
                            first_v6 = Some(v6);
                        }
                    }
                }
            }
            if let Some(v6) = first_v6 {
                let ma = format!("/ip6/{}/tcp/{}", v6, port).parse().ok()?;
                return Some(ma);
            }
            None
        }
        Err(e) => {
            warn!("⚠️  Failed to resolve bind hostname '{}': {}", host, e);
            None
        }
    }
}

fn parse_listen_entry(entry: &str, port: u16) -> Vec<Multiaddr> {
    let mut out = Vec::new();
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return out;
    }

    // If caller already supplied a multiaddr, try to parse directly.
    if trimmed.starts_with("/ip4/") || trimmed.starts_with("/ip6/") {
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

    // Resolve host to IP (prefer IPv4) for listening; do not return DNS multiaddrs here.
    let mut first_v6 = None;
    match (clean_host, port_num).to_socket_addrs() {
        Ok(iter) => {
            for sa in iter {
                match sa.ip() {
                    IpAddr::V4(v4) => {
                        let ma = format!("/ip4/{}/tcp/{}", v4, port_num).parse();
                        if let Ok(addr) = ma {
                            out.push(addr);
                            return out;
                        }
                    }
                    IpAddr::V6(v6) => {
                        if first_v6.is_none() {
                            first_v6 = Some(v6);
                        }
                    }
                }
            }
            if let Some(v6) = first_v6
                && let Ok(addr) = format!("/ip6/{}/tcp/{}", v6, port_num).parse()
            {
                out.push(addr);
                return out;
            }
            warn!(
                "⚠️  Ignoring listen address '{}': DNS resolved to no usable IP",
                trimmed
            );
        }
        Err(e) => {
            warn!(
                "⚠️  Ignoring listen address '{}': DNS resolution failed: {}",
                trimmed, e
            );
        }
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
