use anyhow::{Result, bail};
use std::net::{SocketAddr, ToSocketAddrs};

pub struct Magnet {
    pub info_hash: [u8; 20],
    pub tracker_urls: Vec<String>,
    pub peer_addresses: Vec<SocketAddr>,
}

impl Magnet {
    pub fn new(uri: String) -> Result<Self> {
        let magnet_url = match url::Url::parse(&uri) {
            Err(e) => bail!("Invalid magnet URI: {e}"),
            Ok(magnet_url) => magnet_url,
        };

        let info_hash = match magnet_url.query_pairs().find(|(key, _)| key == "xt") {
            None => bail!("No info hash found in magnet URI"),
            Some((_, info_hash)) => {
                if let Some(info_hash) = info_hash.strip_prefix("urn:btih:") {
                    match info_hash.len() {
                        40 => {
                            // hex encoded info hash
                            let info_hash = hex::decode(info_hash)?;
                            info_hash.try_into().expect(
                                "a properly decoded 40 hex chart string must always be 20b long",
                            )
                        }
                        32 => {
                            // allow lowercase letters on base32 representation
                            let info_hash = info_hash.to_ascii_uppercase();

                            // base32 encoded info hash
                            let decoded = base32::decode(
                                base32::Alphabet::Rfc4648 { padding: false },
                                &info_hash,
                            );
                            let decoded = match decoded {
                                Some(decoded) => decoded,
                                None => bail!("Invalid Base32 info hash"),
                            };
                            let info_hash = match decoded.try_into() {
                                Ok(hash) => hash,
                                Err(_) => bail!("Base32 info hash must decode to exactly 20 bytes"),
                            };
                            info_hash
                        }
                        _ => bail!(
                            "Info hash must be either 40 hex characters or 32 Base32 characters long"
                        ),
                    }
                } else if info_hash.starts_with("urn:btmh:") {
                    bail!("Multi hash formatted info hash (urn:btmh) not yet supported");
                } else {
                    bail!(
                        "Unknown info hash format, not starting with either urn:btih: or urn:btmh:"
                    );
                }
            }
        };

        let tracker_urls: Vec<String> = magnet_url
            .query_pairs()
            .filter(|(key, _)| key == "tr")
            .map(|(_, value)| {
                percent_encoding::percent_decode_str(&value)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .collect();
        // Validate all tracker URLs
        for url in &tracker_urls {
            if url::Url::parse(url).is_err() {
                bail!("Invalid tracker URL: {}", url);
            }
        }

        let mut peer_addresses = Vec::new();
        for (_, xpe_entry) in magnet_url.query_pairs().filter(|(key, _)| key == "x.pe") {
            let peer_addr = percent_encoding::percent_decode_str(&xpe_entry).decode_utf8_lossy();

            // this is blocking but that is fine at startup, let's avoid making this async only for this dns resolution
            match peer_addr.to_socket_addrs() {
                Err(e) => {
                    log::warn!("cannot resolve x.pe entry {peer_addr}, ignoring it: {e}")
                }
                Ok(socket_addr_iter) => {
                    for p in socket_addr_iter {
                        log::debug!("x.pe entry {peer_addr} resolved to {p}");
                        peer_addresses.push(p);
                    }
                }
            }
        }

        Ok(Magnet {
            info_hash,
            tracker_urls,
            peer_addresses,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn test_valid_magnet_uri() {
        let uri = "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&tr=http%3A%2F%2Ftracker.example.com%3A6969%2Fannounce&x.pe=192.168.1.1:6882&x.pe=[2001:db8::1]:6883";

        let magnet = Magnet::new(uri.to_string()).expect("should be a valid magnet");

        // Check info hash
        assert_eq!(
            hex::encode(magnet.info_hash),
            "c9e15763f722f23e98a29decdfae341b98d53056"
        );

        // Check tracker URLs
        assert_eq!(
            magnet.tracker_urls,
            vec!["http://tracker.example.com:6969/announce"]
        );

        // Check peer addresses
        assert_eq!(
            magnet.peer_addresses,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 6882),
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                    6883
                ),
            ]
        );
    }

    #[test]
    fn test_valid_magnet_uri_base32() {
        let uri = "magnet:?xt=urn:btih:ZHQVOY7XELZD5GFCTXWN7LRUDOMNKMCW&tr=http%3A%2F%2Ftracker.example.com%3A6969%2Fannounce&x.pe=example.com:6881&x.pe=192.168.1.1:6882&x.pe=[2001:db8::1]:6883";

        let magnet = Magnet::new(uri.to_string()).expect("should be a valid magnet");

        // The Base32 and hex representations should produce the same 20 bytes.
        assert_eq!(
            hex::encode(magnet.info_hash),
            "c9e15763f722f23e98a29decdfae341b98d53056"
        );
    }

    #[test]
    fn test_valid_magnet_uri_base32_lowercase() {
        let uri = "magnet:?xt=urn:btih:ZHQVOY7XELZD5GFCTXWN7LRUDOMNKMCw&tr=http%3A%2F%2Ftracker.example.com%3A6969%2Fannounce&x.pe=example.com:6881&x.pe=192.168.1.1:6882&x.pe=[2001:db8::1]:6883";

        let magnet = Magnet::new(uri.to_string()).expect("should be a valid magnet");

        // The Base32 and hex representations should produce the same 20 bytes.
        assert_eq!(
            hex::encode(magnet.info_hash),
            "c9e15763f722f23e98a29decdfae341b98d53056"
        );
    }

    #[test]
    fn test_invalid_magnet_uris() {
        // Missing info hash
        assert!(Magnet::new("magnet:?tr=http://tracker.example.com".to_string()).is_err());

        // Invalid info hash format
        assert!(Magnet::new("magnet:?xt=invalid_hash".to_string()).is_err());

        // Info hash wrong length
        assert!(Magnet::new("magnet:?xt=urn:btih:abc123".to_string()).is_err());

        // Invalid tracker URL
        assert!(
            Magnet::new(
                "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&tr=not_a_url"
                    .to_string()
            )
            .is_err()
        );
    }
}
