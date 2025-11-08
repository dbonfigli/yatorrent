use anyhow::{Result, bail};
use percent_encoding;

pub struct Magnet {
    pub info_hash: [u8; 20],
    pub tracker_urls: Vec<String>,
    pub peer_addresses: Vec<String>,
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
                    if info_hash.len() != 40 {
                        bail!("Info hash must be 40 hex characters long");
                    }
                    let info_hash = hex::decode(info_hash)?;
                    info_hash
                        .try_into()
                        .expect("a properly decoded 40 hex chart string must always be 20b long")
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

        let peer_addresses: Vec<String> = magnet_url
            .query_pairs()
            .filter(|(key, _)| key == "x.pe")
            .map(|(_, value)| {
                percent_encoding::percent_decode_str(&value)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .collect();

        // Validate peer addresses format (host:port)
        for addr in &peer_addresses {
            match addr.rsplit_once(':') {
                None => bail!("Invalid peer address format (missing port): {}", addr),
                Some((_, port_str)) => {
                    // Validate that port is a valid u16
                    if let Err(_) = port_str.parse::<u16>() {
                        bail!("Invalid port number in peer address: {}", addr);
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
    use super::*;

    #[test]
    fn test_valid_magnet_uri() {
        let uri = "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&tr=http%3A%2F%2Ftracker.example.com%3A6969%2Fannounce&x.pe=example.com:6881&x.pe=192.168.1.1:6882&x.pe=[2001:db8::1]:6883";

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
            vec!["example.com:6881", "192.168.1.1:6882", "[2001:db8::1]:6883"]
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

        // Invalid peer address (no port)
        assert!(
            Magnet::new(
                "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&x.pe=example.com"
                    .to_string()
            )
            .is_err()
        );

        // Invalid peer address (invalid port)
        assert!(Magnet::new(
            "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&x.pe=example.com:invalid"
                .to_string()
        )
        .is_err());

        // Invalid peer address (port out of range)
        assert!(Magnet::new(
            "magnet:?xt=urn:btih:c9e15763f722f23e98a29decdfae341b98d53056&x.pe=example.com:65536"
                .to_string()
        )
        .is_err());
    }
}
