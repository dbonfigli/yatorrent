use core::str;
use std::ascii;

pub fn force_string(v: &Vec<u8>) -> String {
    str::from_utf8(v)
        .unwrap_or(
            format!(
                "<non_utf-8>{}",
                str::from_utf8(
                    &v.iter()
                        .flat_map(|b| ascii::escape_default(*b))
                        .collect::<Vec<u8>>()
                )
                .unwrap_or("??")
            )
            .as_str(),
        )
        .to_string()
}

pub fn pretty_info_hash(info_hash: [u8; 20]) -> String {
    info_hash
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const NAME: &str = env!("CARGO_PKG_NAME");
const GIT_COMMIT: &str = env!("GIT_COMMIT");

pub fn version_string() -> String {
    if GIT_COMMIT == "unknown" {
        VERSION.to_string()
    } else {
        format!("{NAME} {VERSION} ({GIT_COMMIT})")
    }
}

pub struct FileEntry {
    pub path: String,
    pub size: u64,
}

impl FileEntry {
    pub fn new(path: String, size: u64) -> Self {
        FileEntry { path, size }
    }
}
