use core::str;
use std::ascii;

pub fn force_string(v: &Vec<u8>) -> String {
    str::from_utf8(v)
        .unwrap_or(
            format!(
                "<non utf-8> {}",
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
