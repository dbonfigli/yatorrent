use core::fmt;
use std::collections::HashMap;
use std::error::Error;
use std::str;

use crate::util::force_string;

type IndexOfError = usize;

#[derive(PartialEq, Debug, Clone)]
pub enum ErrorElem {
    Unknown,
    Str,
    Int,
    List,
    Dict,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParseError {
    pub elem: ErrorElem,
    pub index: IndexOfError,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "parse error parsing {:?} at char {}",
            self.elem, self.index
        )
    }
}

impl Error for ParseError {}

#[derive(PartialEq, Debug, Clone)]
pub enum Value {
    Error(ParseError),
    Str(Vec<u8>),
    Int(i64),
    List(Vec<Value>),
    Dict(HashMap<Vec<u8>, Value>, usize, usize), //hash, startIndex, endIndex (index of next char not comprising the dict, similar to range)
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Error(e) => {
                write!(f, "{:?}", e)
            }
            Value::Str(s) => {
                write!(f, "{}", force_string(s))
            }
            Value::Int(i) => write!(f, "{i}"),
            Value::List(l) => {
                write!(
                    f,
                    "[{}]",
                    l.iter()
                        .map(|i| format!("{i}"))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            Value::Dict(h, _, _) => {
                write!(
                    f,
                    "{{ {} }}",
                    h.iter()
                        .map(|(k, v)| format!("{}: {}", force_string(k), v))
                        .collect::<Vec<String>>()
                        .join("; ")
                )
            }
        }
    }
}

impl Value {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Value::Error(_) => b"!!!error!!!".to_vec(),
            Value::Str(v) => encode_str(v),
            Value::Int(v) => encode_int(v),
            Value::List(v) => encode_list(v),
            Value::Dict(v, _, _) => encode_dict(v),
        }
    }

    pub fn new(source: &Vec<u8>) -> Self {
        from_char_vec(&source, 0).0
    }

    pub fn new_with_size(source: &Vec<u8>) -> (Value, usize) {
        from_char_vec(&source, 0)
    }

    fn new_error(elem: ErrorElem, index: IndexOfError) -> Self {
        Value::Error(ParseError { elem, index })
    }
}

// keys must be byte strings and must appear in lexicographical order - libtorrent is very strict on this
fn encode_dict(d: &HashMap<Vec<u8>, Value>) -> Vec<u8> {
    let mut keys: Vec<_> = d.keys().collect();
    keys.sort();
    let mut v = b"d".to_vec();
    for k in keys {
        v.append(&mut encode_str(k));
        v.append(&mut d.get(k).unwrap().encode());
    }
    v.push(b'e');
    v
}

fn encode_list(l: &Vec<Value>) -> Vec<u8> {
    let mut v = b"l".to_vec();
    l.into_iter().for_each(|val| v.append(&mut val.encode()));
    v.push(b'e');
    v
}

fn encode_int(i: &i64) -> Vec<u8> {
    let mut v = b"i".to_vec();
    v.append(&mut i.to_string().as_bytes().to_vec());
    v.push(b'e');
    v
}

fn encode_str(s: &Vec<u8>) -> Vec<u8> {
    let mut v = s.len().to_string().as_bytes().to_vec();
    v.push(b':');
    v.append(&mut s.clone());
    v
}

// source is the source data
// index is where to look from the source
// return Value, index of next char to read
fn from_char_vec(source: &Vec<u8>, index: usize) -> (Value, usize) {
    match source.get(index) {
        Some(b'0'..=b'9') => parse_str(source, index),
        Some(b'i') => parse_int(source, index),
        Some(b'l') => parse_list(source, index),
        Some(b'd') => parse_dict(source, index),
        _ => (Value::new_error(ErrorElem::Unknown, index), index),
    }
}

fn parse_str(source: &Vec<u8>, index: usize) -> (Value, usize) {
    let mut index = index;
    let start_string_len_index = index;
    let end_string_len_index;
    loop {
        match source.get(index) {
            Some(b'0'..=b'9') => index += 1,
            Some(b':') => {
                end_string_len_index = index;
                index += 1;
                break;
            }
            _ => return (Value::new_error(ErrorElem::Str, index), index),
        }
    }
    let string_len_str = match str::from_utf8(&source[start_string_len_index..end_string_len_index])
    {
        Ok(s) => s,
        Err(_) => {
            return (
                Value::new_error(ErrorElem::Str, start_string_len_index),
                index,
            )
        }
    };
    let string_len_opt = string_len_str.parse::<usize>();
    let string_len;
    match string_len_opt {
        Ok(len) => string_len = len,
        Err(_) => {
            return (
                Value::new_error(ErrorElem::Str, start_string_len_index),
                index,
            )
        }
    }
    if string_len == 0 {
        return (Value::Str(Vec::new()), index);
    }
    let end_string_index = index + string_len;
    if end_string_index > source.len() {
        return (
            Value::new_error(ErrorElem::Str, start_string_len_index),
            index,
        );
    }
    (
        Value::Str(source[index..end_string_index].to_vec()),
        end_string_index,
    )
}

fn parse_int(source: &Vec<u8>, index: usize) -> (Value, usize) {
    let mut index = index + 1;
    let start_int_index = index;
    let end_int_index;
    loop {
        match source.get(index) {
            Some(b'0'..=b'9' | b'-') => index += 1,
            Some(b'e') => {
                end_int_index = index;
                index += 1;
                break;
            }
            _ => return (Value::new_error(ErrorElem::Int, index), index),
        }
    }
    let int_str = match str::from_utf8(&source[start_int_index..end_int_index]) {
        Ok(s) => s,
        Err(_) => return (Value::new_error(ErrorElem::Int, start_int_index), index),
    };

    // check invalid
    if int_str == "-0" || (int_str.starts_with("0") && int_str.len() > 1) {
        return (
            Value::new_error(ErrorElem::Int, start_int_index),
            end_int_index,
        );
    }

    // parse int and return
    let int_opt = int_str.parse::<i64>();
    match int_opt {
        Ok(int_val) => (Value::Int(int_val), end_int_index + 1),
        Err(_) => (Value::new_error(ErrorElem::Int, start_int_index), index),
    }
}

fn parse_list(source: &Vec<u8>, index: usize) -> (Value, usize) {
    let mut l = Vec::new();
    let mut index = index + 1;
    loop {
        match source.get(index) {
            None => return (Value::new_error(ErrorElem::List, index), index),
            Some(b'e') => {
                index += 1;
                break;
            }
            _ => {
                let (v, new_index) = from_char_vec(source, index);
                if let Value::Error(_) = v {
                    return (v, index);
                } else {
                    index = new_index;
                    l.push(v);
                }
            }
        }
    }
    (Value::List(l), index)
}

fn parse_dict(source: &Vec<u8>, index: usize) -> (Value, usize) {
    let mut d = HashMap::new();
    let start = index;
    let mut index = index + 1;
    loop {
        match source.get(index) {
            None => return (Value::new_error(ErrorElem::Dict, index), index),
            Some(b'e') => {
                index += 1;
                break;
            }
            _ => {
                let (v, new_index) = from_char_vec(source, index);
                if let Value::Str(k) = v {
                    index = new_index;
                    let (v, new_index) = from_char_vec(source, index);
                    if let Value::Error(_) = v {
                        return (v, index);
                    } else {
                        index = new_index;
                        d.insert(k, v);
                    }
                } else {
                    return (Value::new_error(ErrorElem::Dict, index), index);
                }
            }
        }
    }

    (Value::Dict(d, start, index), index)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Value;
    use crate::bencoding::ErrorElem;
    use crate::bencoding::ParseError;

    #[test]
    fn encode_value() {
        let val_l = Value::List(vec![
            Value::Dict(HashMap::from([(b"k1".to_vec(), Value::Int(1))]), 0, 0),
            Value::Int(2),
            Value::Int(3),
            Value::Str(b"bye".to_vec()),
        ]);
        assert_eq!(b"ld2:k1i1eei2ei3e3:byee".to_vec(), val_l.encode());
    }

    #[test]
    fn decode_int() {
        assert_eq!(Value::new(&b"i2e".to_vec()), Value::Int(2));
        assert_eq!(Value::new(&b"i23e".to_vec()), Value::Int(23));
        assert_eq!(Value::new(&b"i-2312e".to_vec()), Value::Int(-2312));
        assert_eq!(Value::new(&b"i0e".to_vec()), Value::Int(0));
        assert_eq!(
            Value::new(&b"i-0e".to_vec()),
            Value::Error(ParseError {
                elem: ErrorElem::Int,
                index: 1
            })
        );
        assert_eq!(
            Value::new(&b"i01e".to_vec()),
            Value::Error(ParseError {
                elem: ErrorElem::Int,
                index: 1
            })
        );
    }

    #[test]
    fn decode_str() {
        assert_eq!(
            Value::new(&b"5:hello".to_vec()),
            Value::Str(b"hello".to_vec())
        );
        assert_eq!(Value::new(&b"0:".to_vec()), Value::Str(b"".to_vec()));
        assert_eq!(
            Value::new(&b"6:hello".to_vec()),
            Value::Error(ParseError {
                elem: ErrorElem::Str,
                index: 0
            })
        );
    }

    #[test]
    fn decode_list() {
        let val_l = Value::List(vec![Value::Str(b"bye".to_vec())]);
        assert_eq!(Value::new(&b"l3:byee".to_vec()), val_l);
    }

    #[test]
    fn decode_list2() {
        let val_l = Value::List(vec![
            Value::Str(b"bye".to_vec()),
            Value::Str(b"hello".to_vec()),
        ]);
        assert_eq!(Value::new(&b"l3:bye5:helloe".to_vec()), val_l);
    }

    #[test]
    fn decode_list3() {
        let val_l = Value::List(vec![
            Value::Dict(HashMap::from([(b"k1".to_vec(), Value::Int(1))]), 1, 10),
            Value::Int(2),
            Value::Int(3),
            Value::Str(b"bye".to_vec()),
        ]);
        assert_eq!(Value::new(&b"ld2:k1i1eei2ei3e3:byee".to_vec()), val_l);
    }

    #[test]
    fn decode_list4() {
        let val_l = Value::List(vec![Value::Int(2), Value::Int(3), Value::Int(-3)]);
        assert_eq!(Value::new(&b"li2ei3ei-3ee".to_vec()), val_l);
    }

    #[test]
    fn decode_hash() {
        let val_l = Value::Dict(
            HashMap::from([
                (b"k1".to_vec(), Value::Str(b"e2".to_vec())),
                (b"k3".to_vec(), Value::Str(b"e3".to_vec())),
            ]),
            0,
            18,
        );
        assert_eq!(Value::new(&b"d2:k12:e22:k32:e3e".to_vec()), val_l);
    }

    #[test]
    fn decode_hash2() {
        let val_l = Value::Dict(
            HashMap::from([
                (
                    b"k1".to_vec(),
                    Value::List(vec![
                        Value::Int(0),
                        Value::Str(b"hello".to_vec()),
                        Value::Str(b"".to_vec()),
                    ]),
                ),
                (b"k2".to_vec(), Value::Str(b"e3".to_vec())),
            ]),
            0,
            28,
        );
        assert_eq!(Value::new(&b"d2:k1li0e5:hello0:e2:k22:e3e".to_vec()), val_l);
    }
}
