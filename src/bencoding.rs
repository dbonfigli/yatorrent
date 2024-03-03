use std::collections::HashMap;

type IndexOfError = usize;

#[derive(PartialEq, Debug)]
pub enum Value {
    Error(IndexOfError),
    Str(String),
    Int(i64),
    List(Vec<Value>),
    Dict(HashMap<String, Value>),
}

impl Value {
    pub fn encode(&self) -> String {
        match self {
            Value::Error(_) => "".to_string(),
            Value::Str(string_value) => encode_string(string_value),
            Value::Int(int_value) => format!("i{}e", int_value),
            Value::List(list_value) => {
                let mut list_string = "l".to_string();
                list_value
                    .into_iter()
                    .for_each(|v| list_string = format!("{}{}", list_string, v.encode()));
                format!("{}e", list_string)
            }
            Value::Dict(dict_value) => {
                let mut dict_string = "d".to_string();
                dict_value.into_iter().for_each(|entry| {
                    dict_string = format!(
                        "{}{}{}",
                        dict_string,
                        encode_string(entry.0),
                        entry.1.encode()
                    )
                });
                format!("{}e", dict_string)
            }
        }
    }

    pub fn new(source: String) -> Self {
        let v: Vec<char> = source.chars().collect();
        Self::from_char_vec(&v, 0).0
    }

    // source is the source data
    // index is where to look from the source
    // return Value, index of next char to read
    fn from_char_vec(source: &Vec<char>, index: usize) -> (Self, usize) {
        match source.get(index) {
            // str
            Some('0'..='9') => {
                let mut index = index;
                let start_string_len_index = index;
                let end_string_len_index ;
                loop {
                    match source.get(index) {
                        Some('0'..='9') => index += 1,
                        Some(':') => {
                            end_string_len_index = index;
                            index += 1;
                            break;
                        }
                        _ => return (Value::Error(index), index),
                    }
                }
                let string_len_str: String = source[start_string_len_index..end_string_len_index].into_iter().collect();
                let string_len_opt = string_len_str.parse::<usize>();
                let string_len;
                match string_len_opt {
                    Ok(len) => string_len = len,
                    Err(_) => return (Value::Error(start_string_len_index), index),
                }
                if string_len == 0 {
                    return (Value::Str("".to_string()), index);
                }
                let end_string_index = index + string_len;
                if end_string_index > source.len() {
                    return (Value::Error(start_string_len_index), index)
                }
                let string_value: String = source[index..end_string_index].into_iter().collect();
                return (Value::Str(string_value), end_string_index);
            }

            Some('i') => {
                let mut index = index + 1;
                let start_int_index = index;
                let end_int_index;
                loop {
                    match source.get(index) {
                        Some('0'..='9' | '-') => index += 1,
                        Some('e') => {
                            end_int_index = index;
                            index += 1;
                            break;
                        }
                        _ => return (Value::Error(index), index),
                    }
                }
                let int_str: String = source[start_int_index..end_int_index].into_iter().collect();

                // check invalid
                if int_str == "-0" || (int_str.starts_with("0") && int_str.len() > 1) {
                    return (Value::Error(start_int_index), end_int_index);
                }

                // parse int and return
                let int_opt = int_str.parse::<i64>();
                match int_opt {
                    Ok(int_val) => return (Value::Int(int_val), end_int_index + 1),
                    Err(_) => return (Value::Error(start_int_index), index),
                }
            }

            // list
            Some('l') => {
                let mut l = Vec::new();
                let mut index = index + 1;
                loop {
                    match source.get(index) {
                        None => return (Value::Error(index), index),
                        Some('e') => {
                            index += 1;
                            break;
                        }
                        _ => {
                            let (v, new_index) = Self::from_char_vec(source, index);
                            if let Value::Error(index_of_error) = v {
                                return (Value::Error(index_of_error), index);
                            } else {
                                index = new_index;
                                l.push(v);
                            }
                        }
                    }
                }
                (Value::List(l), index)
            }

            // dict
            Some('d') => {
                let mut d = HashMap::new();
                let mut index = index + 1;
                loop {
                    match source.get(index) {
                        None => return (Value::Error(index), index),
                        Some('e') => {
                            index += 1;
                            break;
                        }
                        _ => {
                            let (v, new_index) = Self::from_char_vec(source, index);
                            if let Value::Str(k) = v {
                                index = new_index;
                                let (v, new_index) = Self::from_char_vec(source, index);
                                if let Value::Error(index_of_error) = v {
                                    return (Value::Error(index_of_error), index);
                                } else {
                                    index = new_index;
                                    d.insert(k, v);
                                }
                            } else {
                                return (Value::Error(index), index);
                            }
                        }
                    }
                }
                (Value::Dict(d), index)
            }

            _ => (Value::Error(index), index)
        }
    }
}

fn encode_string(s: &String) -> String {
    format!("{}:{}", s.len(), s)
}

#[cfg(test)]
mod tests {
    use super::Value;
    use std::collections::HashMap;

    #[test]
    fn encode_value() {
        let val_l = Value::List(vec![
            Value::Dict(HashMap::from([("k1".to_string(), Value::Int(1))])),
            Value::Int(2),
            Value::Int(3),
            Value::Str("bye".to_string()),
        ]);
        assert_eq!("ld2:k1i1eei2ei3e3:byee".to_string(), val_l.encode());
    }

    #[test]
    fn decode_int() {
        assert_eq!(Value::new("i2e".to_string()), Value::Int(2));
        assert_eq!(Value::new("i23e".to_string()), Value::Int(23));
        assert_eq!(Value::new("i-2312e".to_string()), Value::Int(-2312));
        assert_eq!(Value::new("i0e".to_string()), Value::Int(0));
        assert_eq!(Value::new("i-0e".to_string()), Value::Error(1));
        assert_eq!(Value::new("i01e".to_string()), Value::Error(1));
    }

    #[test]
    fn decode_str() {
        assert_eq!(Value::new("5:hello".to_string()), Value::Str("hello".to_string()));
        assert_eq!(Value::new("0:".to_string()), Value::Str("".to_string()));
        assert_eq!(Value::new("6:hello".to_string()), Value::Error(0));
    }

    #[test]
    fn decode_list() {
        let val_l = Value::List(vec![
            Value::Str("bye".to_string()),
        ]);
        assert_eq!(Value::new("l3:byee".to_string()), val_l);
    }


    #[test]
    fn decode_list2() {
        let val_l = Value::List(vec![
            Value::Str("bye".to_string()),
            Value::Str("hello".to_string()),
        ]);
        assert_eq!(Value::new("l3:bye5:helloe".to_string()), val_l);
    }

    #[test]
    fn decode_list3() {
        let val_l = Value::List(vec![
            Value::Dict(HashMap::from([("k1".to_string(), Value::Int(1))])),
            Value::Int(2),
            Value::Int(3),
            Value::Str("bye".to_string()),
        ]);
        assert_eq!(Value::new("ld2:k1i1eei2ei3e3:byee".to_string()), val_l);
    }

    #[test]
    fn decode_list4() {
        let val_l = Value::List(vec![
            Value::Int(2),
            Value::Int(3),
            Value::Int(-3),
        ]);
        assert_eq!(Value::new("li2ei3ei-3ee".to_string()), val_l);
    }

    #[test]
    fn decode_hash() {
        let val_l = Value::Dict(HashMap::from([
            ("k1".to_string(), Value::Str("e2".to_string())),
            ("k3".to_string(), Value::Str("e3".to_string()))
        ]));
        assert_eq!(Value::new("d2:k12:e22:k32:e3e".to_string()), val_l);
    }

    #[test]
    fn decode_hash2() {
        let val_l = Value::Dict(HashMap::from([
            ("k1".to_string(), Value::List(vec![
                Value::Int(0),
                Value::Str("hello".to_string()),
                Value::Str("".to_string()),
            ])),
            ("k2".to_string(), Value::Str("e3".to_string())),
        ]));
        assert_eq!(Value::new("d2:k1li0e5:hello0:e2:k22:e3e".to_string()), val_l);
    }
}
