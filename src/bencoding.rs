use std::collections::HashMap;

enum Value {
    Str(String),
    Int(i64),
    List(Vec<Value>),
    Dict(HashMap<String, Value>),

}

impl Value {
    fn encode(&self) -> String {
        match self {
            Value::Str(string_value) => encode_string(string_value),
            Value::Int(int_value) => format!("i{}e", int_value),
            Value::List(list_value) => {
                let mut list_string = "l".to_string();
                list_value.into_iter().for_each(|v| list_string = format!("{}{}", list_string, v.encode()));
                format!("{}e", list_string)
            }
            Value::Dict(dict_value) => {
                let mut dict_string = "d".to_string();
                dict_value.into_iter().for_each(|entry| dict_string = format!("{}{}:{}", dict_string, encode_string(entry.0), entry.1.encode()));
                format!("{}e", dict_string)
            }
        }
    }

    fn new(source: String) -> Self {
        Value::Str("asd".to_string())
    }
}

fn encode_string(s: &String) -> String {
    format!("{}:{}", s.len(), s)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::Value;

    #[test]
    fn encode_value() {
        let val_l = Value::List(vec![
            Value::Dict(HashMap::from([
                ("k1".to_string(), Value::Int(1)),
            ])),
            Value::Int(2),
            Value::Int(3),
            Value::Str("bye".to_string())]);

        assert_eq!("ld2:k1:i1eei2ei3e3:byee".to_string(), val_l.encode());
    }
}


