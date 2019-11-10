use cql::codec::*;
use cql::message::*;
use cql::request::query::*;
use cql::types::*;

use std::io::Cursor;

#[test]
fn message() {
    let a = Query::from("");
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Query::from("a");
    let b = Query::from("a");
    assert_eq!(a, b);

    a.set_query("b");
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Query::from("");
    assert_eq!(m.length(), 10);

    m.set_query("a");
    assert_eq!(m.length(), 11);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let mut a = Query::from("a");
    let mut params = QueryParams::default();
    params.set_names(vec!["v".to_string(), "i".to_string(), "n".to_string()]);
    let id = Value::Some(marshal_varchar("a").unwrap().unwrap());
    let name = Value::Some(marshal_varchar("aa").unwrap().unwrap());
    let value = Value::Some(marshal_int(&123).unwrap().unwrap());
    params.set_values(vec![id, name, value]);
    params.set_result_page_size(10);
    a.set_params(params);

    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Query::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);

    codec.io().set_position(0);
}
