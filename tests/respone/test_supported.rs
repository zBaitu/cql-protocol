use cql::codec::*;
use cql::message::*;
use cql::response::supported::Supported;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = Supported::new();
    assert_eq!(a.is_response(), true);

    a.add_option("a".to_string(), vec!["b".to_string()]);
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Supported::new();
    let b = Supported::new();
    assert_eq!(a, b);

    a.add_option("a".to_string(), vec!["b".to_string()]);
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Supported::new();
    assert_eq!(m.length(), 2);

    m.add_option("a".to_string(), vec!["b".to_string()]);
    assert_eq!(m.length(), 10);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let mut a = Supported::new();
    a.add_option("a".to_string(), vec!["b".to_string()]);
    a.encode(&mut codec).unwrap();

    codec.io().set_position(0);
    let b = Supported::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
