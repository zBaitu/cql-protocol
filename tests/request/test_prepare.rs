use cql::codec::*;
use cql::message::*;
use cql::request::prepare::Prepare;

use std::io::Cursor;

#[test]
fn message() {
    let a = Prepare::from("");
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Prepare::from("a");
    let b = Prepare::from("a");
    assert_eq!(a, b);

    a.set_query("b");
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Prepare::from("");
    assert_eq!(m.length(), 8);

    m.set_query("a");
    assert_eq!(m.length(), 9);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Prepare::from("a");
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Prepare::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);

    codec.io().set_position(0);
}
