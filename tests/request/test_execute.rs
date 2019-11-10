use cql::codec::*;
use cql::message::*;
use cql::request::execute::Execute;
use cql::request::query::QueryParams;

use std::io::Cursor;

fn default() -> Execute {
    Execute::from(Vec::new(), QueryParams::default())
}

#[test]
fn message() {
    let a = default();
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let a = default();
    let b = default();
    assert_eq!(a, b);

    let b = Execute::from(vec![0], QueryParams::default());
    assert_ne!(a, b);
}

#[test]
fn length() {
    let m = default();
    assert_eq!(m.length(), 8);

    let m = Execute::from(vec![0], QueryParams::default());
    assert_eq!(m.length(), 9);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = default();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Execute::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);

    codec.io().set_position(0);
}
