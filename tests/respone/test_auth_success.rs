use cql::codec::*;
use cql::message::*;
use cql::response::auth_success::AuthSuccess;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = AuthSuccess::new();
    assert_eq!(a.is_response(), true);

    a.set_token(Vec::from("a"));
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = AuthSuccess::new();
    let b = AuthSuccess::new();
    assert_eq!(a, b);

    a.set_token(Vec::from("a"));
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = AuthSuccess::new();
    assert_eq!(m.length(), 4);

    m.set_token(Vec::from("a"));
    assert_eq!(m.length(), 5);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = AuthSuccess::from(Vec::from("a"));
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = AuthSuccess::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
