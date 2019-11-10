use cql::codec::*;
use cql::message::*;
use cql::request::auth_response::AuthResponse;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = AuthResponse::new();
    assert_eq!(a.is_response(), false);

    a.set_token(Vec::from("a"));
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = AuthResponse::new();
    let b = AuthResponse::new();
    assert_eq!(a, b);

    a.set_token(Vec::from("a"));
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = AuthResponse::new();
    assert_eq!(m.length(), 4);

    m.set_token(crate::auth_response_token("cassandra", "cassandra"));
    assert_eq!(m.length(), 24);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let mut a = AuthResponse::new();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = AuthResponse::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    a.set_token(crate::auth_response_token("cassandra", "cassandra"));
    a.encode(&mut codec).unwrap();
    println!("{:?}", codec.io().get_ref());
    codec.io().set_position(0);
    let b = AuthResponse::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
