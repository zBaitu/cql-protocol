use cql::codec::*;
use cql::message::*;
use cql::response::auth_challenge::AuthChallenge;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = AuthChallenge::new();
    assert_eq!(a.is_response(), true);

    a.set_token(Vec::from("a"));
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = AuthChallenge::new();
    let b = AuthChallenge::new();
    assert_eq!(a, b);

    a.set_token(Vec::from("a"));
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = AuthChallenge::new();
    assert_eq!(m.length(), 4);

    m.set_token(Vec::from("a"));
    assert_eq!(m.length(), 5);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = AuthChallenge::from(Vec::from("a"));
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = AuthChallenge::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
