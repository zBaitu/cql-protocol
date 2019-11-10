use cql::codec::*;
use cql::def::ErrorCode;
use cql::message::*;
use cql::response::error::*;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = Error::new();
    assert_eq!(a.is_response(), true);

    a.set_error(ErrorCode::AuthenticationError, "a");
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Error::new();
    let b = Error::new();
    assert_eq!(a, b);

    a.set_error(ErrorCode::AuthenticationError, "a");
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Error::new();
    assert_eq!(m.length(), 6);

    m.set_error(ErrorCode::AuthenticationError, "a");
    assert_eq!(m.length(), 7);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Error::from(ErrorCode::AuthenticationError, "a");
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Error::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    let alread_exists = AlreadyExists::new("a", "b");
    let a = Error::from_exception(ExceptionKind::AlreadyExists(alread_exists), "a");
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Error::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}

