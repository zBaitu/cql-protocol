use cql::codec::*;
use cql::message::*;
use cql::response::result::*;

use std::io::Cursor;

fn void() -> Result {
    Result::new(ResultBody::Void(Void::default()))
}

fn rows() -> Result {
    Result::new(ResultBody::Rows(Rows::default()))
}

fn prepared() -> Result {
    Result::new(ResultBody::Prepared(Prepared::default()))
}

fn set_keyspace() -> Result {
    Result::new(ResultBody::SetKeyspace(SetKeyspace::new("a")))
}

fn default() -> Result {
    void()
}

#[test]
fn message() {
    let a = default();
    assert_eq!(a.is_response(), true);
    println!("{}", a);
}

#[test]
fn eq() {
    let a = default();
    let b = void();
    assert_eq!(a, b);

    let b = set_keyspace();
    assert_ne!(a, b);
}

#[test]
fn length() {
    let m = default();
    assert_eq!(m.length(), 4);

    let m = set_keyspace();
    assert_eq!(m.length(), 7);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = default();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Result::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    let a = set_keyspace();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Result::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    let a = rows();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Result::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    let a = prepared();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Result::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
