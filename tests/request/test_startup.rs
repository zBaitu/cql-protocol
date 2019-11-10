use cql::codec::*;
use cql::compression::Compression;
use cql::message::*;
use cql::request::startup::Startup;

use std::io::Cursor;

#[test]
fn message() {
    let a = Startup::new();
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Startup::new();
    let b = Startup::new();
    assert_eq!(a, b);

    a.set_compression(Compression::Lz4);
    assert_ne!(a, b);
    assert_eq!(a.compression().unwrap(), Compression::Lz4);
}

#[test]
fn length() {
    let mut m = Startup::new();
    assert_eq!(m.length(), 22);

    m.set_compression(Compression::Lz4);
    assert_eq!(m.length(), 40);

    m.set_compression(Compression::Snappy);
    assert_eq!(m.length(), 43);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Startup::new();
    a.encode(&mut codec).unwrap();
    assert_eq!(codec.io().get_ref().len(), 22);
    assert_eq!(codec.io().position(), 22);

    codec.io().set_position(0);
    assert_eq!(codec.io().position(), 0);

    let b = Startup::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
