use cql::codec::*;
use cql::def::*;
use cql::message::*;
use cql::request::register::Register;

use std::io::Cursor;

#[test]
fn message() {
    let a = Register::new();
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Register::new();
    let b = Register::new();
    assert_eq!(a, b);

    a.set_events(&[EventType::TopologyChange]);
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Register::new();
    assert_eq!(m.length(), 2);

    m.set_events(&[EventType::TopologyChange]);
    assert_eq!(m.length(), 19);

    m.set_events(&[EventType::TopologyChange, EventType::StatusChange]);
    assert_eq!(m.length(), 34);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Register::new();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Register::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);

    codec.io().set_position(0);

    let a = Register::from(&[EventType::TopologyChange, EventType::StatusChange]);
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Register::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);
}
