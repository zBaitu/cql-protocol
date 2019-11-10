use cql::codec::*;
use cql::def::*;
use cql::message::*;
use cql::response::event::*;

use std::{
    io::Cursor,
    net::{IpAddr, Ipv6Addr, SocketAddr}
};

fn topology_change() -> Event {
    let e = TopologyChange::new(TopologyChangeType::NewNode, SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 80));
    let e = Event::TopologyChange(e);
    e
}

fn status_change() -> Event {
    let e = StatusChange::new(StatusChangeType::Up, SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 80));
    let e = Event::StatusChange(e);
    e
}

fn schema_change() -> Event {
    let mut e = SchemaChange::new(SchemaChangeType::Created, SchemaChangeTarget::Function, "a");
    e.set_name("b");
    e.set_args(vec!["a".to_string(), "b".to_string()]);
    let e = Event::SchemaChange(e);
    e
}

fn default() -> Event {
    topology_change()
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
    let b = topology_change();
    assert_eq!(a, b);

    let c = status_change();
    assert_ne!(a, c);
}

#[test]
fn length() {
    let m = default();
    assert_eq!(m.length(), 48);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = default();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Event::decode(&mut codec).unwrap();
    assert_eq!(a, b);

    codec.io().set_position(0);

    let a = schema_change();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Event::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
