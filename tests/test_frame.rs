use cql::compression::Compression;
use cql::def::Version;
use cql::frame::Frame;
use cql::message::{Message, MessageKind};
use cql::request::Startup;

use std::io::Cursor;

#[test]
fn serde() {
    let mut frame = Frame::new(Version::V3, 0, Cursor::new(Vec::new()), None);

    let mut a = Startup::new();
    frame.encode(&a).unwrap();

    frame.io_mut().set_position(0);
    let (stream_id, b) = frame.decode().unwrap();
    let b = match b {
        MessageKind::Startup(b) => b,
        _ => unreachable!("{:?}", b),
    };

    assert_eq!(stream_id, 0);
    assert_eq!(a, b);

    a.set_compression(Compression::Lz4);
    assert_ne!(a, b);
}
