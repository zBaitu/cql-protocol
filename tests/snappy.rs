use cql::compression::Compressor;

use snap::{Decoder, Encoder};
use std::io;

pub struct Snappy {
    encoder: Encoder,
    decoder: Decoder,
}

impl Snappy {
    pub fn new() -> Self {
        Snappy {
            encoder: Encoder::new(),
            decoder: Decoder::new(),
        }
    }
}

impl Compressor for Snappy {
    fn compress(&mut self, v: &[u8]) -> io::Result<Vec<u8>> {
        let v = self.encoder.compress_vec(v)?;
        Ok(v)
    }

    fn decompress(&mut self, v: &[u8]) -> io::Result<Vec<u8>> {
        let v = self.decoder.decompress_vec(v)?;
        Ok(v)
    }
}

#[test]
fn test() {
    let mut snappy = Snappy::new();
    let a = "hello world";
    let v = snappy.compress(&mut a.as_bytes()).unwrap();
    println!("{}", v.len());
    let v = snappy.decompress(&mut v.as_slice()).unwrap();
    println!("{}", v.len());
    let b = std::str::from_utf8(&v).unwrap();
    assert_eq!(a, b);

    let a = "白兔";
    let v = snappy.compress(&mut a.as_bytes()).unwrap();
    let v = snappy.decompress(&mut v.as_slice()).unwrap();
    let b = std::str::from_utf8(&v).unwrap();
    assert_eq!(a, b);
}
