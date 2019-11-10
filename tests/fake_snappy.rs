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
        let mut v = self.encoder.compress_vec(v)?;
        v.append(&mut vec![0; 10]);
        Ok(v)
    }

    fn decompress(&mut self, v: &[u8]) -> io::Result<Vec<u8>> {
        let v = self.decoder.decompress_vec(v)?;
        Ok(v)
    }
}
