use std::io;

#[derive(Clone, Copy, Debug, Display, EnumString, PartialEq)]
pub enum Compression {
    #[strum(serialize = "lz4")]
    Lz4,
    #[strum(serialize = "snappy")]
    Snappy,
}

pub trait Compressor {
    fn compress(&mut self, v: &[u8]) -> io::Result<Vec<u8>>;
    fn decompress(&mut self, v: &[u8]) -> io::Result<Vec<u8>>;
}
