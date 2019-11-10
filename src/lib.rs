#![feature(concat_idents)]
#![feature(const_vec_new)]
#![feature(specialization)]

#[macro_use]
extern crate maplit;
#[macro_use]
extern crate num_derive;
extern crate strum;
#[macro_use]
extern crate strum_macros;

pub mod result;

pub mod def;
pub mod types;
pub mod vint;

pub mod codec;
pub mod compression;
#[macro_use]
pub mod message;
pub mod request;
pub mod response;

pub mod frame;
