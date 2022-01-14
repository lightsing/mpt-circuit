//! mpt demo

#![allow(dead_code)]
#![deny(missing_docs)]
#![deny(unsafe_code)]

#[macro_use]
extern crate log;

pub use crate::serde::{Hash, Row, RowDeError};
use ::serde::Deserialize;

pub mod mpt;
pub mod operations;
mod serde;
