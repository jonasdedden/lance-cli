pub mod cli;
pub mod commands;
pub mod dataset;
pub mod error;
pub mod indices;
pub mod output;
pub mod projection;
pub mod row_id;
pub mod stats;

#[cfg(test)]
mod test_support;

pub use error::{Error, Result};
