/*#![warn(
clippy::all,
clippy::restriction,
clippy::pedantic,
clippy::nursery,
clippy::cargo,
)]*/

pub mod mdbfile;
pub mod table;
pub mod catalog;
pub mod data;
pub mod column;
pub mod conversion;
mod write;
pub mod utils;
mod map;
mod rc4;
pub mod money;
pub mod time;
pub mod numeric;
pub mod backend;
pub mod error;