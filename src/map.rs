use crate::mdbfile::Mdb;

pub struct UsageMap {
  pub mdb: Mdb,
  pub start: u16,
  pub length: u16,
}