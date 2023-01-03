#[derive(Debug)]
pub enum MdbError {

  // Usage Map Errors
  UnknownMapType,
  StartedFromPartition,
  NoFreePages,

  NextDataPageCycle,

  ReadPage,
  JetVersion
}