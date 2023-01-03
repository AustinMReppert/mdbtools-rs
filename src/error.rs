#[derive(Debug)]
pub enum MdbError {
  // Finding/Reading row errors
  RowTooLarge,
  InvalidRowBounds,

  // Read table errors
  ReadNonTableCatalogueEntry,
  InvalidTableDefinition,

  // Usage Map Errors
  UnknownMapType,
  StartedFromPartition,
  NoFreePages,
  UsageMapInvalidSize,

  NextDataPageCycle,

  PageBufferOverflow,

  DecodeString,

  UnhandledType,

  ReadPage,
  JetVersion
}