use crate::catalog::{CatalogEntry, TableCatalogEntry};
use crate::column::Column;
use crate::data::{mdb_find_page_row_packed};
use crate::mdbfile::{Mdb, MdbFormatVersion};
use crate::column::ColumnType;
use crate::conversion::decode_mdb_string;
use crate::error::MdbError;
use crate::map::{UsageMap};
use crate::utils::get_u16;

pub struct Table {
  pub name: String,
  pub row_count: u32,
  pub variable_column_count: u16,
  pub column_count: u16,
  pub first_data_page: u16,
  pub columns: Vec<Column>,
  real_index_count: u32,
  pub current_page_number: u32,
  pub current_physical_page_number: u16,
  pub current_row: u16,
  pub is_temporary_table: bool,
  pub(crate) strategy: TableStrategy,
  pub mdb: Mdb,

  pub(crate) first_table_definition_page: u32,
  pub usage_map: UsageMap,
}

#[repr(u8)]
#[derive(PartialEq)]
pub enum TableStrategy {
  TableScan = 0,
  LeafScan = 1,
  IndexScan = 2,
}

impl Table {
  pub fn find_column_index(&self, name: &str) -> Option<usize> {
    self.columns.iter().position(|col| { col.name.eq(name) })
  }

  pub fn read_columns(&mut self) -> Result<(), MdbError> {
    self.mdb.read_page(self.first_table_definition_page)?;

    let mut cur_pos: u16 = self.mdb.format.tab_cols_start_offset as u16 + (self.real_index_count as u16 * self.mdb.format.tab_ridx_entry_size as u16);

    let mut column_buffer: Vec<u8> = vec![0; self.mdb.format.tab_col_entry_size as usize];

    let len = column_buffer.len();

    for _i in 0..self.column_count {
      self.columns.push(Column::new(self.mdb.encoding));
    }

    // Column Attributes
    for column in self.columns.iter_mut() {
      self.mdb.read_page_if_n(Some(&mut column_buffer), &mut cur_pos, len as u16)?;

      column.column_type = column_buffer[0].try_into().expect("Invalid column type.");
      column.number = column_buffer[self.mdb.format.column_number_offset];
      column.row_column_number = get_u16(&column_buffer, self.mdb.format.table_row_column_number_offset);

      match column.column_type {
        ColumnType::Numeric | ColumnType::Money | ColumnType::Float | ColumnType::Double => {
          column.scale = column_buffer[self.mdb.format.column_scale_offset];
          column.precision = column_buffer[self.mdb.format.column_precision_offset];
        }
        _ => {}
      }

      column.is_fixed = column_buffer[self.mdb.format.col_flags_offset] & 0x01 != 0;
      column.is_long_auto = column_buffer[self.mdb.format.col_flags_offset] & 0x04 == 0;
      column.is_uuid_auto = column_buffer[self.mdb.format.col_flags_offset] & 0x40 == 0;
      column.is_hyperlink = column_buffer[self.mdb.format.col_flags_offset] & 0x80 != 0;

      column.fixed_offset = get_u16(&column_buffer, self.mdb.format.table_column_offset_fixed);
      column.var_col_num = get_u16(&column_buffer, self.mdb.format.tab_col_offset_var);

      column.size = if column.column_type != ColumnType::Bool {get_u16(&column_buffer, self.mdb.format.column_size_offset) } else { 0 };
    }

    // Column names
    for column in self.columns.iter_mut() {
      let name_size: usize;

      if self.mdb.mdb_file.jet_version == MdbFormatVersion::JET3 {
        name_size = self.mdb.read_page_if_8(&mut cur_pos)? as usize;
      } else {
        name_size = self.mdb.read_page_if_16(&mut cur_pos)? as usize;
      }
      let mut column_name_buffer: Vec<u8> = vec![];
      column_name_buffer.resize(name_size as usize, 0);
      self.mdb.read_page_if_n(Some(&mut column_name_buffer), &mut cur_pos, name_size as u16)?;

      column.name = decode_mdb_string(self.mdb.mdb_file.jet_version, self.mdb.encoding, &column_name_buffer)?;
    }

    Ok(())
  }

  /// Load a table from a catalog entry.
  pub fn from_catalog_entry(entry: CatalogEntry, mdb: &Mdb) -> Result<Table, MdbError> {
    let mut mdb: Mdb = mdb.clone();
    let entry: TableCatalogEntry = match entry {
      CatalogEntry::Table(entry) => entry,
    };

    mdb.read_page(entry.page)?;

    if mdb.get_u8(0) != 2 {
      eprintln!("First byte is not equal to 2.");
      return Err(MdbError::InvalidTableDefinition);
    }

    let page_row = mdb.get_u32(mdb.format.tab_usage_map_offset);
    let real_index_count: u32 = mdb.get_u32(mdb.format.real_index_count_offset);
    let variable_column_count = mdb.get_u16(mdb.format.table_column_count_offset - 2);
    let column_count = mdb.get_u16(mdb.format.table_column_count_offset);

    // Temporary mdb for getting usage map.
    let mut usage_map_mdb = mdb.clone();

    let usage_map_row = mdb_find_page_row_packed(&mut usage_map_mdb, page_row)?;

    let usage_map_buffer = Vec::from(&usage_map_mdb.page_buffer[usage_map_row.start as usize..usage_map_row.start as usize + usage_map_row.length as usize]);
    let usage_map = UsageMap::from_raw(&mut usage_map_mdb, &usage_map_buffer)?;

    let first_data_page = mdb.get_u16(mdb.format.table_first_data_page_offset);

    let table = Table {
      name: entry.name,
      row_count: mdb.get_u32(mdb.format.row_count_offset),
      variable_column_count,
      column_count,
      first_data_page,
      columns: vec![],
      real_index_count,
      current_page_number: 0,
      current_physical_page_number: 0,
      current_row: 0,
      is_temporary_table: false,
      strategy: TableStrategy::TableScan,
      mdb,
      first_table_definition_page: entry.page,
      usage_map
    };

    Ok(table)
  }
}