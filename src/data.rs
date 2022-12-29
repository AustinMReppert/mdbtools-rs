use crate::column::ColumnType;
use crate::mdbfile::{Mdb, PageTypes};
use crate::table::{Table, TableStrategy};
use crate::write::crack_row;
use crate::map::UsageMap;

const OFFSET_MASK: u16 = 0x1fff;

impl Table {

  /// Get the next row. If an error occurs, no more rows should be read.
  pub fn fetch_row(&mut self) -> Result<(), ()> {
    if self.current_page_number == 0 {
      self.current_page_number = 1;
      self.current_row = 0;
      if (!self.is_temporary_table) && (self.strategy != TableStrategy::IndexScan) && self.read_next_data_page().is_err() {
        return Err(());
      }
    }

    loop {
      if self.is_temporary_table {
        // TODO: Implement.
      } else if self.strategy == TableStrategy::IndexScan {} else {
        let rows = self.mdb.get_u16(self.mdb.format.usage_row_count_offset);

        if self.current_row >= rows {
          self.current_row = 0;

          if self.read_next_data_page().is_err() {
            return Err(());
          }
        }
      }

      let res = self.read_row(self.current_row);
      self.current_row += 1;

      if res.is_err() {
        //eprintln!("Error reading row");
      } else {
        for col in &mut self.columns {
          if col.column_type == ColumnType::Memo {
            let res = col.extract_column_text(&self.mdb);
            if res.is_err() {
              eprintln!("Problem getting memo text.");
            }
          }
        }

        return res;
      }

      if res.is_ok() {
        break;
      }
    }

    Err(())
  }

  /// Attempts to read the next data page of a table.
  /// An error may indicate an actual error or simply there is no next data page.
  pub fn read_next_data_page(&mut self) -> Result<(), ()> {
    // TODO: Add fast approach.
    loop {
      let res = self.mdb.read_page(self.current_page_number as u32);
      self.current_page_number += 1;
      if res.is_err() {
        return Err(());
      }

      if self.mdb.page_buffer[0] == PageTypes::PageData as u8 && self.mdb.get_u32(4) == self.first_table_definition_page {
        break;
      }
    }

    Ok(())
  }

  pub fn read_row(&mut self, row: u16) -> Result<(), ()> {
    if self.column_count == 0 || self.columns.is_empty() {
      return Err(());
    }

    let mut row = match mdb_find_row(&mut self.mdb, row) {
      /* Emitting a warning here isn't especially helpful. The row metadata
       * could be bogus for a number of reasons, so just skip to the next one
       * without comment. */
      Ok(row) => {
        if row.length != 0 {
          row
        } else {
          return Err(());
        }
      }
      Err(_) => {
        return Err(());
      }
    };

    let mut deleted_flag = 0;
    if row.start & 0x4000 != 0 {
      deleted_flag += 1;
    }

    let mut lookup_flag = 0;
    if row.start & 0x8000 != 0 {
      lookup_flag += 1;
    }
    row.start &= OFFSET_MASK; /* remove flags */

    if deleted_flag != 0 {
      return Err(());
    }

    match crack_row(self, row.start, row.length) {
      Ok(_) => {
        return Ok(());
      }
      Err(_) => {
        eprintln!("Error reading row.")
      }
    }

    Err(())
  }

}

pub fn mdb_find_page_row_packed(mdb: &mut Mdb, page_row: u32) -> Result<UsageMap, ()> {
  // The row is stored in the bottom byte.
  let row: u8 = (page_row & 0x000000FF) as u8;
  // The page is stored in the top 3 bytes.
  let page = (page_row & 0xFFFFFF00) >> 8;

  mdb_find_page_row(mdb, row, page as u32)
}

pub fn mdb_find_page_row(mdb: &mut Mdb, row: u8, page: u32) -> Result<UsageMap, ()> {
  let mut mdb = mdb.clone();
  if mdb.read_page(page).is_err() {
    return Err(());
  }

  let res = mdb_find_row(&mut mdb, row as u16)?;

  Ok(UsageMap {
    mdb,
    start: res.start,
    length: res.length,
  })
}

pub struct Row {
  start: u16,
  length: u16,
}

/// Find a row assuming mdb has loaded the given page.
pub fn mdb_find_row(mdb: &mut Mdb, row: u16) -> Result<Row, ()> {
  if row > 1000 {
    return Err(());
  }

  let offset = mdb.format.usage_row_count_offset + 2 + (row as usize) * 2;
  let start: u16 = mdb.get_u16(offset);
  let next_start: u16 = if row == 0 { mdb.format.page_size as u16 } else { mdb.get_u16(mdb.format.usage_row_count_offset + (row as usize) * 2) & OFFSET_MASK };
  let length = next_start - (start & OFFSET_MASK);

  if (start & OFFSET_MASK) >= mdb.format.page_size as u16 || (start & OFFSET_MASK) > next_start || next_start > mdb.format.page_size as u16 {
    //eprintln!("Invalid bounds for usage map.");
    return Err(());
  }

  Ok(Row {
    start,
    length,
  })
}

#[derive(Clone)]
pub struct ColBuffer {
  pub value: Vec<u8>,
  pub size: u16,
  pub start: usize,
  pub is_null: bool,
  pub is_fixed: bool,
  pub column_number: u16,
  pub offset: usize,
}