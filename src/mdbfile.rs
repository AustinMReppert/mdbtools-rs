use std::io::{Read, Seek, SeekFrom};
use std::fmt::Formatter;
use std::fs::File;
use std::path::PathBuf;

use encoding_rs::{Encoding, UTF_16LE, WINDOWS_1252};

use crate::{rc4, utils};
use crate::error::MdbError;

const MDB_PAGE_SIZE: usize = 4096;

#[derive(Clone)]
pub struct Mdb {
  pub current_page: u32,
  current_position: u16,
  pub mdb_file: MdbFile,
  pub(crate) page_buffer: [u8; MDB_PAGE_SIZE],
  pub(crate) format: &'static MdbFormatConstants,
  pub(crate) codepage: u16,
  pub encoding: &'static Encoding,
}

impl Mdb {
  pub fn open(path: PathBuf) -> Result<Mdb, MdbError> {
    let file = match File::open(path) {
      Ok(f) => f,
      //Err(_e) => return Err("Could not open database.")
      Err(_e) => return Err(MdbError::ReadPage)
    };

    let mut mdb = Mdb {
      current_page: 0,
      current_position: 0,
      mdb_file: MdbFile {
        file,
        jet_version: MdbFormatVersion::JET3,
        db_key: 0,
        database_password: [0; 14],
        language_id: 0,
      },
      /* need something to bootstrap with, reassign after page 0 is read */
      page_buffer: [0; MDB_PAGE_SIZE],
      format: &MDB_JET3_CONSTANTS,
      codepage: 0,
      encoding: UTF_16LE,
    };
    match mdb.read_page(0) {
      Ok(_) => {}
      Err(e) => {
        return Err(e);
      }
    }
    if mdb.page_buffer[0] != 0 {
      //return Err("Couldn't open database. File is corrupt or not an database.");
      return Err(MdbError::ReadPage)
    }

    let raw_version: u32 = mdb.page_buffer[0x14] as u32;
    mdb.mdb_file.jet_version = match raw_version.try_into() {
      Ok(mdb_version) => mdb_version,
      Err(_) => {
        eprintln!("Unknown Jet version: {}", raw_version);
        return Err(MdbError::JetVersion);
      }
    };
    mdb.format = mdb.mdb_file.jet_version.get_format_constants();

    let mut tmp_key: [u8; 4] = [0xC7, 0xDA, 0x39, 0x6B];
    rc4::create_key_and_encrypt(&mut tmp_key, &mut mdb.page_buffer[0x18..(if mdb.mdb_file.jet_version == MdbFormatVersion::JET3 { 126 } else { 128 })]);

    mdb.mdb_file.db_key = mdb.get_u32(0x3e);

    mdb.codepage = mdb.get_u16(0x3c);
    if mdb.mdb_file.jet_version != MdbFormatVersion::JET3 {
      // UCS-2LE?
      mdb.encoding = UTF_16LE;
    } else {
      mdb.encoding = WINDOWS_1252;
    }

    //println!("JET VERSION: {}", mdb.mdb_file.jet_version.to_string());
    Ok(mdb)
  }

  pub fn read_page(&mut self, page: u32) -> Result<(), MdbError> {
    if page != 0 && self.current_page == page {
      return Ok(());
    }

    let res = self._mdb_read_page(page);

    self.current_page = page;
    self.current_position = 0;

    res
  }

  pub fn get_u32(&self, offset: usize) -> u32 {
    utils::get_u32(&self.page_buffer, offset)
  }

  pub fn get_u16(&self, offset: usize) -> u16 {
    utils::get_u16(&self.page_buffer, offset)
  }

  pub fn get_u8(&self, offset: usize) -> u8 {
    self.page_buffer[offset]
  }

  pub fn _mdb_read_page(&mut self, page: u32) -> Result<(), MdbError> {
    let page_buffer = &mut self.page_buffer;
    let offset: u64 = page as u64 * self.format.page_size as u64;

    let seek_end = self.mdb_file.file.seek(SeekFrom::End(0));
    if seek_end.is_err() {
      //return Err("Unable to seek to end of file");
      return Err(MdbError::ReadPage);
    }

    if seek_end.unwrap() < offset {
      //return Err("Offset is beyond EOF");
      return Err(MdbError::ReadPage);
    }

    let seek_page = self.mdb_file.file.seek(SeekFrom::Start(offset));
    if seek_page.is_err() {
      //return Err("Failed to seek to page");
      return Err(MdbError::ReadPage);
    }

    //let page_buffer = &mut self.page_buffer;
    let res = self.mdb_file.file.read(page_buffer);
    if res.is_err() {
      //return Err("Failed to read page");
      return Err(MdbError::ReadPage);
    }

    let length = res.unwrap();

    // If the number of bytes read is less than a page size, zero the rest.
    page_buffer[length..].fill(0);

    /*
     * un-encrypt the page if necessary.
     * it might make sense to cache the unencrypted data blocks?
     */
    if page != 0 && self.mdb_file.db_key != 0 {
      let tmp_key_i: u32 = self.mdb_file.db_key ^ page;
      let mut tmp_key: [u8; 4] = [
        (tmp_key_i & 0xFF) as u8, ((tmp_key_i >> 8) & 0xFF) as u8,
        ((tmp_key_i >> 16) & 0xFF) as u8, ((tmp_key_i >> 24) & 0xFF) as u8
      ];
      rc4::create_key_and_encrypt(&mut tmp_key, &mut self.page_buffer);
    }

    Ok(())
  }

  /// Read data into a buffer, advancing pages and setting the
  /// page cursor as needed.  In the case that buf in NULL, pages
  /// are still advanced and the page cursor is still updated.
  pub fn read_page_if_n(&mut self, mut buffer_option: Option<&mut [u8]>, cur_pos: &mut u16, len: u16) -> Result<(), MdbError> {
    let mut len: usize = len as usize;
    let end = len;
    let mut buffer_offset: usize = 0;

    while *cur_pos >= self.format.page_size as u16 {
      self.read_page(self.get_u32(4))?;
      *cur_pos -= self.format.page_size as u16 - 8;
    }

    while *cur_pos as usize + len >= self.format.page_size {
      let piece_len = self.format.page_size - *cur_pos as usize;
      println!("piece_len: {}", piece_len);
      if buffer_option.as_ref().is_some() {
        let buffer = buffer_option.as_mut().unwrap();
        if buffer_offset + piece_len > end {
          eprintln!("Buffer overflowed.");
          return Err(MdbError::PageBufferOverflow);
        }

        buffer[buffer_offset..].copy_from_slice(&self.page_buffer.as_slice()[(*cur_pos as usize)..((*cur_pos as usize + piece_len) as usize)]);
        buffer_offset += piece_len;
      }
      len -= piece_len;
      self.read_page(self.get_u32(4))?;
      *cur_pos = 8;
    }

    if len > 0 && buffer_option.as_ref().is_some() {
      let buffer = buffer_option.as_mut().unwrap();
      if buffer_offset + len > end {
        eprintln!("Buffer overflowed.");
        return Err(MdbError::PageBufferOverflow);
      }

      buffer[buffer_offset..(buffer_offset + len)].copy_from_slice(&self.page_buffer.as_slice()[(*cur_pos as usize)..(*cur_pos as usize + len)])
    }
    *cur_pos += len as u16;
    Ok(())
  }

  pub(crate) fn read_page_if_8(&mut self, cur_position: &mut u16) -> Result<u8, MdbError> {
    let mut c: [u8; 1] = [0; 1];
    self.read_page_if_n(Some(&mut c), cur_position, 1)?;

    Ok(c[0])
  }

  pub(crate) fn read_page_if_16(&mut self, cur_position: &mut u16) -> Result<u16, MdbError> {
    let mut c: [u8; 2] = [0; 2];
    self.read_page_if_n(Some(&mut c), cur_position, 2)?;
    Ok((c[0] as u16) + ((c[1] as u16) << 8))
  }
}

pub fn mdb_get_int16(buf: &[u8], offset: usize) -> i16 {
  return *buf.get(offset).unwrap() as i16 + ((*buf.get(offset + 1).unwrap() as i16) << 8);
}

pub fn mdb_get_int32(buf: &[u8], offset: usize) -> i32 {
  return *buf.get(offset).unwrap() as i32 + ((*buf.get(offset).unwrap() as i32) << 8) + ((*buf.get(offset + 1).unwrap() as i32) << 16) + ((*buf.get(offset).unwrap() as i32) << 24);
}

#[allow(dead_code)]
pub struct MdbFormatConstants {
  pub(crate) page_size: usize,
  pub usage_row_count_offset: usize,
  pub(crate) row_count_offset: usize,
  pub table_column_count_offset: usize,
  pub(crate) tab_num_idxs_offset: u16,
  pub real_index_count_offset: usize,
  pub tab_usage_map_offset: usize,
  pub table_first_data_page_offset: usize,
  pub(crate) tab_cols_start_offset: usize,
  pub(crate) tab_ridx_entry_size: u16,
  pub(crate) col_flags_offset: usize,
  pub(crate) column_size_offset: usize,
  pub(crate) column_number_offset: usize,
  pub(crate) tab_col_entry_size: u16,
  pub(crate) tab_free_map_offset: u16,
  pub(crate) tab_col_offset_var: usize,
  pub(crate) table_column_offset_fixed: usize,
  pub(crate) table_row_column_number_offset: usize,
  pub(crate) column_scale_offset: usize,
  pub(crate) column_precision_offset: usize,
}

const MDB_JET3_CONSTANTS: MdbFormatConstants = MdbFormatConstants {
  page_size: 2048,
  row_count_offset: 12,
  usage_row_count_offset: 0x08,
  table_column_count_offset: 25,
  tab_num_idxs_offset: 27,
  real_index_count_offset: 31,
  tab_usage_map_offset: 35,
  table_first_data_page_offset: 36,
  tab_cols_start_offset: 43,
  tab_ridx_entry_size: 8,
  column_scale_offset: 9,
  column_precision_offset: 10,
  col_flags_offset: 13,
  column_size_offset: 16,
  column_number_offset: 1,
  tab_col_entry_size: 18,
  tab_free_map_offset: 39,
  tab_col_offset_var: 3,
  table_column_offset_fixed: 14,
  table_row_column_number_offset: 5,
};

const MDB_JET4_CONSTANTS: MdbFormatConstants = MdbFormatConstants {
  page_size: 4096,
  usage_row_count_offset: 0x0c,
  row_count_offset: 16,
  table_column_count_offset: 45,
  tab_num_idxs_offset: 47,
  real_index_count_offset: 51,
  tab_usage_map_offset: 55,
  table_first_data_page_offset: 56,
  tab_cols_start_offset: 63,
  tab_ridx_entry_size: 12,
  column_scale_offset: 11,
  column_precision_offset: 12,
  col_flags_offset: 15,
  column_size_offset: 23,
  column_number_offset: 5,
  tab_col_entry_size: 25,
  tab_free_map_offset: 59,
  tab_col_offset_var: 7,
  table_column_offset_fixed: 21,
  table_row_column_number_offset: 9,
};

pub struct MdbFile {
  pub file: File,
  pub jet_version: MdbFormatVersion,
  pub db_key: u32,
  database_password: [u8; 14],
  language_id: u16,
}

impl Clone for MdbFile {
  fn clone(&self) -> Self {
    MdbFile {
      file: self.file.try_clone().expect("Failed while cloning mdb."),
      jet_version: self.jet_version,
      db_key: self.db_key,
      database_password: self.database_password,
      language_id: self.language_id,
    }
  }
}

#[repr(u32)]
#[derive(PartialEq, Copy, Clone)]
pub enum MdbFormatVersion {
  JET3 = 0,
  JET4 = 0x01,
  Accdb2007 = 0x02,
  Accdb2010 = 0x03,
  Accdb2013 = 0x04,
  Accdb2016 = 0x05,
  Accdb2019 = 0x06,
}

impl std::fmt::Display for MdbFormatVersion {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", match self {
      MdbFormatVersion::JET3 => "JET3",
      MdbFormatVersion::JET4 => "JET4",
      MdbFormatVersion::Accdb2007 => "ACE12",
      MdbFormatVersion::Accdb2010 => "ACE14",
      MdbFormatVersion::Accdb2013 => "ACE13",
      MdbFormatVersion::Accdb2016 => "ACE16",
      MdbFormatVersion::Accdb2019 => "ACE17"
    })
  }
}

impl TryInto<MdbFormatVersion> for u32 {
  type Error = &'static str;

  fn try_into(self) -> Result<MdbFormatVersion, Self::Error> {
    const JET3_VAL: u32 = MdbFormatVersion::JET3 as u32;
    const JET4_VAL: u32 = MdbFormatVersion::JET4 as u32;
    const ACCDB2007_VAL: u32 = MdbFormatVersion::Accdb2007 as u32;
    const ACCDB2010_VAL: u32 = MdbFormatVersion::Accdb2010 as u32;
    const ACCDB2013_VAL: u32 = MdbFormatVersion::Accdb2013 as u32;
    const ACCDB2016_VAL: u32 = MdbFormatVersion::Accdb2016 as u32;
    const ACCDB2019_VAL: u32 = MdbFormatVersion::Accdb2019 as u32;
    match self {
      JET3_VAL => Ok(MdbFormatVersion::JET3),
      JET4_VAL => Ok(MdbFormatVersion::JET4),
      ACCDB2007_VAL => Ok(MdbFormatVersion::Accdb2007),
      ACCDB2010_VAL => Ok(MdbFormatVersion::Accdb2010),
      ACCDB2013_VAL => Ok(MdbFormatVersion::Accdb2013),
      ACCDB2016_VAL => Ok(MdbFormatVersion::Accdb2016),
      ACCDB2019_VAL => Ok(MdbFormatVersion::Accdb2019),
      _ => Err("Invalid or unknown jet MDB version.")
    }
  }
}

impl MdbFormatVersion {
  fn get_format_constants(&self) -> &'static MdbFormatConstants {
    match self {
      MdbFormatVersion::JET3 => &MDB_JET3_CONSTANTS,
      MdbFormatVersion::JET4 | MdbFormatVersion::Accdb2007 | MdbFormatVersion::Accdb2010 | MdbFormatVersion::Accdb2013 | MdbFormatVersion::Accdb2016 | MdbFormatVersion::Accdb2019 => &MDB_JET4_CONSTANTS
    }
  }
}

#[repr(u8)]
pub enum PageTypes {
  PageDb = 0,
  PageData,
  PageTable,
  PageIndex,
  PageLeaf,
  PageMap,
}
