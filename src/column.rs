use std::fmt::{Display, Formatter};

use chrono::{DateTime, Duration};

use encoding_rs::{Encoding};
use crate::backend::{Backend, BackendType};

use crate::conversion::decode_mdb_string;
use crate::data::{ColBuffer, mdb_find_page_row_packed};
use crate::mdbfile::{Mdb, MdbFormatVersion};
use crate::money::money_column_to_string;
use crate::numeric::numeric_column_to_string;
use crate::time::CDateTime;
use crate::utils::{get_u32};

pub struct Column {
  pub name: String,
  pub column_type: ColumnType,
  pub(crate) number: u8,
  pub(crate) row_column_number: u16,
  pub scale: u8,
  pub precision: u8,
  pub(crate) is_fixed: bool,
  pub(crate) is_long_auto: bool,
  pub(crate) is_uuid_auto: bool,
  pub is_hyperlink: bool,
  pub(crate) fixed_offset: u16,
  pub size: u16,
  pub(crate) var_col_num: u16,
  pub buffer: ColBuffer,
  format: MdbFormatVersion,
  encoding: &'static Encoding,

  // Used for memo
  column_text: Option<String>
}

impl Column {

  pub fn extract_column_text(&mut self, mdb: &Mdb) -> Result<(), ()> {
    let text = self.get_memo_string(mdb);
    if text.is_err() {
      return Err(());
    }

    self.column_text = Some(text.unwrap());

    Ok(())
  }

  pub fn new(encoding: &'static Encoding) -> Self {
    Column {
      name: "".to_string(),
      column_type: ColumnType::Bool,
      number: 0,
      row_column_number: 0,
      scale: 0,
      precision: 0,
      is_fixed: false,
      is_long_auto: false,
      is_uuid_auto: false,
      is_hyperlink: false,
      fixed_offset: 0,
      size: 0,
      var_col_num: 0,
      buffer: ColBuffer {
        value: Vec::with_capacity(200000),
        size: 0,
        start: 0,
        is_null: false,
        is_fixed: false,
        column_number: 0,
        offset: 0,
      },
      format: MdbFormatVersion::JET4,
      encoding,
      column_text: None,
    }
  }
}

#[repr(u8)]
#[derive(PartialEq, Copy, Clone)]
pub enum ColumnType {
  Bool = 0x01,
  Byte = 0x02,
  Int = 0x03,
  LongInt = 0x04,
  Money = 0x05,
  Float = 0x06,
  Double = 0x07,
  Datetime = 0x08,
  Binary = 0x09,
  Text = 0x0a,
  OLE = 0x0b,
  Memo = 0x0c,
  ReplicationId = 0x0f,
  Numeric = 0x10,
  Complex = 0x12,
  ExtendedDatetime = 0x14,
}

impl Display for ColumnType {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      ColumnType::Bool => write!(f, "bool"),
      ColumnType::Byte => write!(f, "byte"),
      ColumnType::Int => write!(f, "int"),
      ColumnType::LongInt => write!(f, "long int"),
      ColumnType::Money => write!(f, "money"),
      ColumnType::Float => write!(f, "float"),
      ColumnType::Double => write!(f, "double"),
      ColumnType::Datetime => write!(f, "datetime"),
      ColumnType::Binary => write!(f, "binary"),
      ColumnType::Text => write!(f, "text"),
      ColumnType::OLE => write!(f, "ole"),
      ColumnType::Memo => write!(f, "memo"),
      ColumnType::ReplicationId => write!(f, "replication id"),
      ColumnType::Numeric => write!(f, "numeric"),
      ColumnType::Complex => write!(f, "complex"),
      ColumnType::ExtendedDatetime => write!(f, "extended datetime"),
    }
  }
}

impl TryFrom<u8> for ColumnType {
  type Error = ();

  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      v if v == ColumnType::Bool as u8 => Ok(ColumnType::Bool),
      v if v == ColumnType::Byte as u8 => Ok(ColumnType::Byte),
      v if v == ColumnType::Int as u8 => Ok(ColumnType::Int),
      v if v == ColumnType::LongInt as u8 => Ok(ColumnType::LongInt),
      v if v == ColumnType::Money as u8 => Ok(ColumnType::Money),
      v if v == ColumnType::Float as u8 => Ok(ColumnType::Float),
      v if v == ColumnType::Double as u8 => Ok(ColumnType::Double),
      v if v == ColumnType::Datetime as u8 => Ok(ColumnType::Datetime),
      v if v == ColumnType::Binary as u8 => Ok(ColumnType::Binary),
      v if v == ColumnType::Text as u8 => Ok(ColumnType::Text),
      v if v == ColumnType::OLE as u8 => Ok(ColumnType::OLE),
      v if v == ColumnType::Memo as u8 => Ok(ColumnType::Memo),
      v if v == ColumnType::ReplicationId as u8 => Ok(ColumnType::ReplicationId),
      v if v == ColumnType::Numeric as u8 => Ok(ColumnType::Numeric),
      v if v == ColumnType::Complex as u8 => Ok(ColumnType::Complex),
      v if v == ColumnType::ExtendedDatetime as u8 => Ok(ColumnType::ExtendedDatetime),
      _ => Err(()),
    }
  }
}

impl Column {
  pub fn get_memo_string(&self, mdb: &Mdb) -> Result<String, ()> {
    let mut mdb = mdb.clone();

    const MEMO_OVERHEAD: usize = 12;
    if self.column_type != ColumnType::Memo {
      panic!("Calling print_memo on non-memo column.");
    }

    if self.buffer.is_null || self.buffer.value.len() == 0 {
      return Ok(String::new());
    }

    if !self.buffer.value.len() < 12 {
      return Ok(String::new());
    }

    let memo_length = get_u32(&self.buffer.value, 0) as usize;

    if memo_length & 0x80000000 != 0 {
      /* inline memo field */
      decode_mdb_string(mdb.mdb_file.jet_version, mdb.encoding, &self.buffer.value[MEMO_OVERHEAD..])
    } else if memo_length & 0x40000000 != 0 {
      /* single-page memo field */
      let page_row = get_u32(&self.buffer.value, 4);

      match mdb_find_page_row_packed(&mut mdb, page_row) {
        Ok(usage_map) => {
          decode_mdb_string(usage_map.mdb.mdb_file.jet_version, usage_map.mdb.encoding, &usage_map.mdb.page_buffer[(usage_map.start as usize)..(usage_map.start + usage_map.length) as usize])
        }
        Err(_) => {
          Err(())
        }
      }

    } else if (memo_length & 0xff000000) == 0 {

      let mut page_row = get_u32(&self.buffer.value, 4);
      let mut temp_offset = 0;
      let mut buffer: Vec<u8> = vec![0; memo_length];

      loop {
        let usage_map = match mdb_find_page_row_packed(&mut mdb, page_row) {
          Ok(res) => res,
          Err(_) => return Err(())
        };

        if temp_offset + usage_map.length as usize - 4 > memo_length {
          break;
        }

        /* Stop processing on zero length multiple page memo fields */
        if usage_map.length < 4 {
          break;
        }

        buffer[(temp_offset as usize)..((temp_offset + usage_map.length as usize - 4) as usize)].copy_from_slice(&usage_map.mdb.page_buffer[(usage_map.start as usize + 4)..(usage_map.start as usize + 4 + usage_map.length as usize - 4)]);
        temp_offset += usage_map.length as usize - 4;

        page_row = get_u32(&usage_map.mdb.page_buffer, usage_map.start as usize);
        if page_row == 0 {
          break;
        }
      }

      if temp_offset < memo_length {
        eprintln!("Warning: incorrect memo length");
      }

      decode_mdb_string(mdb.mdb_file.jet_version, mdb.encoding, &buffer[..temp_offset as usize])
    } else {
      Err(())
    }
  }

  pub fn get_backend_type<'a>(&self, backend: &Backend<'a>) -> BackendType<'a> {
    match self.column_type {
      ColumnType::Bool => backend.mdb_bool,
      ColumnType::Byte => backend.mdb_byte,
      ColumnType::Int => backend.mdb_int,
      ColumnType::LongInt => backend.mdb_longint,
      ColumnType::Money => backend.mdb_money,
      ColumnType::Float => backend.mdb_float,
      ColumnType::Double => backend.mdb_double,
      ColumnType::Datetime => backend.mdb_datetime,
      ColumnType::Binary => backend.mdb_binary,
      ColumnType::Text => backend.mdb_text,
      ColumnType::OLE => backend.mdb_ole,
      ColumnType::Memo => backend.mdb_memo,
      ColumnType::ReplicationId => backend.mdb_replication_id,
      ColumnType::Numeric => backend.mdb_numeric,
      ColumnType::Complex => backend.mdb_complex,
      ColumnType::ExtendedDatetime => backend.mdb_extended_datetime,
    }
  }

}

impl Display for Column {

  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

    if self.buffer.is_null {
      match self.column_type {
        ColumnType::Bool => (),
        _ => {
          return write!(f, "NULL");
        }
      }
    }

    match self.column_type {
      ColumnType::Int => {
        let raw_data: [u8; 2] = self.buffer.value[0..2].try_into().unwrap();
        write!(f, "{}", i16::from_le_bytes(raw_data))
      },
      ColumnType::LongInt => {
        let raw_data: [u8; 4] = self.buffer.value[0..4].try_into().unwrap();
        write!(f, "{}", i32::from_le_bytes(raw_data))
      },
      ColumnType::Float => {
        let raw_data: [u8; 4] = self.buffer.value[0..4].try_into().unwrap();
        write!(f, "{}", f32::from_le_bytes(raw_data))
      },
      ColumnType::Double => {
        let raw_data: [u8; 8] = self.buffer.value[0..8].try_into().unwrap();
        write!(f, "{}", f64::from_le_bytes(raw_data))
      },
      ColumnType::Text => {
        if self.buffer.is_null {
          return write!(f, "");
        }
        write!(f, "{}", decode_mdb_string(self.format, self.encoding, &self.buffer.value).expect("Failed to convert"))
      },
      ColumnType::Memo => {
        write!(f, "{}", self.column_text.as_ref().unwrap())
      }
      ColumnType::ExtendedDatetime => {
        let mut days: i64 = 0;
        for (i, x) in self.buffer.value[12..19].iter().enumerate() {
          days += i64::pow(10, (6 - i) as u32) * (x - 48) as i64;
        }

        let mut nanoseconds: i64 = 0;
        for (i, x) in self.buffer.value[32..39].iter().enumerate() {
          nanoseconds += i64::pow(10, (6 - i) as u32) * (x - 48) as i64;
        }
        nanoseconds *= 100;

        let mut seconds: i64 = 0;
        for (i, x) in self.buffer.value[27..32].iter().enumerate() {
          seconds += i64::pow(10, (4 - i) as u32) * (x - 48) as i64;
        }

        //print!("{:#02}:{:#02}:{:#02}.{} ==== ", (seconds / 3600) % 24, (seconds / 60) % 60, seconds % 60, nanoseconds);
        let start_datetime = DateTime::parse_from_rfc3339("0001-01-01T00:00:00+00:00").expect("failed");
        let datetime = start_datetime + Duration::days(days) + Duration::seconds(seconds) + Duration::nanoseconds(nanoseconds);
        write!(f, "{}", datetime.to_rfc3339())
      },
      ColumnType::Datetime => {
        let raw_data: [u8; 8] = self.buffer.value[0..8].try_into().expect("");
        let raw_time = f64::from_le_bytes(raw_data);
        let dt = CDateTime::from_f64(raw_time);
        write!(f, "{:0>2}/{:0>2}/{} {:0>2}:{:0>2}:{:0>2}", dt.month + 1, dt.month_day, 1900 + dt.year, dt.hour, dt.minute, dt.second)
      },
      ColumnType::ReplicationId => {
        write!(f, "{{{:X}{:X}{:X}{:X}-{:X}{:X}-{:X}{:X}-{:X}{:X}-{:X}{:X}{:X}{:X}{:X}{:X}}}",
               self.buffer.value[3], self.buffer.value[2], self.buffer.value[1], self.buffer.value[0],
               self.buffer.value[5], self.buffer.value[4],
               self.buffer.value[7], self.buffer.value[6],
               self.buffer.value[8], self.buffer.value[9],
               self.buffer.value[10], self.buffer.value[11], self.buffer.value[12], self.buffer.value[13], self.buffer.value[14], self.buffer.value[15]
        )
      },
      ColumnType::Numeric => {
        write!(f, "{}", numeric_column_to_string(&self.buffer.value, self.precision))
      },
      ColumnType::Money => {
        write!(f, "{}", money_column_to_string(&self.buffer.value))
      },
      ColumnType::Bool => {
        write!(f, "{}", self.buffer.is_null)
      },
      _ => {
        write!(f, "todo")
      }
    }
  }
}