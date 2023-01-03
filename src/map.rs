use bitvec::vec::BitVec;

use crate::error::MdbError;
use crate::mdbfile::{Mdb};
use crate::utils::get_u32;

// A bunch of false bits to read from.
const FALSE_BITS: [u8; 4092] = [0; 4092];

pub struct UsageMap {
  pub start_page: u32,
  pub pages: BitVec<u8>,
}

impl UsageMap {
  /// Loads a usage map from the raw mdb bytes.
  pub fn from_raw(mdb: &mut Mdb, buffer: &[u8]) -> Result<(UsageMap), MdbError> {

    if buffer.is_empty() {
      return Err(MdbError::UsageMapInvalidSize);
    }

      match buffer[0] {
      0 => {
        let start_page = get_u32(buffer, 1);
        let pages = BitVec::from_slice(&buffer[5..]);
        Ok(UsageMap {
          start_page,
          pages,
        })
      }
      1 => {
        let mut pages: BitVec<u8> = BitVec::with_capacity((mdb.format.page_size - 4) * 8);
        let bitmap_size = mdb.format.page_size - 4;
        for page_entry_offset in 0..((buffer.len() - 1) / 4) {
          let page = get_u32(buffer, 1 + page_entry_offset * 4);
          if page == 0 {
            pages.extend_from_raw_slice(&FALSE_BITS[0..bitmap_size])
          } else {
            mdb.read_page(page)?;
            pages.extend_from_raw_slice(&mdb.page_buffer[4..4 + bitmap_size])
          }
        }
        Ok(UsageMap {
          start_page: 0,
          pages
        })
      }
      5 => {
        Err(MdbError::StartedFromPartition)
      }
      _ => {
        Err(MdbError::UnknownMapType)
      }
    }
  }

  pub fn get_next_free_page(&self, current_page: u32) -> Result<u32, MdbError> {
    let start = current_page as usize - self.start_page as usize + 1;
    for (index, bit) in self.pages[start..].iter().enumerate() {
      if bit == true {
        return Ok((start + index) as u32 + self.start_page);
      }
    }

    Err(MdbError::NoFreePages)
  }
}