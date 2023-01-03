use encoding_rs::Encoding;
use crate::error::MdbError;

use crate::mdbfile::{MdbFormatVersion};

pub fn decode_mdb_string(format: MdbFormatVersion, encoding: &'static Encoding, source: &[u8]) -> Result<String, MdbError> {
  let mut decoded_string: Vec<u8> = Vec::with_capacity(source.len() * 2);
  let result_string = if !(format == MdbFormatVersion::JET3) && (source.len() >= 2) && (source[0] == 0xff) && (source[1] == 0xfe) {
    decompress_unicode(&source[2..], &mut decoded_string);
    let (result_string, _encoding_used, had_errors) = encoding.decode(&decoded_string);

    if had_errors {
      return Err(MdbError::DecodeString);
    }

    result_string
  } else {
    let (result_string, _encoding_used, had_errors) = encoding.decode(source);

    if had_errors {
      return Err(MdbError::DecodeString);
    }

    result_string
  };

  Ok(result_string.to_string())
}

pub fn decompress_unicode(src: &[u8], res: &mut Vec<u8>) {
  let mut compress: bool = true;
  let mut cur: usize = 0;

  while cur < src.len() {
    if src[cur] == 0 {
      compress = !compress;
      cur += 1;
    } else if compress {
      res.push(src[cur]);
      res.push(0);
      cur += 1;
    } else if src.len() >= 2 {
      res.push(src[cur]);
      cur += 1;
      res.push(src[cur]);
      cur += 1;
    } else {
      // Odd # of bytes
      break;
    }
  }
}