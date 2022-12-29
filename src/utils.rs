pub fn get_u64(buf: &[u8], offset: usize) -> u64 {
  (buf[offset] as u64) +
    ((buf[offset + 1] as u64) << 8) +
    ((buf[offset + 2] as u64) << 16) +
    ((buf[offset + 3] as u64) << 24) +
    ((buf[offset + 4] as u64) << 32) +
    ((buf[offset + 5] as u64) << 40) +
    ((buf[offset + 6] as u64) << 48) +
    ((buf[offset + 7] as u64) << 56)
}

pub fn get_u32(buf: &[u8], offset: usize) -> u32 {
  (buf[offset] as u32) + ((buf[offset + 1] as u32) << 8) + ((buf[offset + 2] as u32) << 16) + ((buf[offset + 3] as u32) << 24)
}

pub fn get_u16(buf: &[u8], offset: usize) -> u16 {
  (buf[offset] as u16) + ((buf[offset + 1] as u16) << 8)
}