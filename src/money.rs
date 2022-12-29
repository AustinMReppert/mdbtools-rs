pub fn money_column_to_string(buffer: &[u8]) -> String {
  let raw_data: [u8; 8] = buffer[0..8].try_into().expect("Invalid money data");
  let val: i64 = i64::from_le_bytes(raw_data);
  let whole = val / 10000;
  let fraction: i64 = i64::abs(val - whole * 10000);
  format!("{}.{}", whole, fraction)
}