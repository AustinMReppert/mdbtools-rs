use crate::mdbfile::{MdbFormatVersion};
use crate::mdbfile::MdbFormatVersion::JET3;
use crate::table::Table;

pub fn crack_row(table: &mut Table, row_start: u16, row_size: u16) -> Result<(), ()> {
  //println!("=========================================");
  let row_start: usize = row_start as usize;
  let row_size: usize = row_size as usize;
  let row_end = row_start + row_size - 1;

  //println!("{}", String::from_utf8_lossy(&table.mdb.page_buffer[row_start..row_start + row_size]));

  let row_cols: usize;
  let col_count_size: usize;
  if table.mdb.mdb_file.jet_version == MdbFormatVersion::JET3 {
    row_cols = table.mdb.get_u8(row_start) as usize;
    col_count_size = 1;
  } else {
    row_cols = table.mdb.get_u16(row_start) as usize;
    col_count_size = 2;
  }

  let bitmask_size: usize = (row_cols + 7) / 8;
  if bitmask_size + if table.mdb.mdb_file.jet_version == JET3 { 0 } else { 1 } >= row_end {
    eprintln!("warning: Invalid page buffer detected in mdb_crack_row.");
    return Err(());
  }

  let nullmask = &table.mdb.page_buffer[row_end - bitmask_size + 1..];

  let mut row_var_cols: u16 = 0;
  let mut var_col_offsets: Vec<u32> = Vec::new();
  if table.variable_column_count > 0 {
    row_var_cols = if table.mdb.mdb_file.jet_version == MdbFormatVersion::JET3 { table.mdb.get_u8(row_end - bitmask_size) as u16 } else { table.mdb.get_u16(row_end - bitmask_size - 1) };
    var_col_offsets.resize((row_var_cols + 1) as usize, 0);

    let success: Result<(), ()> = if table.mdb.mdb_file.jet_version == MdbFormatVersion::JET3 {
      crack_jet_3_row(table, row_start, row_end, bitmask_size, row_var_cols as usize, var_col_offsets.as_mut_slice())
    } else {
      crack_jet_4_row(table, row_end, bitmask_size, row_var_cols as usize, var_col_offsets.as_mut_slice())
    };
    if success.is_err() {
      eprintln!("warning: Invalid page buffer detected in mdb_crack_row.");
      return Err(());
    }

  }

  let row_fixed_cols = row_cols as u16 - row_var_cols;

  /*println!("bitmask_sz {}", bitmask_size);
  println!("row_var_cols {}", row_var_cols);
  println!("row_fixed_cols {}", row_fixed_cols);*/

  let mut fixed_columns_found = 0;
  for col in &mut table.columns {
    //println!("");
    /*fields[i].column_number = i as u16;*/
    /*fields[i].is_fixed = col.is_fixed;*/
    let byte_num: usize = (col.number / 8) as usize;
    let bit_num: usize = (col.number % 8) as usize;
/*    println!("byte_num: {}", byte_num);
    println!("bit_num: {}", bit_num);
    println!("fixed: {}", col.is_fixed);
    println!("col: {}", col.number);
    println!("col: {}", col.name);*/


    col.buffer.is_null = !(byte_num < nullmask.len() && nullmask[byte_num] & (1 << bit_num) != 0);
    //TODO: fix below line
    if col.is_fixed && fixed_columns_found < row_fixed_cols {
      let col_start = col.fixed_offset as usize + col_count_size;
      //println!("col_start: {}", col_start);
      //println!("fixed_offset: {}", col.fixed_offset);
      col.buffer.start = row_start + col_start;
      col.buffer.value.resize(col.size as usize, 0);
      col.buffer.value.copy_from_slice(&table.mdb.page_buffer[row_start + col_start..(row_start + col_start + col.size as usize)]);
      col.buffer.size = col.size;
      fixed_columns_found += 1;
    } else if !col.is_fixed && col.var_col_num < row_var_cols {
      let col_start: usize = var_col_offsets[col.var_col_num as usize] as usize;
      col.buffer.start = row_start + col_start;
      let size: usize = (var_col_offsets[(col.var_col_num as usize)+1] as usize).overflowing_sub(col_start).0;
      col.buffer.value.resize(size, 0);
      col.buffer.value.copy_from_slice(&table.mdb.page_buffer[(row_start + col_start)..(row_start + col_start + size)]);
      col.buffer.size = size as u16;
    } else {
      col.buffer.value.clear();
      col.buffer.start = 0;
      col.buffer.size = 0;
      col.buffer.is_null = true;
    }
    if col.buffer.start + col.buffer.size as usize > row_start + row_size {
      eprintln!("warning: Invalid data location detected in mdb_crack_row. Table: {} Column: {}", table.name, col.name);
      return Err(());
    }
  }
  //println!("=========================================");
  Ok(())
}

fn crack_jet_3_row(table: &Table, row_start: usize, row_end: usize, bitmask_size: usize, row_var_cols: usize, offsets: &mut [u32]) -> Result<(), ()> {
  let row_len: usize = row_end - row_start + 1;
  let mut num_jumps: usize = (row_len - 1) / 256;
  let col_ptr = row_end - bitmask_size - num_jumps - 1;
  if (col_ptr - row_start - row_var_cols) / 256 < num_jumps {
    num_jumps -= 1;
  }

  if bitmask_size + num_jumps + 1 > row_end {
    return Err(());
  }

  if col_ptr >= table.mdb.format.page_size || col_ptr < row_var_cols {
    return Err(());
  }

  let mut jumps_used = 0;
  for i in 0..(row_var_cols + 1) {
    while jumps_used < num_jumps && i == table.mdb.page_buffer[row_end - bitmask_size - 1] as usize {
      jumps_used += 1;
    }
    offsets[i] = table.mdb.page_buffer[col_ptr - i] as u32 + (jumps_used as u32 * 256);
  }

  Ok(())
}

fn crack_jet_4_row(table: &Table, row_end: usize, bitmask_size: usize, row_var_cols: usize, offsets: &mut [u32]) -> Result<(), ()> {

  if bitmask_size + 3 + row_var_cols * 2 + 2 > row_end {
    return Err(());
  }

  for i in 0..(row_var_cols + 1) {
    offsets[i] = table.mdb.get_u16(row_end - bitmask_size - 3 - (i * 2)) as u32;
  }

  Ok(())
}