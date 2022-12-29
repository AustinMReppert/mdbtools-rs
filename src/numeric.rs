const MAX_NUMERIC_PRECISION: usize = 40;

pub fn numeric_column_to_string(buffer: &[u8], precision: u8) -> String {
  if buffer.len() != 17 {
    return String::new();
  }

  let raw_data: [u8; 16] = buffer[1..17].try_into().expect("Invalid money data");
  let negative: bool = buffer[0] & 0x80 != 0;

  let mut multiplier: [u8; MAX_NUMERIC_PRECISION] = [0; MAX_NUMERIC_PRECISION];
  multiplier[0] = 1;

  let mut temp: [u8; MAX_NUMERIC_PRECISION] = [0; MAX_NUMERIC_PRECISION];
  let mut product: [u8; MAX_NUMERIC_PRECISION] = [0; MAX_NUMERIC_PRECISION];

  for i in 0..(raw_data.len()) {
    /* product += multiplier * current byte */
    multiply_byte(&mut product, raw_data[12 - 4 * (i / 4) + i % 4] as i32, &mut multiplier);

    temp.copy_from_slice(&multiplier);
    multiplier.fill(0);

    /* multiplier = multiplier * 256 */
    multiply_byte(&mut multiplier, 256, &mut temp);
  }

  array_to_string(&product, precision as usize, negative)
}

fn multiply_byte(product: &mut [u8], num: i32, multiplier: &mut [u8]) {
  let number: [u8; 3] = [(num % 10) as u8, ((num / 10) % 10) as u8, ((num / 100) % 10) as u8];
  for i in 0..multiplier.len() {
    if multiplier[i] == 0 {
      continue;
    }

    let mut j = 0;
    while j < 3 && i + j < multiplier.len() {
      if number[j] == 0 {
        j += 1;
        continue;
      }
      product[i + j] += multiplier[i] * number[j];
      j += 1;
    }
    do_carry(product, multiplier.len());
  }
}

fn do_carry(product: &mut [u8], length: usize) {
  for j in 0..(length - 1) {
    if product[j] > 9 {
      product[j + 1] += product[j] / 10;
      product[j] %= 10;
    }
  }
  if product[length - 1] > 9 {
    product[length - 1] %= 10;
  }
}

fn array_to_string(array: &[u8], scale: usize, negative: bool) -> String {
  let mut res: String = String::with_capacity(array.len() + 3);
  let mut top: usize = array.len();

  while top > 0 && top - 1 > scale && array[top - 1] == 0 {
    top -= 1;
  }

  if negative {
    res.push('-');
  }

  if top == 0 {
    res.push('0');
  } else {
    for i in (1..=top).rev() {
      if i == scale {
        res.push('.');
      }
      res.push((array[i - 1] + 48) as char);
    }
  }

  res
}