struct Rc4 {
  state: [u8; 256],
  x: u16,
  y: u16,
}

impl Rc4 {

  fn setup_key(&mut self, key_data_ptr: &[u8]) {
    let mut index1: u16;
    let mut index2: u16;

    for counter in 0..256 {
      self.state[counter] = counter as u8;
    }

    self.x = 0;
    self.y = 0;
    index1 = 0;
    index2 = 0;
    for counter in 0..256 {
      index2 = (key_data_ptr[index1 as usize] as u16 + self.state[counter] as u16 + index2) % 256;
      self.state.swap(counter, index2 as usize);
      index1 = (index1 + 1) % (key_data_ptr.len() as u16);
    }
  }

  /// This algorithm does 'encrypt in place' instead of inbuff/outbuff.
  /// Note also: encryption and decryption use same routine.
  /// Implementation supplied by (Adam Back) at <adam@cypherspace.org>.
  fn encrypt(&mut self, buff: &mut [u8]) {
    let mut xor_index: u16;

    for byte in buff {
      self.x = (self.x + 1) % 256;
      self.y = (self.state[self.x as usize] as u16 + self.y) % 256;
      self.state.swap(self.x as usize, self.y as usize);
      xor_index = (self.state[self.x as usize] as u16 + self.state[self.y as usize] as u16) % 256;
      *byte ^= self.state[xor_index as usize];
    }
  }
}


pub fn create_key_and_encrypt(key_data: &mut [u8], buf: &mut [u8]) {
  let mut key = Rc4 {
    state: [0; 256],
    x: 0,
    y: 0,
  };
  key.setup_key(key_data);
  key.encrypt(buf);
}