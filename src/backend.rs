#[derive(Copy, Clone)]
pub struct Backend<'a> {
  pub name: &'a str,
  pub mdb_bool: BackendType<'a>,
  pub mdb_byte: BackendType<'a>,
  pub mdb_int: BackendType<'a>,
  pub mdb_longint: BackendType<'a>,
  pub mdb_money: BackendType<'a>,
  pub mdb_float: BackendType<'a>,
  pub mdb_double: BackendType<'a>,
  pub mdb_datetime: BackendType<'a>,
  pub mdb_binary: BackendType<'a>,
  pub mdb_text: BackendType<'a>,
  pub mdb_ole: BackendType<'a>,
  pub mdb_memo: BackendType<'a>,
  pub mdb_replication_id: BackendType<'a>,
  pub mdb_numeric: BackendType<'a>,
  pub mdb_extended_datetime: BackendType<'a>,
  pub mdb_complex: BackendType<'a>,

  pub quote_name: fn(name: &str) -> String,
  pub default_quote_str: &'a str,
  pub default_null_str: &'a str,

  pub create_table_string: &'a str,
  pub column_string: &'a str,
}

impl<'a> PartialEq<Self> for Backend<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.name.eq(other.name)
  }
}

impl<'a> Eq for Backend<'a> {

}

pub trait QuoteName {
  fn quote_name(&self, name: String) -> String;
}

#[derive(Copy, Clone)]
pub struct BackendType<'a> {
  pub name: &'a str,
  pub needs_precision: bool,
  pub needs_scale: bool,
  pub needs_byte_length: bool,
  pub needs_char_length: bool
}

impl<'a> BackendType<'a> {

  const fn new(name: &'a str) -> Self {
    Self {
      name,
      needs_precision: false,
      needs_scale: false,
      needs_byte_length: false,
      needs_char_length: false,
    }
  }

  const fn needs_precision(mut self) -> Self {
    self.needs_precision = true;
    self
  }

  const fn needs_scale(mut self) -> Self {
    self.needs_scale = true;
    self
  }

  const fn needs_byte_length(mut self) -> Self {
    self.needs_byte_length = true;
    self
  }

  const fn needs_char_length(mut self) -> Self {
    self.needs_char_length = true;
    self
  }


}

pub const POSTGRES_BACKEND: Backend = Backend {
  name: "Postgres",
  mdb_bool: BackendType::new("BOOLEAN"),
  mdb_byte: BackendType::new("SMALLINT"),
  mdb_int: BackendType::new("INTEGER"),
  mdb_longint: BackendType::new("INTEGER"),
  mdb_money: BackendType::new("NUMERIC(15,4)"),
  mdb_float: BackendType::new("REAL"),
  mdb_double: BackendType::new("DOUBLE PRECISION"),
  mdb_datetime: BackendType::new("TIMESTAMP WITHOUT TIME ZONE"),
  mdb_binary: BackendType::new("BYTEA"),
  mdb_text: BackendType::new("VARCHAR").needs_char_length(),
  mdb_ole: BackendType::new("BYTEA"),
  mdb_memo: BackendType::new("TEXT"),
  mdb_replication_id: BackendType::new("UUID"),
  mdb_numeric: BackendType::new("NUMERIC").needs_precision().needs_scale(),
  mdb_extended_datetime: BackendType::new("TIMESTAMP"),
  mdb_complex: BackendType::new("complex?"),
  quote_name: quote_double_quotes,

  default_quote_str: "'",
  default_null_str: "NULL",
  create_table_string: "CREATE TABLE IF NOT EXISTS {quoted_table_name} (",
  column_string: r#"{quoted_column_name} {column_type}"#
};

/// Double the quote char and surround with quote.
fn quote_double_quotes(name: &str) -> String {
  format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_name_brackets(name: &str) -> String {
  format!("[{}]", name.replace(']', "]]"))
}

pub const MSSQL_BACKEND: Backend = Backend {
  name: "mssql",
  mdb_bool: BackendType::new("BIT"),
  mdb_byte: BackendType::new("CHAR"),
  mdb_int: BackendType::new("SMALLINT"),
  mdb_longint: BackendType::new("INT"),
  mdb_money: BackendType::new("MONEY"),
  mdb_float: BackendType::new("REAL"),
  mdb_double: BackendType::new("FLOAT"),
  mdb_datetime: BackendType::new("SMALLDATETIME"),
  mdb_binary: BackendType::new("VARBINARY").needs_byte_length(),
  mdb_text: BackendType::new("NVARCHAR").needs_char_length(),
  mdb_ole: BackendType::new("VARBINARY(MAX)"),
  mdb_memo: BackendType::new("NVARCHAR(MAX)"),
  mdb_replication_id: BackendType::new("UNIQUEIDENTIFIER"),
  mdb_numeric: BackendType::new("NUMERIC").needs_precision().needs_scale(),
  mdb_extended_datetime: BackendType::new("DATETIME2"),
  mdb_complex: BackendType::new("complex?"),
  quote_name: quote_name_brackets,

  default_quote_str: "'",
  default_null_str: "NULL",
  create_table_string: "IF OBJECT_ID(N'{quoted_table_name}', N'U') IS NULL\n  create table {quoted_table_name} (",

  column_string: "  {quoted_column_name} {column_type}"
};

pub const CSV_BACKEND: Backend = Backend {
  name: "csv",
  mdb_bool: BackendType::new("bool"),
  mdb_byte: BackendType::new("byte"),
  mdb_int: BackendType::new("int"),
  mdb_longint: BackendType::new("longint"),
  mdb_money: BackendType::new("money"),
  mdb_float: BackendType::new("float"),
  mdb_double: BackendType::new("double"),
  mdb_datetime: BackendType::new("datetime"),
  mdb_binary: BackendType::new("binary").needs_byte_length(),
  mdb_text: BackendType::new("text").needs_char_length(),
  mdb_ole: BackendType::new("ole"),
  mdb_memo: BackendType::new("memo"),
  mdb_replication_id: BackendType::new("replication_id"),
  mdb_numeric: BackendType::new("numeric").needs_precision().needs_scale(),
  quote_name: quote_double_quotes,
  mdb_extended_datetime: BackendType::new("DATETIME2"),
  mdb_complex: BackendType::new("complex?"),
  create_table_string: "",
  column_string: "",
  default_quote_str: "\"",
  default_null_str: "",
};

/// Double the quote char and surround with quote.
pub fn quote_generic(name: &str, quote: &str, escape: &Option<String>) -> String {
  let escape_string = if escape.is_some() {
    escape.clone().unwrap() + quote
  } else {
    quote.to_owned() + quote
  };

  format!("{quote}{}{quote}", name.replace(quote, &escape_string))
}

// quoted_table_name
// table_name
// quoted_column_name