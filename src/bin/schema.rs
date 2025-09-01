use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ColorChoice};

use mdbtools;
use mdbtools::backend;
use mdbtools::backend::Backend;
use mdbtools::mdbfile::Mdb;
use mdbtools::catalog::{CatalogEntry, read_catalog, TableCatalogEntry};
use mdbtools::table::Table;

/// Get listing of tables in an MDB database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, color = ColorChoice::Auto)]
struct Args {
  /// Suppress header row.
  #[arg(short = 'H', long, default_value_t = false)]
  no_header: bool,

  /// Add types to header.
  #[arg(long, default_value_t = false)]
  types: bool,

  /// Path to file.
  #[arg(short, long, value_name = "FILE")]
  file: PathBuf,

  /// Don't wrap text-like fields in quotes.
  #[arg(short = 'Q', long, default_value_t = false)]
  no_quote: bool,

  /// Specify an alternative quote. Default is ".
  #[arg(short = 'q', long, default_value_t = String::from("\""))]
  quote: String,

  /// Backend. Default is CSV.
  #[arg(short = 'b', long, default_value_t = String::from("csv"))]
  backend: String,

  /// Null specifier. Default is "".
  #[arg(short = 'n', long, default_value_t = String::from(""))]
  null: String,

  /// Schema/namespace. Default is "".
  #[arg(short = 's', long)]
  schema: Option<String>,

  /// Table name.
  #[arg(short = 'T', long)]
  table: Option<String>,
}

pub fn main() -> ExitCode {
  let args = Args::parse();

  let backend_name = args.backend.to_lowercase();
  let backends: Vec<Backend> = vec![backend::MSSQL_BACKEND, backend::POSTGRES_BACKEND];

  let mut backend: Backend = backend::MSSQL_BACKEND;
  for (index, cur_backend) in backends.iter().enumerate() {
    if backend_name.eq(&cur_backend.name.to_lowercase()) {
      backend = cur_backend.clone();
      break;
    }
    if index + 1 == backends.len() {
      eprintln!("Backend not found.");
      return ExitCode::FAILURE;
    }
  };

  let mut mdb = match Mdb::open(args.file.clone()) {
    Ok(mdb) => mdb,
    Err(_err) => {
      return ExitCode::FAILURE;
    }
  };

  let catalog = match read_catalog(&mut mdb) {
    Ok(row) => row,
    Err(_) => {
      println!("Error reading system table. Exiting.");
      return ExitCode::FAILURE;
    }
  };

  for catalog_entry in catalog {
    match catalog_entry {
      CatalogEntry::Table(table) => {
        if args.table.is_some() && !args.table.as_ref().unwrap().eq(&table.name) {
          continue;
        }
        print_table_schema(table, &args, &mut mdb, &backend);
      }
    }
  };

  ExitCode::SUCCESS
}

fn print_table_schema(table: TableCatalogEntry, args: &Args, mdb: &mut Mdb, backend: &Backend) {
  if table.is_system_table() && args.table.is_none() {
    return;
  }

  let mut table = Table::from_catalog_entry(CatalogEntry::Table(table), mdb).expect("Could not read table.");
  table.read_columns().expect("Could not read table.");

  let schema_name = args.schema.as_deref().unwrap_or(&String::new()).to_string();
  let quoted_schema_name = if schema_name.is_empty() {schema_name.clone()} else {(backend.quote_name)(&schema_name) + "."};
  let quoted_table_name = quoted_schema_name.clone() + (backend.quote_name)(&table.name).as_str();
  let table_name = quoted_schema_name.clone() + table.name.as_str();

  let mut create_statement = backend.create_table_string.to_string();
  create_statement = create_statement.replace("{quoted_table_name}", &quoted_table_name);
  create_statement = create_statement.replace("{table_name}", &table_name);
  println!("{}", create_statement);

  let mut first: bool = true;
  for col in  table.columns {
    let quoted_column_name = (backend.quote_name)(&col.name);
    let mut col_string = backend.column_string.to_string();
    let column_type = col.get_backend_type(backend);
    col_string = col_string.replace("{quoted_column_name}", &quoted_column_name);

    let precision = col.precision;
    let scale = col.scale;
    if column_type.needs_precision && column_type.needs_scale {
      col_string = col_string.replace("{column_type}",  &format!("{{column_type}}({precision},{scale})"));
    } else if column_type.needs_precision {
      col_string = col_string.replace("{column_type}",  &format!("{{column_type}}({precision})"));
    }

    col_string = col_string.replace("{column_type}", &column_type.name);

    if !first {
      print!(",\n  {}", col_string);
    } else {
      print!("  {}", col_string);
    }
    if column_type.needs_char_length {
      print!("({})", col.size/2);
    }
    first = false;
  }
  println!();

  println!(");");
  println!();
}