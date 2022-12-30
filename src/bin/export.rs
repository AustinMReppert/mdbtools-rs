use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ColorChoice};

use mdbtools;
use mdbtools::backend;
use mdbtools::backend::Backend;
use mdbtools::mdbfile::Mdb;
use mdbtools::catalog::{CatalogEntry, read_catalog, TableCatalogEntry};
use mdbtools::column::ColumnType;
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

  /// Specify an alternative column delimiter. Default is comma.
  #[arg(short = 'd', long, default_value_t = String::from(","))]
  delimiter: String,

  /// Table name.
  #[arg(short = 't', long = "table")]
  table: String,

  /// Path to file.
  #[arg(short, long, value_name = "FILE")]
  file: PathBuf,

  /// Don't wrap text-like fields in quotes.
  #[arg(short = 'Q', long, default_value_t = false)]
  no_quote: bool,

  /// Specify an alternative quote. The backend is consulted for the default.
  #[arg(short = 'q', long)]
  quote: Option<String>,

  /// Backend. Default is CSV.
  #[arg(short = 'b', long, default_value_t = String::from("csv"))]
  backend: String,

  /// Null specifier. Default is backend specific.
  #[arg(short = 'n', long)]
  null: Option<String>,

  /// Schema/namespace. Default is "".
  #[arg(short = 's', long, default_value_t = String::from(""))]
  schema: String,

  /// Escape quoted characters within a field. The default approach is to double thq quote string.
  #[arg(short = 'X', long)]
  escape: Option<String>,
}

pub fn main() -> ExitCode {
  let args = Args::parse();

  let mut mdb = match Mdb::open(args.file.clone()) {
    Ok(mdb) => mdb,
    Err(err) => {
      eprintln!("{}", err);
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

  let mut table_catalog_entry: Option<TableCatalogEntry> = None;
  for catalog_entry in catalog {
    match catalog_entry {
      CatalogEntry::Table(table) => {
        if table.name.eq(&args.table) {
          table_catalog_entry = Some(table);
          break;
        }
      }
    }
  };

  if table_catalog_entry.is_none() {
    println!("Table not found.");
    return ExitCode::FAILURE;
  }

  let table_catalog_entry = table_catalog_entry.unwrap();
  let mut table = Table::from_catalog_entry(CatalogEntry::Table(table_catalog_entry), &mut mdb).expect("Could not read table.");
  let columns = table.read_columns().expect("Could not read table.");

  let backend_name = args.backend.to_lowercase();
  let backends: Vec<Backend> = vec![backend::CSV_BACKEND, backend::MSSQL_BACKEND, backend::POSTGRES_BACKEND];

  let mut backend: Backend = backend::CSV_BACKEND;
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

  if backend == backend::CSV_BACKEND {
    print_header(&args, &mut table);
  } else {
    if table.row_count == 0 {
      return ExitCode::SUCCESS;
    }

    print!("INSERT INTO {} (", (backend.quote_name)(&table.name));

    for (index, col) in (&table.columns).into_iter().enumerate() {
      if index != 0 {
        print!(", ");
      }

      print!("{}", (backend.quote_name)(&col.name));
    }
    println!(")\nVALUES");
  }

  let null = args.null.unwrap_or(backend.default_null_str.to_string());

  let mut first = true;
  loop {
    let _result = match table.fetch_row() {
      Ok(row) => row,
      Err(_) => {
        // If there is a problem, return the known good values.
        break;
      }
    };

    if backend != backend::CSV_BACKEND {
      if !first {
        print!("),\n  (");
      } else {
        print!("  (");
      }
    }

    first = false;

    for (index, col) in (&table.columns).into_iter().enumerate() {
      if index != 0 {
        print!(",");
      }

      let mut is_null = false;
      let mut col_string = match col.buffer.is_null {
        true => {
          match col.column_type {
            ColumnType::Bool => col.to_string(),
            _ => {
              is_null = true;
              null.clone()
            }
          }
        }
        false => col.to_string()
      };

      let should_quote = !is_null && should_quote(col.column_type);
      if should_quote && !args.no_quote {
        col_string = backend::quote_generic(&col_string, backend.default_quote_str, &args.escape);
      }

      print!("{}", col_string);

      if index == table.columns.len() - 1 {
        if backend != backend::CSV_BACKEND {
        } else {
          println!();
        }
      }
    }
  }

  if backend != backend::CSV_BACKEND {
    println!("\n);");
  }

  ExitCode::SUCCESS
}

fn print_header(args: &Args, table: &mut Table) {
  if !args.no_header {
    for (index, col) in (&table.columns).into_iter().enumerate() {
      print!("{}", col.name);
      if args.types {
        print!("({}", col.column_type);
        if col.is_hyperlink {
          print!("/Hyperlink");
        }
        print!(")");
      }

      if index == table.columns.len() - 1 {
        println!();
      } else {
        print!(",");
      }
    }
  }
}

fn should_quote(column_type: ColumnType) -> bool {
  matches!(column_type, ColumnType::Text | ColumnType::Bool | ColumnType::Memo | ColumnType::Datetime | ColumnType::ExtendedDatetime)
}