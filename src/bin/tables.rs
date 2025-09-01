use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ColorChoice};

use mdbtools;
use mdbtools::mdbfile::Mdb;
use mdbtools::catalog::{CatalogEntry, read_catalog};

/// Get listing of tables in an MDB database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, color = ColorChoice::Auto)]
struct Args {
  /// Include system tables
  ///
  /// System tables are generally those beginning with 'MSys'.
  #[arg(short = 'S', long, default_value_t = false)]
  system: bool,

  /// One table name per line
  ///
  /// Specifies that the tables should be listed 1 per line.
  #[arg(short = '1', long, default_value_t = false)]
  single_column: bool,

  /// Table name delimiter
  ///
  /// Specifies an alternative delimiter. If no delimiter is specified, table names will be delimited by a space.
  #[arg(short = 'd', long, default_value_t = String::from(" "))]
  delimiter: String,

  /// Type of entry
  #[arg(short = 't', long = "type")]
  entry_type: Option<String>,

  /// Show type
  #[arg(short = 'T', long = "showtype")]
  show_type: Option<bool>,

  /// Path to file
  #[arg(short, long, value_name = "FILE")]
  file: PathBuf,
}

pub fn main() -> ExitCode {
  let args = Args::parse();

  let mut mdb = match Mdb::open(args.file) {
    Ok(mdb) => mdb,
    Err(_err) => {
      return ExitCode::FAILURE;
    },
  };

  let catalog = match read_catalog(&mut mdb) {
    Ok(row) => row,
    Err(_) => {
      println!("Error reading system table. Exiting.");
      return ExitCode::FAILURE;
    }
  };


  for (index, catalog_entry) in catalog.iter().enumerate() {
    match catalog_entry {
      CatalogEntry::Table(table) => {
        if !args.system && table.is_system_table() {
          continue;
        }

        print!("{}", table.name);
        if index != catalog.len() - 1 {
          print!("{}", args.delimiter);
        }
        if args.single_column {
          println!();
        }
      }
    }
  }

  if !args.single_column {
    println!();
  }

  ExitCode::SUCCESS
}