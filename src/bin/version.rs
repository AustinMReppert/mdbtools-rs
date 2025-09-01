use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser};

use mdbtools;
use mdbtools::mdbfile::Mdb;

/// Display MDB file version
///
/// It  will return a single line of output corresponding to the program that produced the file: 'JET3' (for files produced by Access 97), 'JET4' (Access 2000, XP and 2003),
/// 'ACE12' (Access 2007), 'ACE14' (Access 2010), 'ACE15' (Access 2013), or 'ACE16' (Access 2016).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
struct Args {

  /// Path to file
  #[arg(short, long, value_name = "FILE")]
  file: PathBuf,
}

pub fn main() -> ExitCode {
  let args = Args::parse();

  let mdb = match Mdb::open(args.file) {
    Ok(mdb) => mdb,
    Err(_err) => {
      return ExitCode::FAILURE;
    },
  };

  println!("{}", mdb.mdb_file.jet_version);

  ExitCode::SUCCESS
}