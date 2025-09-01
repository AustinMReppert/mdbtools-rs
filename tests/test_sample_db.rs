use mdbtools;
use mdbtools::backend;
use mdbtools::catalog::{read_catalog, CatalogEntry};
use mdbtools::column::ColumnType;
use mdbtools::table::Table;
use std::path::PathBuf;

use std::fmt::Write;

fn open_sample_db() -> mdbtools::mdbfile::Mdb {
    let path = PathBuf::from("testdata/ASampleDatabase.accdb");
    mdbtools::mdbfile::Mdb::open(path).expect("Failed to open database")
}

fn get_table(mdb: &mut mdbtools::mdbfile::Mdb, table_name: &str) -> Table {
    let tables = read_catalog(mdb).expect("Failed to read catalog");

    let table_entry = tables
        .into_iter()
        .find_map(|entry| match entry {
            CatalogEntry::Table(table) if table.name == table_name => Some(table),
            _ => None,
        })
        .expect(&format!("Table '{}' not found", table_name));

    Table::from_catalog_entry(CatalogEntry::Table(table_entry), mdb).expect("Failed to read table")
}

fn assert_column_type(
    table: &Table,
    column_name: &str,
    expected_type: mdbtools::column::ColumnType,
) {
    let column = table
        .columns
        .iter()
        .find(|c| c.name == column_name)
        .unwrap_or_else(|| panic!("Column '{}' not found", column_name));

    assert_eq!(
        column.column_type, expected_type,
        "Column '{}' has wrong type",
        column_name
    );
}

#[test]
fn test_open_database() {
    open_sample_db();
}

#[test]
fn test_table_exists() {
    let mut mdb = open_sample_db();
    let mut table = get_table(&mut mdb, "Asset Items");
    let res = table.read_columns();
    assert!(res.is_ok(), "Failed to read columns for 'Asset Items'");
}

#[test]
fn test_read_columns() {
    let mut mdb = open_sample_db();
    let mut table = get_table(&mut mdb, "Asset Items");
    table.read_columns().expect("Failed to read columns");
    assert!(!table.columns.is_empty(), "Columns not read");
}

#[test]
fn test_column_types() {
    let mut mdb = open_sample_db();
    let mut table = get_table(&mut mdb, "Asset Items");
    table.read_columns().expect("Failed to read columns");

    let expected_columns = vec![
        ("Asset No", mdbtools::column::ColumnType::Text),
        ("Asset Category", mdbtools::column::ColumnType::Text),
        ("Make", mdbtools::column::ColumnType::Text),
        ("Model", mdbtools::column::ColumnType::Text),
        ("Description", mdbtools::column::ColumnType::Text),
        ("Owner", mdbtools::column::ColumnType::Text),
        ("Serial No", mdbtools::column::ColumnType::Text),
        ("Acquired", mdbtools::column::ColumnType::Datetime),
        ("Cost", mdbtools::column::ColumnType::Money),
        ("Warranty", mdbtools::column::ColumnType::LongInt),
        ("Tax Scale", mdbtools::column::ColumnType::Text),
        ("Supplier No", mdbtools::column::ColumnType::Text),
        ("Comments", mdbtools::column::ColumnType::Memo),
    ];

    for (name, col_type) in expected_columns {
        assert_column_type(&table, name, col_type);
    }
}

fn table_to_csv(table: &mut Table) -> String {
    table.read_columns().expect("Failed to read columns");

    let mut output = String::new();
    let mut first_row = true;

    loop {
        let _row = match table.fetch_row() {
            Ok(row) => row,
            Err(_) => break, // No more rows
        };

        if !first_row {
            writeln!(output).unwrap();
        }
        first_row = false;

        for (index, col) in table.columns.iter().enumerate() {
            if index != 0 {
                write!(output, ",").unwrap();
            }

            let mut is_null = false;
            let mut col_string = if col.buffer.is_null {
                match col.column_type {
                    ColumnType::Bool => col.to_string(),
                    _ => {
                        is_null = true;
                        "null".to_string()
                    }
                }
            } else {
                col.to_string()
            };

            let should_quote = !is_null && should_quote(col.column_type);
            if should_quote {
                col_string = backend::quote_generic(&col_string, "\"", &Some("\\".to_string()));
            }

            write!(output, "{}", col_string).unwrap();
        }
    }

    output
}

#[test]
fn test_data() {
    let mut mdb = open_sample_db();
    let mut table = get_table(&mut mdb, "Asset Items");
    let csv_output = table_to_csv(&mut table);
    let expected_csv =
        std::fs::read_to_string("testdata/AssetItems.csv").expect("Failed to read expected CSV");
    // Normalize line endings to LF
    let normalized_output = csv_output.replace("\r\n", "\n");
    let normalized_expected = expected_csv.replace("\r\n", "\n");

    assert_eq!(
        normalized_output, normalized_expected,
        "CSV output does not match expected"
    );
}

fn should_quote(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Text
            | ColumnType::Bool
            | ColumnType::Memo
            | ColumnType::Datetime
            | ColumnType::ExtendedDatetime
    )
}
