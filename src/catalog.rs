use crate::mdbfile::Mdb;
use crate::table::Table;
use crate::utils::{get_u16, get_u32};

pub enum CatalogEntry {
  Table(TableCatalogEntry)
}

/*enum {
  MDB_FORM = 0,
  MDB_TABLE = 1,
  MDB_MACRO,
  MDB_SYSTEM_TABLE,
  MDB_REPORT,
  MDB_QUERY,
  MDB_LINKED_TABLE,
  MDB_MODULE,
  MDB_RELATIONSHIP,
  MDB_UNKNOWN_09,
  MDB_UNKNOWN_0A, /* User access */
  MDB_DATABASE_PROPERTY,
  MDB_ANY = -1
};*/

pub struct TableCatalogEntry {
  pub name: String,
  pub page: u32,
  pub flags: u32,
}

impl TableCatalogEntry {
  pub fn is_system_table(&self) -> bool {
    self.flags & 0x80000002 != 0
  }
}

pub fn read_catalog(mdb: &mut Mdb) -> Result<Vec<CatalogEntry>, ()> {
  let mut catalog_entries: Vec<CatalogEntry> = Vec::new();

  match mdb.read_page(2) {
    Ok(_) => {}
    Err(_) => { return Err(()); }
  }

  let system_objects_table_catalog_entry = CatalogEntry::Table(TableCatalogEntry {
    name: "MSysObjects".to_string(),
    page: 2,
    flags: 0,
  });

  let mut system_objects_table = match Table::from_catalog_entry(system_objects_table_catalog_entry, mdb) {
    Ok(system_objects_table) => system_objects_table,
    Err(_) => {
      eprintln!("Failed to load system objects table for catalog.");
      return Err(());
    }
  };

  system_objects_table.read_columns().expect("failed to read columns");

  let id_index = system_objects_table.find_column_index("Id").expect("Id column not found in system table.");
  let name_index = system_objects_table.find_column_index("Name").expect("Name column not found in system table.");
  let type_index = system_objects_table.find_column_index("Type").expect("Type column not found in system table.");
  let flags_index = system_objects_table.find_column_index("Flags").expect("Flags column not found in system table.");
  //let properties_index = system_objects_table.find_column_index("LvProp").expect("LvProp column not found in system table.");

  while let Ok(_row) = system_objects_table.fetch_row() {
    let id_column = &system_objects_table.columns[id_index];
    let name_column = &system_objects_table.columns[name_index];
    let type_column = &system_objects_table.columns[type_index];
    let flags_column = &system_objects_table.columns[flags_index];
    //let properties_column = &system_objects_table.columns[properties_index];

    let entry_type = get_u16(&type_column.buffer.value, 0);
    let name = system_objects_table.mdb.encoding.decode(&name_column.buffer.value).0.to_string();
    let id = get_u32(&id_column.buffer.value, 0);
    let flags = get_u32(&flags_column.buffer.value, 0);
    if entry_type == 1 {
      catalog_entries.push(CatalogEntry::Table(TableCatalogEntry {
        name,
        page: id & 0x00FFFFFF,
        flags,
      }))
    }
  }

  Ok(catalog_entries)
}