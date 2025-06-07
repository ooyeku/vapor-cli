pub mod bookmarks;
pub mod db;
pub mod display;
pub mod export;
pub mod transactions;
pub mod repl;

pub use repl::repl_mode;
pub use db::list_tables;
pub use display::{execute_sql, show_table_schema, show_all_schemas, show_database_info, OutputFormat, QueryOptions};
pub use export::export_to_csv;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_list_tables() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        // Create a test database with a table
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        ).unwrap();
        
        // Test listing tables
        let tables = list_tables(db_path).unwrap();
        assert!(tables.contains(&"test_table".to_string()));
    }

    #[test]
    fn test_execute_sql() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        ).unwrap();
        
        // Test inserting data
        execute_sql(&conn, "INSERT INTO test_table (name) VALUES ('test')", &QueryOptions::default()).unwrap();
        
        // Test selecting data with explicit column types
        execute_sql(&conn, "SELECT id, name FROM test_table", &QueryOptions::default()).unwrap();
    }

    #[test]
    fn test_show_table_schema() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        ).unwrap();
        
        // Test showing schema
        show_table_schema(&conn, "test_table").unwrap();
    }

    #[test]
    fn test_show_all_schemas() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        ).unwrap();
        
        // Test showing all schemas
        show_all_schemas(&conn).unwrap();
    }

    #[test]
    fn test_show_database_info() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        ).unwrap();
        
        // Test showing database info
        show_database_info(&conn, db_path).unwrap();
    }
} 