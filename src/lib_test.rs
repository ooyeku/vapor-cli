//! # Library Integration Tests
//!
//! This module contains integration-style tests for the `vapor-cli` library functions.
//! It uses `tempfile` to create temporary databases for each test, ensuring that
//! tests are isolated and do not interfere with each other or the user's system.

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::fs;

        /// Tests the `list_tables` function.
    ///
    /// This test verifies that `list_tables` can correctly identify and list the names
    /// of tables in a newly created database.
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

        /// Tests the `execute_sql` function for basic `INSERT` and `SELECT` operations.
    ///
    /// This test ensures that `execute_sql` can:
    /// 1. Successfully insert data into a table.
    /// 2. Successfully select data from a table.
    /// It does not validate the output format, only that the operations execute without error.
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
        
        // Test selecting data
        execute_sql(&conn, "SELECT * FROM test_table", &QueryOptions::default()).unwrap();
    }
} 