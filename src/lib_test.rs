#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::fs;

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
        
        // Test selecting data
        execute_sql(&conn, "SELECT * FROM test_table", &QueryOptions::default()).unwrap();
    }
} 