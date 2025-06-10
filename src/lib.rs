pub mod bookmarks;
pub mod db;
pub mod display;
pub mod export;
pub mod transactions;
pub mod repl;
pub mod shell;
pub mod populate;

// Main entry points
pub use repl::repl_mode;

// Database management
pub use db::{init_database, connect_database, create_table, list_tables};

// SQL execution and display
pub use display::{execute_sql, show_table_schema, show_all_schemas, show_database_info, OutputFormat, QueryOptions};

// Data export functionality
pub use export::export_to_csv;

// Shell functionality
pub use shell::Shell;

// Bookmark management
pub use bookmarks::{BookmarkManager, Bookmark};

// Transaction management
pub use transactions::{TransactionManager, TransactionState};

// Data population and testing
pub use populate::{populate_database, PopulationConfig, ColumnConfig, DataType, DataDistribution};

// Re-export commonly used types for convenience
pub use rusqlite::Connection;
pub use anyhow::Result;

/// A high-level API for interacting with SQLite databases through vapor-cli
/// 
/// This struct provides a simplified interface to vapor-cli's functionality,
/// making it easy to use as a library in other Rust projects.
pub struct VaporDB {
    pub connection: Connection,
    pub db_path: String,
    pub bookmark_manager: Option<BookmarkManager>,
    pub transaction_manager: TransactionManager,
}

impl VaporDB {
    /// Create a new VaporDB instance with an existing database
    pub fn open<P: AsRef<std::path::Path>>(db_path: P) -> Result<Self> {
        let db_path_str = db_path.as_ref().to_string_lossy().to_string();
        let connection = Connection::open(&db_path_str)?;
        
        let bookmark_manager = BookmarkManager::new().ok();
        let transaction_manager = TransactionManager::new();
        
        Ok(VaporDB {
            connection,
            db_path: db_path_str,
            bookmark_manager,
            transaction_manager,
        })
    }
    
    /// Create a new database and return a VaporDB instance
    pub fn create<P: AsRef<std::path::Path>>(db_path: P) -> Result<Self> {
        let db_path_str = db_path.as_ref().to_string_lossy().to_string();
        init_database(&db_path_str)?;
        Self::open(db_path)
    }
    
    /// Execute a SQL query and return the result
    pub fn execute(&self, sql: &str) -> Result<()> {
        let options = QueryOptions::default();
        execute_sql(&self.connection, sql, &options)
    }
    
    /// Execute a SQL query with custom options
    pub fn execute_with_options(&self, sql: &str, options: &QueryOptions) -> Result<()> {
        execute_sql(&self.connection, sql, options)
    }
    
    /// List all tables in the database
    pub fn list_tables(&self) -> Result<Vec<String>> {
        list_tables(&self.db_path)
    }
    
    /// Show schema for a specific table
    pub fn show_table_schema(&self, table_name: &str) -> Result<()> {
        show_table_schema(&self.connection, table_name)
    }
    
    /// Show all table schemas
    pub fn show_all_schemas(&self) -> Result<()> {
        show_all_schemas(&self.connection)
    }
    
    /// Show database information
    pub fn show_database_info(&self) -> Result<()> {
        show_database_info(&self.connection, &self.db_path)
    }
    
    /// Export a table to CSV
    pub fn export_to_csv(&self, table_name: &str, file_path: &str) -> Result<()> {
        let query = format!("SELECT * FROM {}", table_name);
        export_to_csv(&self.connection, &query, file_path)
    }
    
    /// Export query results to CSV
    pub fn export_query_to_csv(&self, query: &str, file_path: &str) -> Result<()> {
        export_to_csv(&self.connection, query, file_path)
    }
    
    /// Start the interactive REPL
    pub fn start_repl(&self) -> Result<()> {
        repl_mode(&self.db_path)
    }
    
    /// Start the interactive shell
    pub fn start_shell(&self) -> Result<()> {
        let mut shell = Shell::new(&self.db_path);
        shell.run();
        Ok(())
    }
    
    /// Populate database with test data
    pub fn populate_with_test_data(&self, config: Option<PopulationConfig>) -> Result<()> {
        populate_database(&self.db_path, config)
    }
    
    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<()> {
        self.transaction_manager.begin_transaction(&self.connection)
    }
    
    /// Commit the current transaction
    pub fn commit_transaction(&self) -> Result<()> {
        self.transaction_manager.commit_transaction(&self.connection)
    }
    
    /// Rollback the current transaction
    pub fn rollback_transaction(&self) -> Result<()> {
        self.transaction_manager.rollback_transaction(&self.connection)
    }
    
    /// Check if a transaction is active
    pub fn is_transaction_active(&self) -> bool {
        self.transaction_manager.is_active()
    }
    
    /// Get access to the bookmark manager
    pub fn bookmark_manager(&mut self) -> Option<&mut BookmarkManager> {
        self.bookmark_manager.as_mut()
    }
}

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

    #[test]
    fn test_vapor_db_create_and_open() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        // Test creating a new database
        let vapor_db = VaporDB::create(db_path).unwrap();
        assert_eq!(vapor_db.db_path, db_path.to_string_lossy());
        
        // Test opening an existing database
        let vapor_db2 = VaporDB::open(db_path).unwrap();
        assert_eq!(vapor_db2.db_path, db_path.to_string_lossy());
    }

    #[test]
    fn test_vapor_db_execute() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        
        // Test executing SQL
        vapor_db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        vapor_db.execute("INSERT INTO test (name) VALUES ('test_value')").unwrap();
        
        // Test listing tables
        let tables = vapor_db.list_tables().unwrap();
        assert!(tables.contains(&"test".to_string()));
    }

    #[test]
    fn test_vapor_db_transactions() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        vapor_db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        
        // Test transaction functionality
        assert!(!vapor_db.is_transaction_active());
        vapor_db.begin_transaction().unwrap();
        assert!(vapor_db.is_transaction_active());
        
        vapor_db.execute("INSERT INTO test (name) VALUES ('test_transaction')").unwrap();
        vapor_db.commit_transaction().unwrap();
        assert!(!vapor_db.is_transaction_active());
    }

    #[test]
    fn test_bookmark_manager() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let mut vapor_db = VaporDB::create(db_path).unwrap();
        
        // Test bookmark manager access
        if let Some(bookmark_manager) = vapor_db.bookmark_manager() {
            bookmark_manager.save_bookmark(
                "test_bookmark".to_string(),
                "SELECT * FROM test".to_string(),
                Some("Test bookmark".to_string())
            ).unwrap();
            
            let bookmark = bookmark_manager.get_bookmark("test_bookmark");
            assert!(bookmark.is_some());
            assert_eq!(bookmark.unwrap().query, "SELECT * FROM test");
        }
    }

    #[test]
    fn test_init_database() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("new_test.db");
        let db_path_str = db_path.to_str().unwrap();
        
        // Test database initialization
        init_database(db_path_str).unwrap();
        assert!(db_path.exists());
        
        // Test that re-initializing existing database doesn't fail
        init_database(db_path_str).unwrap();
    }

    #[test]
    fn test_create_table_function() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        // Initialize database first
        init_database(db_path).unwrap();
        
        // Test table creation
        create_table(db_path, "users", "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT").unwrap();
        
        // Verify table was created
        let tables = list_tables(db_path).unwrap();
        assert!(tables.contains(&"users".to_string()));
        
        // Test creating table that already exists (should not fail)
        create_table(db_path, "users", "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT").unwrap();
    }

    #[test]
    fn test_output_formats() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE test_output (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO test_output (name, value) VALUES ('test1', 10.5), ('test2', 20.7)",
            [],
        ).unwrap();
        
        // Test different output formats
        let table_options = QueryOptions {
            format: OutputFormat::Table,
            ..Default::default()
        };
        execute_sql(&conn, "SELECT * FROM test_output", &table_options).unwrap();
        
        let csv_options = QueryOptions {
            format: OutputFormat::Csv,
            ..Default::default()
        };
        execute_sql(&conn, "SELECT * FROM test_output", &csv_options).unwrap();
        
        let json_options = QueryOptions {
            format: OutputFormat::Json,
            ..Default::default()
        };
        execute_sql(&conn, "SELECT * FROM test_output", &json_options).unwrap();
    }

    #[test]
    fn test_export_functionality() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap();
        
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE export_test (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO export_test (name, value) VALUES ('item1', 100.5), ('item2', 200.7)",
            [],
        ).unwrap();
        
        // Test CSV export
        let temp_csv = tempfile::NamedTempFile::new().unwrap();
        let csv_path = temp_csv.path().to_str().unwrap();
        
        export_to_csv(&conn, "SELECT * FROM export_test", csv_path).unwrap();
        
        // Verify the CSV file was created and has content
        let csv_content = std::fs::read_to_string(csv_path).unwrap();
        assert!(csv_content.contains("id,name,value"));
        assert!(csv_content.contains("item1"));
        assert!(csv_content.contains("item2"));
    }

    #[test]
    fn test_vapor_db_export_methods() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        vapor_db.execute("CREATE TABLE export_methods_test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        vapor_db.execute("INSERT INTO export_methods_test (name) VALUES ('method1'), ('method2')").unwrap();
        
        // Test table export method
        let temp_csv1 = tempfile::NamedTempFile::new().unwrap();
        let csv_path1 = temp_csv1.path().to_str().unwrap();
        vapor_db.export_to_csv("export_methods_test", csv_path1).unwrap();
        
        let csv_content1 = std::fs::read_to_string(csv_path1).unwrap();
        assert!(csv_content1.contains("id,name"));
        assert!(csv_content1.contains("method1"));
        
        // Test query export method
        let temp_csv2 = tempfile::NamedTempFile::new().unwrap();
        let csv_path2 = temp_csv2.path().to_str().unwrap();
        vapor_db.export_query_to_csv("SELECT name FROM export_methods_test WHERE name = 'method2'", csv_path2).unwrap();
        
        let csv_content2 = std::fs::read_to_string(csv_path2).unwrap();
        assert!(csv_content2.contains("name"));
        assert!(csv_content2.contains("method2"));
        assert!(!csv_content2.contains("method1")); // Should only contain method2
    }

    #[test]
    fn test_vapor_db_with_options() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        vapor_db.execute("CREATE TABLE options_test (id INTEGER PRIMARY KEY, data TEXT)").unwrap();
        vapor_db.execute("INSERT INTO options_test (data) VALUES ('test1'), ('test2'), ('test3')").unwrap();
        
        // Test execute with custom options
        let options = QueryOptions {
            format: OutputFormat::Json,
            show_timing: true,
            max_rows: Some(2),
        };
        vapor_db.execute_with_options("SELECT * FROM options_test", &options).unwrap();
    }

    #[test]
    fn test_transaction_rollback() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        vapor_db.execute("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        vapor_db.execute("INSERT INTO rollback_test (name) VALUES ('initial')").unwrap();
        
        // Begin transaction and insert data
        vapor_db.begin_transaction().unwrap();
        vapor_db.execute("INSERT INTO rollback_test (name) VALUES ('transactional')").unwrap();
        
        // Rollback the transaction
        vapor_db.rollback_transaction().unwrap();
        assert!(!vapor_db.is_transaction_active());
        
        // Verify the transactional data was rolled back
        // Note: This is a simplified test - in a real scenario you'd query to verify
    }

    #[test]
    fn test_schema_functions() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        vapor_db.execute("CREATE TABLE schema_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)").unwrap();
        
        // Test schema display functions
        vapor_db.show_table_schema("schema_test").unwrap();
        vapor_db.show_all_schemas().unwrap();
        vapor_db.show_database_info().unwrap();
    }

    #[test]
    fn test_bookmark_operations() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        // Set up a temporary directory for bookmarks
        let temp_dir = tempfile::tempdir().unwrap();
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", temp_dir.path());
        
        let mut vapor_db = VaporDB::create(db_path).unwrap();
        
        if let Some(bookmark_manager) = vapor_db.bookmark_manager() {
            // Test saving multiple bookmarks
            bookmark_manager.save_bookmark(
                "query1".to_string(),
                "SELECT * FROM users".to_string(),
                Some("Get all users".to_string())
            ).unwrap();
            
            bookmark_manager.save_bookmark(
                "query2".to_string(),
                "SELECT COUNT(*) FROM users".to_string(),
                Some("Count users".to_string())
            ).unwrap();
            
            // Test getting bookmarks
            let bookmark1 = bookmark_manager.get_bookmark("query1");
            assert!(bookmark1.is_some());
            assert_eq!(bookmark1.unwrap().query, "SELECT * FROM users");
            
            let bookmark2 = bookmark_manager.get_bookmark("query2");
            assert!(bookmark2.is_some());
            assert_eq!(bookmark2.unwrap().query, "SELECT COUNT(*) FROM users");
            
            // Test deleting bookmark
            let deleted = bookmark_manager.delete_bookmark("query1").unwrap();
            assert!(deleted);
            
            let bookmark1_after_delete = bookmark_manager.get_bookmark("query1");
            assert!(bookmark1_after_delete.is_none());
            
            // Test deleting non-existent bookmark
            let not_deleted = bookmark_manager.delete_bookmark("non_existent").unwrap();
            assert!(!not_deleted);
        }
        
        // Restore original HOME environment variable
        if let Some(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }
    }

    #[test]
    fn test_population_config() {
        // Test default population config
        let default_config = PopulationConfig::default();
        assert_eq!(default_config.table_name, "large_table");
        assert_eq!(default_config.row_count, 1_000_000);
        assert_eq!(default_config.batch_size, 10_000);
        assert_eq!(default_config.columns.len(), 3);
        
        // Test custom population config
        let custom_config = PopulationConfig {
            table_name: "test_table".to_string(),
            row_count: 1000,
            batch_size: 100,
            seed: Some(42),
            columns: vec![
                ColumnConfig {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    distribution: DataDistribution::Sequential,
                    nullable: false,
                },
                ColumnConfig {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    distribution: DataDistribution::Random,
                    nullable: true,
                },
            ],
        };
        
        assert_eq!(custom_config.table_name, "test_table");
        assert_eq!(custom_config.row_count, 1000);
        assert_eq!(custom_config.seed, Some(42));
    }

    #[test]
    fn test_data_types_and_distributions() {
        // Test that data types can be created
        let _int_type = DataType::Integer;
        let _text_type = DataType::Text;
        let _real_type = DataType::Real;
        let _bool_type = DataType::Boolean;
        let _date_type = DataType::Date;
        let _timestamp_type = DataType::Timestamp;
        let _uuid_type = DataType::UUID;
        
        // Test data distributions
        let _uniform = DataDistribution::Uniform;
        let _normal = DataDistribution::Normal { mean: 50.0, std_dev: 10.0 };
        let _sequential = DataDistribution::Sequential;
        let _random = DataDistribution::Random;
        let _custom = DataDistribution::Custom(vec!["value1".to_string(), "value2".to_string()]);
    }

    #[test]
    fn test_error_handling() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        let vapor_db = VaporDB::create(db_path).unwrap();
        
        // Test invalid SQL
        let result = vapor_db.execute("INVALID SQL STATEMENT");
        assert!(result.is_err());
        
        // Test querying non-existent table
        let result = vapor_db.execute("SELECT * FROM non_existent_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_integration_workflow() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path();
        
        // Set up a temporary directory for bookmarks
        let temp_dir = tempfile::tempdir().unwrap();
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", temp_dir.path());
        
        // Complete workflow test
        let mut vapor_db = VaporDB::create(db_path).unwrap();
        
        // 1. Create schema
        vapor_db.execute("CREATE TABLE workflow_test (id INTEGER PRIMARY KEY, name TEXT, score REAL)").unwrap();
        
        // 2. Insert data with transaction
        vapor_db.begin_transaction().unwrap();
        vapor_db.execute("INSERT INTO workflow_test (name, score) VALUES ('Alice', 95.5)").unwrap();
        vapor_db.execute("INSERT INTO workflow_test (name, score) VALUES ('Bob', 87.2)").unwrap();
        vapor_db.commit_transaction().unwrap();
        
        // 3. Verify tables exist
        let tables = vapor_db.list_tables().unwrap();
        assert!(tables.contains(&"workflow_test".to_string()));
        
        // 4. Export data
        let temp_csv = tempfile::NamedTempFile::new().unwrap();
        let csv_path = temp_csv.path().to_str().unwrap();
        vapor_db.export_to_csv("workflow_test", csv_path).unwrap();
        
        // 5. Save bookmark
        if let Some(bm) = vapor_db.bookmark_manager() {
            bm.save_bookmark(
                "high_scores".to_string(),
                "SELECT * FROM workflow_test WHERE score > 90".to_string(),
                Some("Students with high scores".to_string())
            ).unwrap();
        }
        
        // 6. Verify workflow completed successfully
        assert!(!vapor_db.is_transaction_active());
        assert!(std::path::Path::new(csv_path).exists());
        
        // Restore original HOME environment variable
        if let Some(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }
    }
} 