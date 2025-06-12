//! # Database Management
//! 
//! This module provides all the core functionalities for interacting with the SQLite database.
//! It handles database initialization, connection, table creation, and listing tables.
//! The functions in this module are designed to be robust, with features like retry logic
//! for connections and integrity checks to ensure database validity.

use anyhow::{Context, Result};
use prettytable::{row, Table};
use rusqlite::{params, Connection};
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Initializes a new SQLite database file.
///
/// This function creates a new database file at the specified path. It includes logic to:
/// - Append `.db` if not present.
/// - Check if the database already exists to prevent overwriting.
/// - Create parent directories if they don't exist.
/// - Use a retry mechanism for creating the connection.
/// - Verify the integrity of the newly created database.
///
/// # Arguments
///
/// * `name` - The name of the database file to create.
///
/// # Returns
///
/// A `Result` which is `Ok(())` on successful creation, or an `Err` with context if it fails.
pub fn init_database(name: &str) -> Result<()> {
    let db_path = if name.ends_with(".db") {
        name.to_string()
    } else {
        format!("{}.db", name)
    };

    // Check if the database already exists
    if Path::new(&db_path).exists() {
        println!("Database '{}' already exists.", db_path);
        // Verify it's a valid SQLite database
        verify_database_integrity(&db_path)?;
        return Ok(());
    }

    // Create the database directory if it doesn't exist
    if let Some(parent) = Path::new(&db_path).parent() {
        if !parent.exists() {
            println!("Creating directory: {:?}", parent);
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create directory: {:?}. Check permissions and disk space.",
                    parent
                )
            })?;
        }
    }

    // Create a new SQLite database with retry logic
    let _conn = create_connection_with_retry(&db_path, 3)?;

    // Verify the database was created successfully
    verify_database_integrity(&db_path)?;

    println!("Successfully created database: {}", db_path);
    // Connection will be automatically dropped when it goes out of scope

    Ok(())
}

/// Connects to an existing SQLite database.
///
/// This function establishes a connection to a given database file. It performs several checks:
/// - Ensures the database file exists.
/// - Verifies that the path points to a file, not a directory.
/// - Uses a retry mechanism for the connection.
/// - Performs an integrity check on the database upon successful connection.
///
/// # Arguments
///
/// * `path` - The file path to the SQLite database.
///
/// # Returns
///
/// A `Result` which is `Ok(())` on successful connection, or an `Err` with context if it fails.
pub fn connect_database(path: &str) -> Result<()> {
    // Check if the database exists
    if !Path::new(path).exists() {
        anyhow::bail!(
            "Database '{}' does not exist. Use 'vapor-cli init --name {}' to create it.",
            path,
            path.trim_end_matches(".db")
        );
    }

    // Check if it's a file (not a directory)
    let metadata =
        fs::metadata(path).with_context(|| format!("Cannot read file metadata for '{}'", path))?;

    if metadata.is_dir() {
        anyhow::bail!("'{}' is a directory, not a database file", path);
    }

    // Try to connect to the database with retry logic
    let _conn = create_connection_with_retry(path, 3)?;

    // Verify database integrity
    verify_database_integrity(path)?;

    println!("Successfully connected to database: {}", path);
    // Connection will be automatically dropped when it goes out of scope

    Ok(())
}

/// Creates a new table in the specified database.
///
/// This function adds a new table to the database with the given name and column definitions.
/// It includes validation to:
/// - Ensure the database file exists.
/// - Check if a table with the same name already exists.
/// - Perform basic validation on the column definition syntax.
/// - Verify that the table was actually created after execution.
///
/// # Arguments
///
/// * `db_path` - The path to the database file.
/// * `table_name` - The name of the table to create.
/// * `columns` - A string defining the table's columns (e.g., "id INTEGER PRIMARY KEY, name TEXT").
///
/// # Returns
///
/// A `Result` which is `Ok(())` on successful table creation, or an `Err` with context if it fails.
pub fn create_table(db_path: &str, table_name: &str, columns: &str) -> Result<()> {
    // Validate database exists and is accessible
    if !Path::new(db_path).exists() {
        anyhow::bail!(
            "Database '{}' does not exist. Use 'vapor-cli init --name {}' to create it.",
            db_path,
            db_path.trim_end_matches(".db")
        );
    }

    // Connect to the database with retry logic
    let conn = create_connection_with_retry(db_path, 3)?;

    // Check if table already exists
    let table_exists = check_table_exists(&conn, table_name)?;
    if table_exists {
        println!(
            "Table '{}' already exists in database: {}",
            table_name, db_path
        );
        return Ok(());
    }

    // Validate column definition syntax
    validate_column_syntax(columns)?;

    // Create the table with proper error handling
    let create_table_sql = format!("CREATE TABLE {} ({})", table_name, columns);

    conn.execute(&create_table_sql, params![])
        .with_context(|| {
            format!(
                "Failed to create table '{}'. Check column syntax: {}",
                table_name, columns
            )
        })?;

    // Verify table was created successfully
    let table_exists_after = check_table_exists(&conn, table_name)?;
    if !table_exists_after {
        anyhow::bail!(
            "Table creation appeared to succeed but table '{}' is not found",
            table_name
        );
    }

    println!(
        "Successfully created table '{}' in database: {}",
        table_name, db_path
    );
    // Connection will be automatically dropped when it goes out of scope

    Ok(())
}

/// Lists all user-created tables in the specified database.
///
/// This function queries the `sqlite_master` table to find all tables, excluding the internal
/// `sqlite_` tables. It then prints the list of tables in a formatted table to the console.
///
/// # Arguments
///
/// * `db_path` - The path to the database file.
///
/// # Returns
///
/// A `Result` containing a `Vec<String>` of table names on success, or an `Err` with context if it fails.
pub fn list_tables(db_path: &str) -> Result<Vec<String>> {
    // Validate database exists and is accessible
    if !Path::new(db_path).exists() {
        anyhow::bail!(
            "Database '{}' does not exist. Use 'vapor-cli init --name {}' to create it.",
            db_path,
            db_path.trim_end_matches(".db")
        );
    }

    // Connect to the database with retry logic
    let conn = create_connection_with_retry(db_path, 3)?;

    // Query for all tables with error handling
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .context("Failed to prepare statement for listing tables. Database may be corrupted.")?;

    let table_names = stmt
        .query_map(params![], |row| row.get::<_, String>(0))
        .context("Failed to execute query for listing tables")?;

    // Create a pretty table for display
    let mut table = Table::new();
    table.add_row(row!["Table Name"]);

    let mut has_tables = false;
    let mut table_count = 0;
    let mut tables = Vec::new();

    for table_name_result in table_names {
        let name =
            table_name_result.with_context(|| "Failed to read table name from database result")?;
        table.add_row(row![&name]);
        tables.push(name);
        has_tables = true;
        table_count += 1;
    }

    if has_tables {
        println!("Tables in database '{}':", db_path);
        table.printstd();
        println!("Total: {} table(s)", table_count);
    } else {
        println!("No tables found in database: {}", db_path);
        println!("Use 'create-table' command to create your first table.");
    }

    // Connection will be automatically dropped when it goes out of scope
    Ok(tables)
}

/// Create a database connection with retry logic for handling temporary issues
fn create_connection_with_retry(db_path: &str, max_retries: u32) -> Result<Connection> {
    let mut last_error = None;

    for attempt in 1..=max_retries {
        match Connection::open(db_path) {
            Ok(conn) => {
                if attempt > 1 {
                    println!("Connection succeeded on attempt {}", attempt);
                }
                return Ok(conn);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    println!("Connection attempt {} failed, retrying...", attempt);
                    std::thread::sleep(Duration::from_millis(100 * attempt as u64));
                }
            }
        }
    }

    Err(last_error.unwrap())
        .with_context(|| format!(
            "Failed to connect to database '{}' after {} attempts. Database may be locked or corrupted.",
            db_path, max_retries
        ))
}

/// Verify database integrity
fn verify_database_integrity(db_path: &str) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("Cannot open database '{}' for integrity check", db_path))?;

    // Run a simple integrity check
    let integrity_result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .with_context(|| format!("Database '{}' failed integrity check", db_path))?;

    if integrity_result != "ok" {
        anyhow::bail!("Database integrity check failed: {}", integrity_result);
    }

    // Test basic functionality
    let test_result: i32 = conn
        .query_row("SELECT 1", [], |row| row.get(0))
        .with_context(|| format!("Database '{}' failed basic functionality test", db_path))?;

    if test_result != 1 {
        anyhow::bail!(
            "Basic functionality test failed: expected 1, got {}",
            test_result
        );
    }

    Ok(())
}

/// Check if a table exists in the database
fn check_table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1")
        .context("Failed to prepare table existence check query")?;

    let count: i64 = stmt
        .query_row(params![table_name], |row| row.get(0))
        .with_context(|| format!("Failed to check if table '{}' exists", table_name))?;

    Ok(count > 0)
}

/// Validate column definition syntax
fn validate_column_syntax(columns: &str) -> Result<()> {
    let columns = columns.trim();

    // Check for basic SQL injection patterns
    let dangerous_patterns = ["DROP", "DELETE", "INSERT", "UPDATE", "EXEC"];
    let columns_upper = columns.to_uppercase();

    for pattern in &dangerous_patterns {
        if columns_upper.contains(pattern) {
            anyhow::bail!(
                "Column definition contains potentially dangerous SQL keyword: {}",
                pattern
            );
        }
    }

    // Check for balanced parentheses
    let open_parens = columns.chars().filter(|&c| c == '(').count();
    let close_parens = columns.chars().filter(|&c| c == ')').count();

    if open_parens != close_parens {
        anyhow::bail!("Column definition has unbalanced parentheses");
    }

    // Check for at least one column definition
    if !columns.contains(' ') && !columns.contains(',') {
        anyhow::bail!(
            "Column definition appears incomplete. Example: 'id INTEGER PRIMARY KEY, name TEXT'"
        );
    }

    Ok(())
}
