use anyhow::{Context, Result};
use prettytable::{row, Table};
use rusqlite::{Connection, params};
use std::time::{Instant, Duration};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum DisplayError {
    DatabaseError(String),
    QueryError(String),
}

impl fmt::Display for DisplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DisplayError::QueryError(msg) => write!(f, "Query error: {}", msg),
            DisplayError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
        }
    }
}

impl Error for DisplayError {}

pub struct QueryOptions {
    pub format: OutputFormat,
    pub max_rows: Option<usize>,
    pub show_timing: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
            max_rows: Some(1000),
            show_timing: true,
        }
    }
}

#[allow(dead_code)]
pub struct QueryCache {
    results: HashMap<String, (Vec<Vec<String>>, Instant)>,
    max_size: usize,
    ttl: Duration,
}

#[allow(dead_code)]
impl QueryCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            results: HashMap::new(),
            max_size,
            ttl,
        }
    }

    pub fn get(&self, query: &str) -> Option<&Vec<Vec<String>>> {
        if let Some((results, timestamp)) = self.results.get(query) {
            if timestamp.elapsed() < self.ttl {
                return Some(results);
            }
        }
        None
    }

    pub fn insert(&mut self, query: String, results: Vec<Vec<String>>) {
        // Remove oldest entries if cache is full
        if self.results.len() >= self.max_size {
            let oldest_key = self.results.iter()
                .min_by_key(|(_, (_, time))| time)
                .map(|(key, _)| key.clone());
            
            if let Some(key) = oldest_key {
                self.results.remove(&key);
            }
        }
        
        self.results.insert(query, (results, Instant::now()));
    }

    pub fn clear(&mut self) {
        self.results.clear();
    }
}

#[allow(dead_code)]
pub struct ProgressiveLoader {
    batch_size: usize,
    total_rows: usize,
    loaded_rows: usize,
    column_names: Vec<String>,
    current_batch: Vec<Vec<String>>,
}

#[allow(dead_code)]
impl ProgressiveLoader {
    pub fn new(batch_size: usize, column_names: Vec<String>) -> Self {
        Self {
            batch_size,
            total_rows: 0,
            loaded_rows: 0,
            column_names,
            current_batch: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        self.current_batch.push(row);
        self.loaded_rows += 1;
        
        if self.current_batch.len() >= self.batch_size {
            self.flush_batch();
        }
    }

    pub fn flush_batch(&mut self) {
        if !self.current_batch.is_empty() {
            display_as_table(&self.column_names, &self.current_batch);
            println!("Loaded {}/{} rows...", self.loaded_rows, self.total_rows);
            self.current_batch.clear();
        }
    }

    pub fn set_total_rows(&mut self, total: usize) {
        self.total_rows = total;
    }

    pub fn is_complete(&self) -> bool {
        self.loaded_rows >= self.total_rows
    }
}

/// Execute a SQL command and display the results with enhanced formatting and timing
pub fn execute_sql(conn: &Connection, sql: &str, options: &QueryOptions) -> Result<()> {
    let start_time = Instant::now();
    
    // Execute the query
    let mut stmt = conn.prepare(sql)
        .context("Failed to prepare SQL statement")?;
    
    // Check if it's a SELECT query
    let is_select = sql.trim().to_uppercase().starts_with("SELECT");
    
    if is_select {
        // Get column names before executing the query
        let column_names: Vec<String> = stmt.column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        
        let mut rows = stmt.query([])
            .context("Failed to execute SELECT query")?;
        
        // Collect all rows
        let mut all_rows = Vec::new();
        let mut row_count = 0;
        
        while let Some(row) = rows.next()? {
            let mut row_values = Vec::new();
            for i in 0..column_names.len() {
                let value: String = row.get(i)?;
                row_values.push(value);
            }
            all_rows.push(row_values);
            row_count += 1;
            
            if let Some(limit) = options.max_rows {
                if row_count >= limit {
                    break;
                }
            }
        }
        
        // Display results based on format
        if !all_rows.is_empty() {
            match options.format {
                OutputFormat::Table => display_as_table(&column_names, &all_rows),
                OutputFormat::Json => display_as_json(&column_names, &all_rows)?,
                OutputFormat::Csv => display_as_csv(&column_names, &all_rows),
            }
        }
        
        println!("{} row(s) returned", row_count);
        
        if let Some(limit) = options.max_rows {
            if row_count >= limit {
                println!("(Limited to {} rows. Use '.limit 0' to show all rows)", limit);
            }
        }
    } else {
        // For non-SELECT queries
        let affected = stmt.execute([])
            .context("Failed to execute non-SELECT query")?;
        
        println!("{} row(s) affected", affected);
    }
    
    if options.show_timing {
        println!("Query executed in {:.3}ms", start_time.elapsed().as_secs_f64() * 1000.0);
    }
    
    Ok(())
}

fn display_as_table(column_names: &[String], rows: &[Vec<String>]) {
    let mut table = Table::new();
    table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);

    // Add header row
    let mut header_row = prettytable::Row::empty();
    for col_name in column_names {
        header_row.add_cell(prettytable::Cell::new(col_name).style_spec("b"));
    }
    table.add_row(header_row);

    // Add data rows
    for row_values in rows {
        let mut data_row = prettytable::Row::empty();
        for value in row_values {
            data_row.add_cell(prettytable::Cell::new(value));
        }
        table.add_row(data_row);
    }

    table.printstd();
}

fn display_as_json(column_names: &[String], rows: &[Vec<String>]) -> Result<()> {
    let mut json_rows = Vec::new();
    
    for row_values in rows {
        let mut json_row = serde_json::Map::new();
        for (i, value) in row_values.iter().enumerate() {
            let json_value = if value == "NULL" {
                Value::Null
            } else if let Ok(int_val) = value.parse::<i64>() {
                Value::Number(serde_json::Number::from(int_val))
            } else if let Ok(float_val) = value.parse::<f64>() {
                Value::Number(serde_json::Number::from_f64(float_val).unwrap_or_else(|| serde_json::Number::from(0)))
            } else {
                Value::String(value.clone())
            };
            json_row.insert(column_names[i].clone(), json_value);
        }
        json_rows.push(Value::Object(json_row));
    }
    
    let output = json!({
        "data": json_rows,
        "columns": column_names,
        "row_count": rows.len()
    });
    
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn display_as_csv(column_names: &[String], rows: &[Vec<String>]) {
    // Print header
    println!("{}", column_names.join(","));
    
    // Print rows
    for row_values in rows {
        let escaped_values: Vec<String> = row_values.iter()
            .map(|v| {
                if v.contains(',') || v.contains('"') || v.contains('\n') {
                    format!("\"{}\"", v.replace('"', "\"\""))
                } else {
                    v.clone()
                }
            })
            .collect();
        println!("{}", escaped_values.join(","));
    }
}

/// Show the schema for a specific table
pub fn show_table_schema(conn: &Connection, table_name: &str) -> Result<()> {
    // Check if the table exists
    let mut check_stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
        .context("Failed to prepare statement for checking table existence")?;

    let exists: bool = check_stmt.exists(params![table_name])
        .context(format!("Failed to check if table '{}' exists", table_name))?;

    if !exists {
        println!("Table '{}' does not exist.", table_name);
        return Ok(());
    }

    // Get the table schema
    let pragma_sql = format!("PRAGMA table_info({})", table_name);
    let mut stmt = conn.prepare(&pragma_sql)
        .context(format!("Failed to prepare statement for table schema: {}", table_name))?;

    let columns = stmt.query_map(params![], |row| {
        Ok((
            row.get::<_, i32>(0)?, // cid
            row.get::<_, String>(1)?, // name
            row.get::<_, String>(2)?, // type
            row.get::<_, bool>(3)?, // notnull
            row.get::<_, Option<String>>(4)?, // dflt_value
            row.get::<_, i32>(5)?, // pk
        ))
    }).context(format!("Failed to query schema for table: {}", table_name))?;

    // Create a pretty table for display
    let mut table = Table::new();
    table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
    table.add_row(row!["ID", "Name", "Type", "Not Null", "Default Value", "Primary Key"]);

    let mut has_columns = false;
    for column_result in columns {
        has_columns = true;
        let (cid, name, type_name, not_null, default_value, pk) = column_result
            .context(format!("Failed to read column info for table: {}", table_name))?;

        let not_null_str = if not_null { "YES" } else { "NO" };
        let pk_str = if pk > 0 { "YES" } else { "NO" };
        let default_str = default_value.unwrap_or_else(|| "NULL".to_string());

        table.add_row(row![cid, name, type_name, not_null_str, default_str, pk_str]);
    }

    if has_columns {
        println!("Schema for table '{}':", table_name);
        table.printstd();
    } else {
        println!("No columns found for table: {}", table_name);
    }

    Ok(())
}

/// Show the schema for all tables
pub fn show_all_schemas(conn: &Connection) -> Result<()> {
    // Get all table names
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .context("Failed to prepare statement for listing tables")?;

    let table_names = stmt.query_map(params![], |row| row.get::<_, String>(0))
        .context("Failed to query tables")?;

    let mut has_tables = false;
    for (i, table_name_result) in table_names.enumerate() {
        has_tables = true;
        let table_name = table_name_result.context("Failed to read table name")?;
        if i > 0 {
            println!(); 
        }
        show_table_schema(conn, &table_name)?;
    }

    if !has_tables {
        println!("No tables found in the database.");
    }

    Ok(())
}

/// Show database information and statistics
pub fn show_database_info(conn: &Connection, db_path: &str) -> Result<()> {
    println!("Database Information:");
    println!("  Path: {}", db_path);
    
    // Get database file size
    if let Ok(metadata) = std::fs::metadata(db_path) {
        let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
        println!("  Size: {:.2} MB", size_mb);
    }
    
    // Get SQLite version
    let version: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    println!("  SQLite Version: {}", version);
    
    // Get page size and page count
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    println!("  Page Size: {} bytes", page_size);
    println!("  Page Count: {}", page_count);
    
    // Get table statistics
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
    let table_names = stmt.query_map([], |row| row.get::<_, String>(0))?;
    
    println!("\nTable Statistics:");
    let mut total_rows = 0;
    
    for table_name_result in table_names {
        let table_name = table_name_result?;
        let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
        let row_count: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
        println!("  {}: {} rows", table_name, row_count);
        total_rows += row_count;
    }
    
    println!("  Total Rows: {}", total_rows);
    
    Ok(())
} 