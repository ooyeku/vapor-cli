use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

/// Export query results to CSV with comprehensive error handling and validation
pub fn export_to_csv(conn: &Connection, query: &str, filename: &str) -> Result<()> {
    // Validate inputs
    validate_export_inputs(query, filename)?;
    
    // Prepare the statement with error handling
    let mut stmt = conn.prepare(query)
        .with_context(|| format!("Failed to prepare export query. Check SQL syntax: {}", query))?;
        
    let column_names: Vec<String> = stmt.column_names()
        .iter()
        .map(|&s| s.to_string())
        .collect();

    if column_names.is_empty() {
        anyhow::bail!("Query returned no columns. Make sure your query includes SELECT statements.");
    }

    // Create the CSV writer with error handling
    let mut wtr = csv::Writer::from_path(filename)
        .with_context(|| format!("Failed to create CSV file '{}'. Check permissions and disk space.", filename))?;

    // Write header row
    wtr.write_record(&column_names)
        .with_context(|| format!("Failed to write CSV header to '{}'", filename))?;

    // Execute query and write rows with progress tracking
    let mut rows = stmt.query([])
        .with_context(|| format!("Failed to execute export query: {}", query))?;
        
    let mut row_count = 0;
    let mut error_count = 0;
    
    while let Some(row) = rows.next()
        .with_context(|| format!("Failed to fetch row {} from query results", row_count + 1))? {
        
        match process_row(&row, &column_names) {
            Ok(record) => {
                if let Err(e) = wtr.write_record(&record) {
                    error_count += 1;
                    eprintln!("Warning: Failed to write row {}: {}", row_count + 1, e);
                    
                    if error_count > 10 {
                        anyhow::bail!("Too many write errors ({}). Stopping export.", error_count);
                    }
                } else {
                    row_count += 1;
                    
                    // Progress indicator for large exports
                    if row_count % 10000 == 0 {
                        println!("Exported {} rows...", row_count);
                    }
                }
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Warning: Failed to process row {}: {}", row_count + 1, e);
                
                if error_count > 10 {
                    anyhow::bail!("Too many processing errors ({}). Stopping export.", error_count);
                }
            }
        }
    }

    // Ensure all data is written to disk
    wtr.flush()
        .with_context(|| format!("Failed to flush data to CSV file '{}'", filename))?;

    // Verify the file was created successfully
    verify_export_file(filename, row_count)?;

    if error_count > 0 {
        println!("Export completed with {} warning(s)", error_count);
    }
    
    println!("Successfully exported {} rows to '{}'", row_count, filename);
    
    Ok(())
}

fn validate_export_inputs(query: &str, filename: &str) -> Result<()> {
    // Validate query
    if query.trim().is_empty() {
        anyhow::bail!("Export query cannot be empty");
    }
    
    let query_lower = query.to_lowercase();
    if !query_lower.contains("select") {
        anyhow::bail!("Export query must contain a SELECT statement");
    }
    
    // Check for potentially dangerous operations
    let dangerous_keywords = ["drop", "delete", "update", "insert", "create", "alter"];
    for keyword in &dangerous_keywords {
        if query_lower.contains(keyword) {
            eprintln!("Warning: Query contains '{}' - this may modify data", keyword);
        }
    }
    
    // Validate filename
    if filename.trim().is_empty() {
        anyhow::bail!("Filename cannot be empty");
    }
    
    if filename.len() > 255 {
        anyhow::bail!("Filename is too long (maximum 255 characters)");
    }
    
    // Check for invalid filename characters
    if filename.contains(|c: char| c.is_control() || "\\/:*?\"<>|".contains(c)) {
        anyhow::bail!("Filename contains invalid characters. Avoid: \\ / : * ? \" < > |");
    }
    
    // Check if file already exists and warn user
    if Path::new(filename).exists() {
        eprintln!("Warning: File '{}' already exists and will be overwritten", filename);
    }
    
    // Check if the directory is writable
    if let Some(parent) = Path::new(filename).parent() {
        if parent != Path::new("") && !parent.exists() {
            anyhow::bail!("Directory '{}' does not exist. Create it first or use a different path.", parent.display());
        }
    }
    
    Ok(())
}

fn process_row(row: &rusqlite::Row, column_names: &[String]) -> Result<Vec<String>> {
    let mut record = Vec::with_capacity(column_names.len());
    
    for i in 0..column_names.len() {
        let val: rusqlite::types::Value = row.get(i)
            .with_context(|| format!("Failed to get value from column {} ('{}')", i, column_names[i]))?;
            
        let value_str = match val {
            rusqlite::types::Value::Null => String::new(),
            rusqlite::types::Value::Integer(i) => i.to_string(),
            rusqlite::types::Value::Real(f) => {
                // Handle special float values
                if f.is_nan() {
                    "NaN".to_string()
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        "Infinity".to_string()
                    } else {
                        "-Infinity".to_string()
                    }
                } else {
                    f.to_string()
                }
            },
            rusqlite::types::Value::Text(t) => {
                // Escape CSV special characters if needed
                if t.contains(',') || t.contains('"') || t.contains('\n') {
                    format!("\"{}\"", t.replace('"', "\"\""))
                } else {
                    t
                }
            },
            rusqlite::types::Value::Blob(b) => {
                // For binary data, provide a more informative representation
                format!("[BLOB {} bytes]", b.len())
            },
        };
        
        record.push(value_str);
    }
    
    Ok(record)
}

fn verify_export_file(filename: &str, expected_rows: usize) -> Result<()> {
    let path = Path::new(filename);
    
    if !path.exists() {
        anyhow::bail!("Export file '{}' was not created", filename);
    }
    
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Cannot read metadata for export file '{}'", filename))?;
    
    if metadata.len() == 0 && expected_rows > 0 {
        anyhow::bail!("Export file '{}' is empty but {} rows were expected", filename, expected_rows);
    }
    
    // Basic file size sanity check
    let file_size = metadata.len();
    if file_size > 0 {
        println!("Export file size: {} bytes", file_size);
    }
    
    Ok(())
} 