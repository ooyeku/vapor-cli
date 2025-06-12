//! # Data Import and Export
//!
//! This module provides functionality for importing data into and exporting data from the
//! SQLite database. It currently focuses on the CSV format, which is a common and
//! versatile format for data interchange.
//!
//! ## Key Functions:
//! - `import_csv_to_table`: Imports data from a CSV file into a specified database table.
//! - `export_to_csv`: Exports the results of a SQL query to a CSV file.
//!
//! The module includes robust error handling, input validation, and progress indicators
//! for long-running operations to ensure a reliable user experience.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

/// Imports data from a CSV file into a specified database table.
///
/// This function reads a CSV file, using the header row to map columns to the
/// corresponding columns in the target table. The entire import process is wrapped
/// in a single database transaction to ensure atomicity.
///
/// # Arguments
///
/// * `conn` - A mutable reference to the active `rusqlite::Connection`.
/// * `file_path` - The path to the CSV file to be imported.
/// * `table_name` - The name of the database table to insert data into.
///
/// # Returns
///
/// A `Result` which is `Ok(())` on successful import, or an `Err` if the file cannot
/// be read, the CSV is malformed, or the database insertion fails.
pub fn import_csv_to_table(conn: &mut Connection, file_path: &str, table_name: &str) -> Result<()> {
    let file = Path::new(file_path);
    if !file.exists() {
        anyhow::bail!("File not found: {}", file_path);
    }

    let mut rdr = csv::Reader::from_path(file_path)?;
    let headers = rdr.headers()?.clone();

    let tx = conn.transaction()?;

    {
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            headers
                .iter()
                .map(|h| format!("\"{}\"", h))
                .collect::<Vec<_>>()
                .join(","),
            headers.iter().map(|_| "?").collect::<Vec<_>>().join(",")
        );

        let mut stmt = tx.prepare(&sql)?;

        for result in rdr.records() {
            let record = result?;
            let params: Vec<&str> = record.iter().collect();
            stmt.execute(rusqlite::params_from_iter(params))?;
        }
    } // stmt is dropped here

    tx.commit()?;
    Ok(())
}

/// Exports the results of a SQL query to a CSV file.
///
/// This function executes a given `SELECT` query and writes the entire result set to a
/// specified CSV file. It includes comprehensive validation of inputs, progress updates
/// for large exports, and robust error handling during file writing.
///
/// # Arguments
///
/// * `conn` - A reference to the active `rusqlite::Connection`.
/// * `query` - The `SELECT` SQL query whose results will be exported.
/// * `filename` - The path to the output CSV file. The file will be overwritten if it exists.
///
/// # Returns
///
/// A `Result` which is `Ok(())` on successful export, or an `Err` if the query is invalid,
/// the file cannot be written, or other errors occur during the process.
pub fn export_to_csv(conn: &Connection, query: &str, filename: &str) -> Result<()> {
    // Validate inputs
    validate_export_inputs(query, filename)?;

    // Prepare the statement with error handling
    let mut stmt = conn.prepare(query).with_context(|| {
        format!(
            "Failed to prepare export query. Check SQL syntax: {}",
            query
        )
    })?;

    let column_names: Vec<String> = stmt.column_names().iter().map(|&s| s.to_string()).collect();

    if column_names.is_empty() {
        anyhow::bail!(
            "Query returned no columns. Make sure your query includes SELECT statements."
        );
    }

    // Create the CSV writer with error handling
    let mut wtr = csv::Writer::from_path(filename).with_context(|| {
        format!(
            "Failed to create CSV file '{}'. Check permissions and disk space.",
            filename
        )
    })?;

    // Write header row
    wtr.write_record(&column_names)
        .with_context(|| format!("Failed to write CSV header to '{}'", filename))?;

    // Execute query and write rows with progress tracking
    let mut rows = stmt
        .query([])
        .with_context(|| format!("Failed to execute export query: {}", query))?;

    let mut row_count = 0;
    let mut error_count = 0;

    while let Some(row) = rows
        .next()
        .with_context(|| format!("Failed to fetch row {} from query results", row_count + 1))?
    {
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
                    anyhow::bail!(
                        "Too many processing errors ({}). Stopping export.",
                        error_count
                    );
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

/// Helper function to validate the inputs for the `export_to_csv` function.
///
/// Performs checks for:
/// - Non-empty query and filename.
/// - Presence of a `SELECT` statement in the query.
/// - Warnings for potentially destructive keywords (e.g., `DROP`, `DELETE`).
/// - Invalid characters in the filename.
/// - Existence of the output directory.
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
            eprintln!(
                "Warning: Query contains '{}' - this may modify data",
                keyword
            );
        }
    }

    // Validate filename
    if filename.trim().is_empty() {
        anyhow::bail!("Filename cannot be empty");
    }

    if filename.len() > 255 {
        anyhow::bail!("Filename is too long (maximum 255 characters)");
    }

    // Check for invalid filename characters (but allow path separators)
    let path = Path::new(filename);
    if let Some(file_name) = path.file_name() {
        let name_str = file_name.to_string_lossy();
        // Only check the actual filename part, not the full path
        if name_str
            .chars()
            .any(|c| c.is_control() || "\\:*?\"<>|".contains(c))
        {
            anyhow::bail!("Filename contains invalid characters. Avoid: \\ : * ? \" < > |");
        }
    }

    // Check if file already exists and warn user
    if Path::new(filename).exists() {
        eprintln!(
            "Warning: File '{}' already exists and will be overwritten",
            filename
        );
    }

    // Check if the directory is writable
    if let Some(parent) = Path::new(filename).parent() {
        if parent != Path::new("") && !parent.exists() {
            anyhow::bail!(
                "Directory '{}' does not exist. Create it first or use a different path.",
                parent.display()
            );
        }
    }

    Ok(())
}

/// Helper function to process a single database row into a vector of strings for CSV writing.
///
/// Handles the conversion of different SQLite data types (`Null`, `Integer`, `Real`, `Text`, `Blob`)
/// into their string representations. It also escapes text fields as needed for the CSV format.
fn process_row(row: &rusqlite::Row, column_names: &[String]) -> Result<Vec<String>> {
    let mut record = Vec::with_capacity(column_names.len());

    for i in 0..column_names.len() {
        let val: rusqlite::types::Value = row.get(i).with_context(|| {
            format!(
                "Failed to get value from column {} ('{}')",
                i, column_names[i]
            )
        })?;

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
            }
            rusqlite::types::Value::Text(t) => {
                // Escape CSV special characters if needed
                if t.contains(',') || t.contains('"') || t.contains('\n') {
                    format!("\"{}\"", t.replace('"', "\"\""))
                } else {
                    t
                }
            }
            rusqlite::types::Value::Blob(b) => {
                // For binary data, provide a more informative representation
                format!("[BLOB {} bytes]", b.len())
            }
        };

        record.push(value_str);
    }

    Ok(record)
}

/// Helper function to verify that the export file was created and appears valid.
///
/// Checks if the file exists and if its size is non-zero when rows were expected to be written.
fn verify_export_file(filename: &str, expected_rows: usize) -> Result<()> {
    let path = Path::new(filename);

    if !path.exists() {
        anyhow::bail!("Export file '{}' was not created", filename);
    }

    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Cannot read metadata for export file '{}'", filename))?;

    if metadata.len() == 0 && expected_rows > 0 {
        anyhow::bail!(
            "Export file '{}' is empty but {} rows were expected",
            filename,
            expected_rows
        );
    }

    // Basic file size sanity check
    let file_size = metadata.len();
    if file_size > 0 {
        println!("Export file size: {} bytes", file_size);
    }

    Ok(())
}
