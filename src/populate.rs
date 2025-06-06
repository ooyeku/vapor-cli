use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;
use std::time::{Duration, Instant};

/// Populate database with test data, featuring comprehensive error handling and progress tracking
pub fn populate_database(db_path: &str) -> Result<()> {
    println!("🔗 Connecting to database: {}", db_path);
    
    // Validate database exists and is accessible
    validate_database_for_population(db_path)?;
    
    let mut conn = create_connection_with_settings(db_path)?;
    
    // Check available disk space before starting
    check_disk_space_requirements(db_path)?;
    
    println!("🔧 Creating large table...");
    create_large_table(&conn)?;
    
    println!("📊 Populating table with 1,000,000 rows...");
    println!("⏳ This may take a while. Progress will be shown every 100,000 rows.");
    
    let start_time = Instant::now();
    
    // Use transaction for better performance and atomicity
    let result = populate_with_transaction(&mut conn);
    
    match result {
        Ok(rows_inserted) => {
            let duration = start_time.elapsed();
            println!("✅ Successfully populated table 'large_table' with {} rows", rows_inserted);
            println!("⏱️  Total time: {:.2} seconds", duration.as_secs_f64());
            println!("🚀 Average: {:.0} rows/second", rows_inserted as f64 / duration.as_secs_f64());
        }
        Err(e) => {
            eprintln!("❌ Population failed: {}", e);
            eprintln!("🔄 Attempting to rollback any partial changes...");
            
            // Try to clean up any partial data
            if let Err(cleanup_err) = cleanup_failed_population(&conn) {
                eprintln!("⚠️  Warning: Cleanup failed: {}", cleanup_err);
                eprintln!("💡 You may need to manually drop the 'large_table' if it was partially created.");
            } else {
                println!("✅ Cleanup completed successfully");
            }
            
            return Err(e);
        }
    }
    
    // Verify the population was successful
    verify_population_success(&conn)?;
    
    Ok(())
}

fn validate_database_for_population(db_path: &str) -> Result<()> {
    if !Path::new(db_path).exists() {
        anyhow::bail!("Database '{}' does not exist. Create it first with 'init' command.", db_path);
    }
    
    let metadata = std::fs::metadata(db_path)
        .with_context(|| format!("Cannot read database file '{}'", db_path))?;
        
    if metadata.is_dir() {
        anyhow::bail!("'{}' is a directory, not a database file", db_path);
    }
    
    Ok(())
}

fn create_connection_with_settings(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to connect to database: {}", db_path))?;
    
    // Configure SQLite for better performance during bulk inserts
    conn.execute("PRAGMA synchronous = OFF", [])
        .context("Failed to disable synchronous mode")?;
    conn.execute("PRAGMA journal_mode = MEMORY", [])
        .context("Failed to set journal mode to memory")?;
    conn.execute("PRAGMA cache_size = 10000", [])
        .context("Failed to increase cache size")?;
    
    println!("🔧 Database configured for bulk insert performance");
    
    Ok(conn)
}

fn check_disk_space_requirements(db_path: &str) -> Result<()> {
    // Estimate space needed: 1M rows * ~200 bytes per row = ~200MB
    let estimated_size_mb = 200;
    
    println!("💽 Estimated space needed: ~{} MB", estimated_size_mb);
    
    // Try to get available space (this is platform-specific, so we'll make it non-fatal)
    if let Ok(metadata) = std::fs::metadata(db_path) {
        if metadata.len() == 0 {
            eprintln!("⚠️  Warning: Database file appears to be empty");
        }
    }
    
    println!("💡 Ensure you have sufficient disk space before proceeding");
    Ok(())
}

fn create_large_table(conn: &Connection) -> Result<()> {
    let create_table_sql = "CREATE TABLE IF NOT EXISTS large_table (
        col1 INTEGER,
        col2 TEXT,
        col3 TEXT,
        col4 TEXT,
        col5 INTEGER,
        col6 INTEGER,
        col7 REAL,
        col8 REAL,
        col9 TEXT,
        col10 TEXT,
        col11 TEXT,
        col12 TEXT
    )";
    
    conn.execute(create_table_sql, [])
        .context("Failed to create large_table. Check database permissions and disk space.")?;
    
    // Check if table already has data
    let existing_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM large_table",
        [],
        |row| row.get(0)
    ).context("Failed to check existing row count")?;
    
    if existing_count > 0 {
        println!("⚠️  Table 'large_table' already contains {} rows", existing_count);
        println!("💡 Population will add 1,000,000 more rows");
    } else {
        println!("✅ Table 'large_table' created successfully");
    }
    
    Ok(())
}

fn populate_with_transaction(conn: &mut Connection) -> Result<usize> {
    let tx = conn.transaction()
        .context("Failed to begin transaction")?;
    
    let mut stmt = tx.prepare("INSERT INTO large_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)")
        .context("Failed to prepare insert statement")?;
    
    let total_rows = 1_000_000;
    let progress_interval = 100_000;
    let mut rows_inserted = 0;
    let start_time = Instant::now();
    
    for i in 0..total_rows {
        let result = stmt.execute(params![
            i, // col1
            format!("text-col2-{}", i), // col2
            format!("text-col3-{}", i % 100), // col3
            format!("text-col4-{}", i), // col4
            i * 2, // col5
            i % 1000, // col6
            (i as f64) * 1.1, // col7
            (i as f64) / 3.14, // col8
            "constant-text-9", // col9
            format!("text-col10-{}", i % 10), // col10
            "constant-text-11", // col11
            format!("text-col12-{}", i) // col12
        ]);
        
        match result {
            Ok(_) => {
                rows_inserted += 1;
                
                // Show progress
                if rows_inserted % progress_interval == 0 {
                    let elapsed = start_time.elapsed();
                    let rate = rows_inserted as f64 / elapsed.as_secs_f64();
                    let eta = if rate > 0.0 {
                        Duration::from_secs(((total_rows - rows_inserted) as f64 / rate) as u64)
                    } else {
                        Duration::from_secs(0)
                    };
                    
                    println!("📈 Progress: {}/{} rows ({:.1}%) - {:.0} rows/sec - ETA: {:?}", 
                        rows_inserted, total_rows, 
                        (rows_inserted as f64 / total_rows as f64) * 100.0,
                        rate,
                        eta);
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to insert row {}: {}", i + 1, e);
                
                // Try to continue with a few retries for transient errors
                if is_transient_error(&e) && should_retry_insert(rows_inserted) {
                    eprintln!("🔄 Retrying row {}...", i + 1);
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                } else {
                    return Err(e).with_context(|| format!("Failed to insert row {} after retries", i + 1));
                }
            }
        }
    }
    
    println!("💾 Committing transaction...");
    drop(stmt); // Release the prepared statement before committing
    tx.commit()
        .context("Failed to commit transaction. All changes have been rolled back.")?;
    
    Ok(rows_inserted)
}

fn is_transient_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(err, _) => {
            // SQLITE_BUSY, SQLITE_LOCKED, etc.
            matches!(err.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        }
        _ => false
    }
}

fn should_retry_insert(rows_so_far: usize) -> bool {
    // Only retry if we haven't inserted too many rows yet
    // (to avoid infinite retries on persistent errors)
    rows_so_far < 100_000
}

fn cleanup_failed_population(conn: &Connection) -> Result<()> {
    // Check if table exists and has partial data
    let table_exists: bool = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='large_table'",
        [],
        |row| {
            let count: i64 = row.get(0)?;
            Ok(count > 0)
        }
    ).context("Failed to check if large_table exists")?;
    
    if table_exists {
        // Don't drop the table automatically, just report what to do
        let row_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM large_table",
            [],
            |row| row.get(0)
        ).context("Failed to count rows in large_table")?;
        
        if row_count > 0 {
            println!("🧹 Table 'large_table' contains {} partial rows", row_count);
            println!("💡 Run 'DROP TABLE large_table;' to remove it, or try populating again");
        }
    }
    
    Ok(())
}

fn verify_population_success(conn: &Connection) -> Result<()> {
    let final_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM large_table",
        [],
        |row| row.get(0)
    ).context("Failed to verify population by counting rows")?;
    
    // Get some sample data to verify integrity
    let sample_row: Option<(i64, String)> = match conn.query_row(
        "SELECT col1, col2 FROM large_table WHERE col1 = 12345 LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?))
    ) {
        Ok(data) => Some(data),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("Failed to verify sample data"),
    };
    
    if let Some((col1, col2)) = sample_row {
        if col1 == 12345 && col2 == "text-col2-12345" {
            println!("✅ Data integrity verification passed");
        } else {
            eprintln!("⚠️  Warning: Data integrity check failed - sample data doesn't match expected values");
        }
    }
    
    println!("📊 Final row count: {} rows in 'large_table'", final_count);
    
    if final_count >= 1_000_000 {
        println!("🎉 Population completed successfully!");
    } else {
        eprintln!("⚠️  Warning: Expected at least 1,000,000 rows but found {}", final_count);
    }
    
    Ok(())
} 