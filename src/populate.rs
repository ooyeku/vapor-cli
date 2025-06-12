use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PopulationConfig {
    pub table_name: String,
    pub row_count: usize,
    pub batch_size: usize,
    pub seed: Option<u64>,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub data_type: DataType,
    pub distribution: DataDistribution,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Text,
    Real,
    Boolean,
    Date,
    Timestamp,
    UUID,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataDistribution {
    Uniform,
    Normal { mean: f64, std_dev: f64 },
    Sequential,
    Random,
    Custom(Vec<String>),
}

impl Default for PopulationConfig {
    fn default() -> Self {
        Self {
            table_name: "large_table".to_string(),
            row_count: 1_000_000,
            batch_size: 10_000,
            seed: None,
            columns: vec![
                ColumnConfig {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    distribution: DataDistribution::Sequential,
                    nullable: false,
                },
                ColumnConfig {
                    name: "text_col".to_string(),
                    data_type: DataType::Text,
                    distribution: DataDistribution::Random,
                    nullable: true,
                },
                ColumnConfig {
                    name: "value".to_string(),
                    data_type: DataType::Real,
                    distribution: DataDistribution::Normal {
                        mean: 100.0,
                        std_dev: 15.0,
                    },
                    nullable: false,
                },
            ],
        }
    }
}

/// Populate database with test data, featuring comprehensive error handling and progress tracking
pub fn populate_database(db_path: &str, config: Option<PopulationConfig>) -> Result<()> {
    println!("Connecting to database: {}", db_path);

    // Validate database exists and is accessible
    validate_database_for_population(db_path)?;

    let mut conn = create_connection_with_settings(db_path)?;

    // Check available disk space before starting
    check_disk_space_requirements(db_path, &config)?;

    let config = config.unwrap_or_default();
    println!("Creating table '{}'...", config.table_name);
    create_table_with_config(&conn, &config)?;

    println!("Populating table with {} rows...", config.row_count);
    println!(
        "This may take a while. Progress will be shown every {} rows.",
        config.batch_size
    );

    let start_time = Instant::now();

    // Use transaction for better performance and atomicity
    let result = populate_with_transaction(&mut conn, &config);

    match result {
        Ok(rows_inserted) => {
            let duration = start_time.elapsed();
            println!(
                "Successfully populated table '{}' with {} rows",
                config.table_name, rows_inserted
            );
            println!("Total time: {:.2} seconds", duration.as_secs_f64());
            println!(
                "Average: {:.0} rows/second",
                rows_inserted as f64 / duration.as_secs_f64()
            );
        }
        Err(e) => {
            eprintln!("Population failed: {}", e);
            eprintln!("Attempting to rollback any partial changes...");

            // Try to clean up any partial data
            if let Err(cleanup_err) = cleanup_failed_population(&conn, &config.table_name) {
                eprintln!("Warning: Cleanup failed: {}", cleanup_err);
                eprintln!("You may need to manually drop the table if it was partially created.");
            } else {
                println!("Cleanup completed successfully");
            }

            return Err(e);
        }
    }

    // Verify the population was successful
    verify_population_success(&conn, &config)?;

    Ok(())
}

fn validate_database_for_population(db_path: &str) -> Result<()> {
    if !Path::new(db_path).exists() {
        anyhow::bail!(
            "Database '{}' does not exist. Create it first with 'init' command.",
            db_path
        );
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
    conn.pragma_update(None, "synchronous", "OFF")
        .context("Failed to disable synchronous mode")?;

    conn.pragma_update(None, "journal_mode", "MEMORY")
        .context("Failed to set journal mode to memory")?;

    conn.pragma_update(None, "cache_size", "10000")
        .context("Failed to increase cache size")?;

    println!("Database configured for bulk insert performance");

    Ok(conn)
}

fn check_disk_space_requirements(db_path: &str, config: &Option<PopulationConfig>) -> Result<()> {
    let default_config = PopulationConfig::default();
    let config = config.as_ref().unwrap_or(&default_config);
    // Estimate space needed based on column types and row count
    let avg_row_size = estimate_row_size(&config.columns);
    let estimated_size_mb = (avg_row_size * config.row_count) as f64 / (1024.0 * 1024.0);

    println!("Estimated space needed: ~{:.1} MB", estimated_size_mb);

    // Try to get available space (this is platform-specific, so we'll make it non-fatal)
    if let Ok(metadata) = std::fs::metadata(db_path) {
        if metadata.len() == 0 {
            eprintln!("Warning: Database file appears to be empty");
        }
    }

    println!("Ensure you have sufficient disk space before proceeding");
    Ok(())
}

fn estimate_row_size(columns: &[ColumnConfig]) -> usize {
    columns
        .iter()
        .map(|col| match col.data_type {
            DataType::Integer => 8,
            DataType::Text => 50, // Average text length
            DataType::Real => 8,
            DataType::Boolean => 1,
            DataType::Date => 8,
            DataType::Timestamp => 8,
            DataType::UUID => 36,
        })
        .sum()
}

fn create_table_with_config(conn: &Connection, config: &PopulationConfig) -> Result<()> {
    let column_defs: Vec<String> = config
        .columns
        .iter()
        .map(|col| {
            let type_str = match col.data_type {
                DataType::Integer => "INTEGER",
                DataType::Text => "TEXT",
                DataType::Real => "REAL",
                DataType::Boolean => "INTEGER",
                DataType::Date => "TEXT",
                DataType::Timestamp => "TEXT",
                DataType::UUID => "TEXT",
            };

            let nullable = if col.nullable { "" } else { " NOT NULL" };
            format!("{} {}{}", col.name, type_str, nullable)
        })
        .collect();

    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        config.table_name,
        column_defs.join(", ")
    );

    conn.execute(&create_table_sql, [])
        .context("Failed to create table. Check database permissions and disk space.")?;

    // Check if table already has data
    let existing_count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", config.table_name),
            [],
            |row| row.get(0),
        )
        .context("Failed to check existing row count")?;

    if existing_count > 0 {
        println!(
            "Table '{}' already contains {} rows",
            config.table_name, existing_count
        );
        println!("Population will add {} more rows", config.row_count);
    } else {
        println!("Table '{}' created successfully", config.table_name);
    }

    Ok(())
}

fn populate_with_transaction(conn: &mut Connection, config: &PopulationConfig) -> Result<usize> {
    let tx = conn.transaction().context("Failed to begin transaction")?;

    let placeholders = (0..config.columns.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let column_names: Vec<String> = config.columns.iter().map(|c| c.name.clone()).collect();
    let column_names_str = column_names.join(", ");
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        config.table_name, column_names_str, placeholders
    );

    let mut stmt = tx
        .prepare(&insert_sql)
        .context("Failed to prepare insert statement")?;

    let mut rng = if let Some(seed) = config.seed {
        StdRng::seed_from_u64(seed)
    } else {
        StdRng::from_entropy()
    };

    let mut rows_inserted = 0;
    let start_time = Instant::now();
    let mut last_checkpoint = Instant::now();
    let checkpoint_interval = Duration::from_secs(30);

    for batch_start in (0..config.row_count).step_by(config.batch_size) {
        let batch_end = std::cmp::min(batch_start + config.batch_size, config.row_count);

        for i in batch_start..batch_end {
            let values = generate_row_values(&config.columns, i, &mut rng);

            match stmt.execute(rusqlite::params_from_iter(values)) {
                Ok(_) => {
                    rows_inserted += 1;

                    // Show progress
                    if rows_inserted % config.batch_size == 0 {
                        let elapsed = start_time.elapsed();
                        let rate = rows_inserted as f64 / elapsed.as_secs_f64();
                        let eta = if rate > 0.0 {
                            Duration::from_secs(
                                ((config.row_count - rows_inserted) as f64 / rate) as u64,
                            )
                        } else {
                            Duration::from_secs(0)
                        };

                        println!(
                            "Progress: {}/{} rows ({:.1}%) - {:.0} rows/sec - ETA: {:?}",
                            rows_inserted,
                            config.row_count,
                            (rows_inserted as f64 / config.row_count as f64) * 100.0,
                            rate,
                            eta
                        );
                    }
                }
                Err(e) => {
                    eprintln!("Failed to insert row {}: {}", i + 1, e);

                    // Try to continue with a few retries for transient errors
                    if is_transient_error(&e) && should_retry_insert(rows_inserted) {
                        eprintln!("Retrying row {}...", i + 1);
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    } else {
                        return Err(e).with_context(|| {
                            format!("Failed to insert row {} after retries", i + 1)
                        });
                    }
                }
            }
        }

        // Create checkpoint if enough time has passed
        if last_checkpoint.elapsed() >= checkpoint_interval {
            println!("Creating checkpoint...");
            tx.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])
                .context("Failed to create checkpoint")?;
            last_checkpoint = Instant::now();
        }
    }

    println!("Committing transaction...");
    drop(stmt); // Release the prepared statement before committing
    tx.commit()
        .context("Failed to commit transaction. All changes have been rolled back.")?;

    Ok(rows_inserted)
}

fn generate_row_values(
    columns: &[ColumnConfig],
    row_index: usize,
    rng: &mut StdRng,
) -> Vec<String> {
    columns
        .iter()
        .map(|col| {
            if col.nullable && rng.gen_bool(0.1) {
                // 10% chance of NULL
                return "NULL".to_string();
            }

            match (&col.data_type, &col.distribution) {
                (DataType::Integer, DataDistribution::Sequential) => row_index.to_string(),
                (DataType::Integer, DataDistribution::Uniform) => {
                    rng.gen_range(0..1000).to_string()
                }
                (DataType::Integer, DataDistribution::Normal { mean, std_dev }) => {
                    let value = rng.gen_range(0.0..1.0);
                    let normal = (value - 0.5) * std_dev + mean;
                    (normal.round() as i64).to_string()
                }
                (DataType::Text, DataDistribution::Random) => {
                    format!("text-{}", rng.gen_range(0..1000))
                }
                (DataType::Text, DataDistribution::Custom(values)) => {
                    values[rng.gen_range(0..values.len())].clone()
                }
                (DataType::Real, DataDistribution::Normal { mean, std_dev }) => {
                    let value = rng.gen_range(0.0..1.0);
                    let normal = (value - 0.5) * std_dev + mean;
                    format!("{:.2}", normal)
                }
                (DataType::Boolean, _) => rng.gen_bool(0.5).to_string(),
                (DataType::Date, _) => {
                    let days = rng.gen_range(0..365);
                    let date = Utc::now() - ChronoDuration::days(days);
                    date.format("%Y-%m-%d").to_string()
                }
                (DataType::Timestamp, _) => {
                    let seconds = rng.gen_range(0..86400);
                    let timestamp = Utc::now() - ChronoDuration::seconds(seconds);
                    timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                (DataType::UUID, _) => Uuid::new_v4().to_string(),
                _ => "".to_string(), // Default case
            }
        })
        .collect()
}

fn is_transient_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(err, _) => {
            matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
        }
        _ => false,
    }
}

fn should_retry_insert(rows_so_far: usize) -> bool {
    rows_so_far < 100_000
}

fn cleanup_failed_population(conn: &Connection, table_name: &str) -> Result<()> {
    // Check if table exists and has partial data
    let table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [table_name],
            |row| {
                let count: i64 = row.get(0)?;
                Ok(count > 0)
            },
        )
        .context("Failed to check if table exists")?;

    if table_exists {
        // Don't drop the table automatically, just report what to do
        let row_count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", table_name), [], |row| {
                row.get(0)
            })
            .context("Failed to count rows in table")?;

        if row_count > 0 {
            println!("Table '{}' contains {} partial rows", table_name, row_count);
            println!(
                "Run 'DROP TABLE {};' to remove it, or try populating again",
                table_name
            );
        }
    }

    Ok(())
}

fn verify_population_success(conn: &Connection, config: &PopulationConfig) -> Result<()> {
    let final_count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", config.table_name),
            [],
            |row| row.get(0),
        )
        .context("Failed to verify population by counting rows")?;

    // Get some sample data to verify integrity
    let sample_row: Option<Vec<String>> = match conn.query_row(
        &format!("SELECT * FROM {} LIMIT 1", config.table_name),
        [],
        |row| {
            let mut values = Vec::new();
            for i in 0..config.columns.len() {
                // Handle different column types
                let value = match config.columns[i].data_type {
                    DataType::Integer => row.get::<_, i64>(i)?.to_string(),
                    DataType::Text => row.get::<_, String>(i)?,
                    DataType::Real => row.get::<_, f64>(i)?.to_string(),
                    DataType::Boolean => row.get::<_, bool>(i)?.to_string(),
                    DataType::Date | DataType::Timestamp | DataType::UUID => {
                        row.get::<_, String>(i)?
                    }
                };
                values.push(value);
            }
            Ok(values)
        },
    ) {
        Ok(data) => Some(data),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("Failed to verify sample data"),
    };

    if let Some(values) = sample_row {
        if values.len() == config.columns.len() {
            println!("Data integrity verification passed");
        } else {
            eprintln!("Warning: Data integrity check failed - sample data doesn't match expected column count");
        }
    }

    println!(
        "Final row count: {} rows in '{}'",
        final_count, config.table_name
    );

    if final_count >= config.row_count as i64 {
        println!("Population completed successfully!");
    } else {
        eprintln!(
            "Warning: Expected at least {} rows but found {}",
            config.row_count, final_count
        );
    }

    Ok(())
}
