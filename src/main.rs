use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::process;

mod bookmarks;
mod db;
mod display;
mod export;
mod populate;
mod repl;
mod transactions;

use db::{init_database, connect_database, create_table, list_tables};
use populate::populate_database;
use repl::repl_mode;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new SQLite database
    Init {
        /// Name of the database file
        #[arg(short, long)]
        name: String,
    },
    /// Connect to an existing SQLite database
    Connect {
        /// Path to the database file
        #[arg(short, long)]
        path: String,
    },
    /// Create a new table in the connected database
    CreateTable {
        /// Path to the database file
        #[arg(short, long)]
        db_path: String,
        /// Name of the table to create
        #[arg(short, long)]
        name: String,
        /// Column definitions in format "name type, name type, ..."
        /// Example: "id INTEGER PRIMARY KEY, name TEXT, age INTEGER"
        #[arg(short, long)]
        columns: String,
    },
    /// List all tables in the connected database
    ListTables {
        /// Path to the database file
        #[arg(short, long)]
        db_path: String,
    },
    /// Start an interactive SQL REPL (Read-Eval-Print Loop)
    Repl {
        /// Path to the database file
        #[arg(short, long)]
        db_path: String,
    },
    /// Populate the database with a large amount of data for testing
    Populate {
        /// Path to the database file
        #[arg(short, long)]
        db_path: String,
    },
}

fn main() {
    // Set up better panic handling
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("Vapor CLI encountered an unexpected error:");
        eprintln!("{}", panic_info);
        eprintln!("This may be due to:");
        eprintln!("   • Corrupted database file");
        eprintln!("   • Insufficient system resources");
        eprintln!("   • Hardware issues");
        eprintln!("Try restarting the application or check your database file integrity.");
    }));

    // Run the main application and handle errors gracefully
    if let Err(error) = run() {
        print_error_with_context(&error);
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init { name } => {
            validate_database_name(name)?;
            init_database(name)
                .with_context(|| format!("Failed to initialize database '{}'", name))?;
        }
        Commands::Connect { path } => {
            validate_database_path(path)?;
            connect_database(path)
                .with_context(|| format!("Failed to connect to database '{}'", path))?;
        }
        Commands::CreateTable { db_path, name, columns } => {
            validate_database_path(db_path)?;
            validate_table_name(name)?;
            validate_column_definition(columns)?;
            create_table(db_path, name, columns)
                .with_context(|| format!("Failed to create table '{}' in database '{}'", name, db_path))?;
        }
        Commands::ListTables { db_path } => {
            validate_database_path(db_path)?;
            list_tables(db_path)
                .with_context(|| format!("Failed to list tables in database '{}'", db_path))?;
        }
        Commands::Repl { db_path } => {
            validate_database_path(db_path)?;
            repl_mode(db_path)
                .with_context(|| format!("REPL session failed for database '{}'", db_path))?;
        }
        Commands::Populate { db_path } => {
            validate_database_path(db_path)?;
            populate_database(db_path, None)
                .with_context(|| format!("Failed to populate database '{}'", db_path))?;
        }
    }

    Ok(())
}

fn validate_database_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        anyhow::bail!("Database name cannot be empty");
    }
    
    if name.contains(|c: char| c.is_control() || "\\/:*?\"<>|".contains(c)) {
        anyhow::bail!("Database name contains invalid characters. Avoid: \\ / : * ? \" < > |");
    }
    
    if name.len() > 255 {
        anyhow::bail!("Database name is too long (maximum 255 characters)");
    }
    
    Ok(())
}

fn validate_database_path(path: &str) -> Result<()> {
    if path.trim().is_empty() {
        anyhow::bail!("Database path cannot be empty");
    }
    
    if path.len() > 1024 {
        anyhow::bail!("Database path is too long (maximum 1024 characters)");
    }
    
    Ok(())
}

fn validate_table_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        anyhow::bail!("Table name cannot be empty");
    }
    
    if !name.chars().next().unwrap_or('0').is_alphabetic() && name.chars().next() != Some('_') {
        anyhow::bail!("Table name must start with a letter or underscore");
    }
    
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!("Table name can only contain letters, numbers, and underscores");
    }
    
    if name.len() > 64 {
        anyhow::bail!("Table name is too long (maximum 64 characters)");
    }
    
    let reserved_words = ["TABLE", "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"];
    if reserved_words.contains(&name.to_uppercase().as_str()) {
        anyhow::bail!("Table name '{}' is a reserved SQL keyword", name);
    }
    
    Ok(())
}

fn validate_column_definition(columns: &str) -> Result<()> {
    if columns.trim().is_empty() {
        anyhow::bail!("Column definition cannot be empty");
    }
    
    // Basic validation - check if it looks like valid column definitions
    if !columns.contains(' ') {
        anyhow::bail!("Column definition must include column types (e.g., 'id INTEGER, name TEXT')");
    }
    
    Ok(())
}

fn print_error_with_context(error: &anyhow::Error) {
    eprintln!("Error: {}", error);
    
    // Print the error chain for better debugging
    let mut current = error.source();
    let mut level = 1;
    while let Some(err) = current {
        eprintln!("   {}. Caused by: {}", level, err);
        current = err.source();
        level += 1;
        if level > 5 { // Prevent infinite loops
            break;
        }
    }
    
    // Provide helpful suggestions based on error type
    let error_msg = error.to_string().to_lowercase();
    
    eprintln!("\nSuggestions:");
    
    if error_msg.contains("no such file") || error_msg.contains("not found") {
        eprintln!("   • Check if the file path is correct");
        eprintln!("   • Make sure the database file exists");
        eprintln!("   • Use 'init' command to create a new database");
    } else if error_msg.contains("permission") || error_msg.contains("access") {
        eprintln!("   • Check file permissions");
        eprintln!("   • Make sure you have read/write access to the directory");
        eprintln!("   • Try running with appropriate permissions");
    } else if error_msg.contains("locked") || error_msg.contains("busy") {
        eprintln!("   • Close other applications using the database");
        eprintln!("   • Wait a moment and try again");
        eprintln!("   • Check if another Vapor CLI instance is running");
    } else if error_msg.contains("syntax") || error_msg.contains("sql") {
        eprintln!("   • Check your SQL syntax");
        eprintln!("   • Use quotes around table/column names with spaces");
        eprintln!("   • Verify column definitions follow SQLite syntax");
    } else if error_msg.contains("disk") || error_msg.contains("space") {
        eprintln!("   • Check available disk space");
        eprintln!("   • Clean up unnecessary files");
        eprintln!("   • Consider using a different location");
    } else {
        eprintln!("   • Check the command syntax with --help");
        eprintln!("   • Verify your input parameters");
        eprintln!("   • Try the operation again");
    }
    
    eprintln!("\nFor more help, use: vapor-cli --help");
}
