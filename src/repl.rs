use anyhow::{Context, Result};
use atty::Stream;
use rusqlite::Connection;
use rustyline::DefaultEditor;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use crate::shell::Shell;

use crate::bookmarks::BookmarkManager;
use crate::db::list_tables;
use crate::display::{execute_sql, show_table_schema, show_all_schemas, show_database_info, OutputFormat, QueryOptions};
use crate::export::{export_to_csv, import_csv_to_table};
use crate::transactions::TransactionManager;

/// Start an interactive SQL REPL (Read-Eval-Print Loop) with enhanced error handling
pub fn repl_mode(db_path: &str) -> Result<()> {
    // Convert to absolute path
    let db_path = std::fs::canonicalize(db_path)
        .with_context(|| format!("Failed to resolve absolute path for database '{}'", db_path))?
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Database path contains invalid UTF-8 characters"))?
        .to_string();

    // Validate database exists and is accessible
    if !Path::new(&db_path).exists() {
        anyhow::bail!(
            "Database '{}' does not exist. Use 'vapor-cli init --name {}' to create it.",
            db_path,
            db_path.trim_end_matches(".db")
        );
    }

    // Verify database integrity before starting REPL
    verify_database_file(&db_path)?;

    // Connect to the database with retry logic
    let mut conn = create_robust_connection(&db_path)?;

    // Handle non-interactive mode (piped input)
    if !atty::is(Stream::Stdin) {
        return handle_non_interactive_mode(&conn);
    }

    println!("Connected to database: {}", db_path);
    println!("REPL with timing, bookmarks, and transaction support");
    print_help_summary();

    // Initialize REPL components with error handling
    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("Warning: Could not initialize readline editor: {}", e);
            eprintln!("   Falling back to basic input mode.");
            return handle_basic_repl_mode(&conn);
        }
    };

    // Load command history if available
    let history_path = Path::new(".vapor_history");
    if history_path.exists() {
        if let Err(e) = rl.load_history(history_path) {
            eprintln!("Warning: Could not load command history: {}", e);
        }
    }

    let mut multi_line_input = String::new();
    let last_select_query = Arc::new(Mutex::new(String::new()));
    let bookmarks = Arc::new(Mutex::new(BookmarkManager::new()
        .with_context(|| "Failed to initialize bookmarks")?));
    let transaction_manager = TransactionManager::new();
    let mut query_options = QueryOptions::default();

    loop {
        let prompt = get_prompt(&multi_line_input, &transaction_manager);
        
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() && multi_line_input.is_empty() {
                    continue;
                }

                // Handle multi-line input
                let command_to_execute = handle_multi_line_input(&mut multi_line_input, line);

                                if let Some(command) = command_to_execute {
                    let command_trimmed = command.trim();
                    let result = if command_trimmed.starts_with('.') {
                        handle_special_commands(command_trimmed, &mut conn, &db_path, &bookmarks, &last_select_query, &transaction_manager, &mut query_options)
                    } else {
                        handle_single_line_command(command_trimmed, &mut conn, &transaction_manager, &mut query_options)
                    };

                    if let Err(e) = result {
                        print_command_error(&command, &e);
                        if is_critical_error(&e) {
                            if !offer_reconnection(&db_path) {
                                break; // Exit REPL
                            }
                        }
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("EOF");
                break;
            }
            Err(err) => {
                eprintln!("Input error: {}", err);
                eprintln!("Try typing your command again or type 'help' for assistance.");
                continue;
            }
        }
    }

    // Cleanup on exit
    cleanup_repl_session(&conn, &transaction_manager, &mut rl, history_path)?;
    println!("Goodbye!");
    Ok(())
}

fn verify_database_file(db_path: &str) -> Result<()> {
    let metadata = std::fs::metadata(db_path)
        .with_context(|| format!("Cannot read database file '{}'", db_path))?;
    
    if metadata.is_dir() {
        anyhow::bail!("'{}' is a directory, not a database file", db_path);
    }
    
    if metadata.len() == 0 {
        eprintln!("Warning: Database file '{}' is empty", db_path);
    }
    
    Ok(())
}

fn create_robust_connection(db_path: &str) -> Result<Connection> {
    let mut last_error = None;
    let max_retries = 3;
    
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
                    std::thread::sleep(std::time::Duration::from_millis(100 * attempt as u64));
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

fn handle_non_interactive_mode(conn: &Connection) -> Result<()> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    
    let options = QueryOptions::default();
    execute_sql(conn, &input, &options)
}

fn handle_basic_repl_mode(conn: &Connection) -> Result<()> {
    let mut buffer = String::with_capacity(1024);  // Pre-allocate buffer with reasonable capacity
    let options = QueryOptions::default();
    let stdout = std::io::stdout();
    let mut stdout_handle = stdout.lock();  // Lock stdout once instead of multiple times
    
    loop {
        stdout_handle.write_all(b"vapor> ")?;
        stdout_handle.flush()?;
        
        buffer.clear();  // Clear buffer without deallocating
        if std::io::stdin().read_line(&mut buffer)? == 0 {
            break;
        }
        
        let line = buffer.trim();
        if line.is_empty() {
            continue;
        }
        
        if let Err(e) = execute_sql(conn, line, &options) {
            writeln!(stdout_handle, "Error: {}", e)?;
        }
    }
    
    Ok(())
}

fn get_prompt(multi_line_input: &str, transaction_manager: &TransactionManager) -> &'static str {
    if multi_line_input.is_empty() {
        if transaction_manager.is_active() {
            "*> "
        } else {
            "> "
        }
    } else {
        "... "
    }
}

fn handle_multi_line_input(multi_line_input: &mut String, line: &str) -> Option<String> {
    if !multi_line_input.is_empty() {
        multi_line_input.push_str(" ");
        multi_line_input.push_str(line);
        if line.ends_with(';') {
            let command = multi_line_input.trim().to_string();
            multi_line_input.clear();
            Some(command)
        } else {
            None
        }
    } else if line.ends_with(';') || is_complete_command(line) {
        Some(line.to_string())
    } else {
        multi_line_input.push_str(line);
        None
    }
}

fn is_complete_command(line: &str) -> bool {
    let line_lower = line.to_lowercase();
    // These commands don't need semicolons
    matches!(line_lower.as_str(), 
        "exit" | "quit" | "help" | "tables" | "clear" | "info"
    ) || line_lower.starts_with("schema") ||
         line_lower.starts_with(".") ||
         line_lower.starts_with("begin") ||
         line_lower.starts_with("commit") ||
         line_lower.starts_with("rollback") ||
         line_lower.starts_with("drop")
}

fn print_help_summary() {
    println!("Vapor CLI - SQLite Database Management");
    println!("\nSpecial Commands:");
    println!("  .help              Show this help message");
    println!("  .tables            List all tables");
    println!("  .schema [table]    Show schema for all tables or specific table");
    println!("  .info             Show database information");
    println!("  .format [type]    Set output format (table, json, csv)");
    println!("  .limit [n]        Set row limit (0 for no limit)");
    println!("  .timing           Enable query timing");
    println!("  .notiming         Disable query timing");
    println!("  .clear            Clear screen");
    println!("  .exit/.quit       Exit REPL");
    println!("\nSQL Commands:");
    println!("  Enter any valid SQL command ending with semicolon");
    println!("  Example: SELECT * FROM users;");
}

fn print_command_error(command: &str, error: &anyhow::Error) {
    eprintln!("Error executing command '{}':", command);
    eprintln!("{}", error);
}

fn is_critical_error(error: &anyhow::Error) -> bool {
    let error_msg = error.to_string().to_lowercase();
    error_msg.contains("database is locked") ||
    error_msg.contains("connection") ||
    error_msg.contains("i/o error") ||
    error_msg.contains("disk")
}

fn offer_reconnection(db_path: &str) -> bool {
    print!("Would you like to try reconnecting to '{}'? (y/N): ", db_path);
    std::io::stdout().flush().unwrap_or(());
    
    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_ok() {
        input.trim().to_lowercase().starts_with('y')
    } else {
        false
    }
}

fn cleanup_repl_session(
    conn: &Connection,
    transaction_manager: &TransactionManager,
    rl: &mut DefaultEditor,
    history_path: &Path,
) -> Result<()> {
    // Rollback any active transaction
    if transaction_manager.is_active() {
        println!("Rolling back active transaction...");
        transaction_manager.rollback_transaction(conn)?;
    }
    
    // Save command history
    if let Err(e) = rl.save_history(history_path) {
        eprintln!("Warning: Could not save command history: {}", e);
    }
    
    Ok(())
}

fn handle_special_commands(
    command: &str,
    conn: &mut Connection,
    db_path: &str,
    bookmarks: &Arc<Mutex<BookmarkManager>>,
    last_select_query: &Arc<Mutex<String>>,
    transaction_manager: &TransactionManager,
    query_options: &mut QueryOptions,
) -> Result<()> {
    let command = command.trim();
    let parts: Vec<&str> = command.split_whitespace().collect();
    let base_command = parts.get(0).cloned().unwrap_or("");

    match base_command {
        ".help" => show_help(),
        ".shell" => {
            let mut shell = Shell::new(db_path);
            shell.run();
        }
        ".exit" | ".quit" => std::process::exit(0),
        ".tables" => {
            let tables = list_tables(db_path)?;
            for table in tables {
                println!("{}", table);
            }
        }
        ".clear" => {
            print!("\x1B[2J\x1B[1;1H");
            std::io::stdout().flush().context("Failed to flush stdout")?;
        }
        ".info" => show_database_info(conn, db_path)?,
        ".format" => {
            if parts.len() > 1 {
                match parts[1] {
                    "table" => query_options.format = OutputFormat::Table,
                    "json" => query_options.format = OutputFormat::Json,
                    "csv" => query_options.format = OutputFormat::Csv,
                    _ => println!("Invalid format. Available: table, json, csv"),
                }
            } else {
                println!("Current format: {:?}", query_options.format);
                println!("Usage: .format [table|json|csv]");
            }
        }
        ".limit" => {
            if parts.len() > 1 {
                if let Ok(n) = parts[1].parse::<usize>() {
                    if n == 0 {
                        query_options.max_rows = None;
                        println!("Row limit removed");
                    } else {
                        query_options.max_rows = Some(n);
                        println!("Row limit set to {}", n);
                    }
                } else {
                    println!("Invalid limit value. Use a positive number or 0 for no limit.");
                }
            } else {
                match query_options.max_rows {
                    None => println!("No row limit set"),
                    Some(n) => println!("Current row limit: {}", n),
                }
            }
        }
        ".timing" => {
            query_options.show_timing = true;
            println!("Query timing enabled");
        }
        ".notiming" => {
            query_options.show_timing = false;
            println!("Query timing disabled");
        }
        ".export" => {
            if parts.len() > 1 {
                let filename = parts[1];
                let query = last_select_query.lock().unwrap().clone();
                if query.is_empty() {
                    println!("No SELECT query has been executed yet.");
                } else {
                    export_to_csv(conn, &query, filename)?;
                }
            } else {
                println!("Usage: .export FILENAME");
            }
        }
        ".import" => {
            if parts.len() >= 3 {
                import_csv_to_table(conn, parts[1], parts[2])?;
            } else {
                println!("Usage: .import CSV_FILENAME TABLE_NAME");
            }
        }
        ".bookmark" => {
            return handle_bookmark_command(command, bookmarks, last_select_query, conn, query_options);
        }
        ".schema" => {
            if parts.len() > 1 {
                show_table_schema(conn, parts[1])?;
            } else {
                show_all_schemas(conn)?;
            }
        }
        ".status" => {
            transaction_manager.show_status();
        }
        _ => {
            println!("Unknown command: '{}'. Type '.help' for a list of commands.", command);
        }
    }
    Ok(())
}

fn handle_single_line_command(
    line: &str,
    conn: &mut Connection,
    transaction_manager: &TransactionManager,
    query_options: &mut QueryOptions,
) -> Result<()> {
    let line = line.trim();
    match line.to_lowercase().as_str() {
        "begin" | "begin transaction" => transaction_manager.begin_transaction(conn),
        "commit" | "commit transaction" => transaction_manager.commit_transaction(conn),
        "rollback" | "rollback transaction" => transaction_manager.rollback_transaction(conn),
        _ => {
            // Regular SQL query
            execute_sql(conn, line, query_options)
        }
    }
}

fn handle_bookmark_command(
    line: &str,
    bookmarks: &Arc<Mutex<BookmarkManager>>,
    last_select_query: &Arc<Mutex<String>>,
    conn: &mut Connection,
    query_options: &QueryOptions,
) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        println!("Usage: .bookmark [save|list|run|show|delete] [args...]");
        return Ok(());
    }

    let mut bookmarks = bookmarks.lock().unwrap();

    match parts[1] {
        "save" => {
            if parts.len() < 3 {
                println!("Usage: .bookmark save NAME [DESCRIPTION]");
                return Ok(());
            }
            let name = parts[2].to_string();
            let description = if parts.len() > 3 {
                Some(parts[3..].join(" "))
            } else {
                None
            };
            let query = last_select_query.lock().unwrap().clone();
            if query.is_empty() {
                println!("No query to save. Execute a query first.");
            } else {
                bookmarks.save_bookmark(name.clone(), query, description)?;
                println!("Bookmark '{}' saved.", name);
            }
        },
        "list" => {
            bookmarks.list_bookmarks();
        },
        "run" => {
            if parts.len() < 3 {
                println!("Usage: .bookmark run NAME");
                return Ok(());
            }
            let name = parts[2];
            if let Some(bookmark) = bookmarks.get_bookmark(name) {
                println!("Executing bookmark '{}': {}", name, bookmark.query);
                execute_sql(conn, &bookmark.query, query_options)?;
            } else {
                println!("Bookmark '{}' not found.", name);
            }
        },
        "show" => {
            if parts.len() < 3 {
                println!("Usage: .bookmark show NAME");
                return Ok(());
            }
            let name = parts[2];
            if bookmarks.show_bookmark(name).is_none() {
                println!("Bookmark '{}' not found.", name);
            }
        },
        "delete" => {
            if parts.len() < 3 {
                println!("Usage: .bookmark delete NAME");
                return Ok(());
            }
            let name = parts[2];
            if bookmarks.delete_bookmark(name)? {
                println!("Bookmark '{}' deleted.", name);
            } else {
                println!("Bookmark '{}' not found.", name);
            }
        },
        _ => {
            println!("Unknown bookmark command. Use: save, list, run, show, or delete");
        }
    }
    Ok(())
}

/// Display help information for the REPL
pub fn show_help() {
    println!("Enhanced REPL Commands:");
    println!();
    println!("SQL Operations:");
    println!("  SQL statements - Any valid SQL statement ending with semicolon");
    println!("  begin/commit/rollback - Transaction control");
    println!();
    println!("Database Information:");
    println!("  tables - List all tables in the database");
    println!("  schema [table_name] - Show schema for a table or all tables");
    println!("  info - Show database information and statistics");
    println!();
    println!("Output Control:");
    println!("  .format [table|json|csv] - Set output format (default: table)");
    println!("  .limit [N] - Set row limit, 0 for no limit (default: 1000)");
    println!("  .timing [on|off] - Toggle query timing (default: on)");
    println!("  .export FILENAME - Export last SELECT query to CSV file");
    println!("  .import CSV_FILENAME TABLE_NAME - Import CSV file into table");
    println!();
    println!("Bookmarks:");
    println!("  .bookmark save NAME [DESC] - Save current query as bookmark");
    println!("  .bookmark list - List all saved bookmarks");
    println!("  .bookmark run NAME - Execute a saved bookmark");
    println!("  .bookmark show NAME - Show bookmark details");
    println!("  .bookmark delete NAME - Delete a bookmark");
    println!();
    println!("Session Management:");
    println!("  .status - Show transaction status");
    println!("  clear - Clear the screen");
    println!("  help - Show this help message");
    println!("  exit/quit - Exit the REPL");
    println!();
    println!("Features:");
    println!("  • Multi-line input support (continue until semicolon)");
    println!("  • Command history with arrow keys");
    println!("  • Query timing and result pagination");
    println!("  • Transaction status in prompt (* indicates active transaction)");
    println!("  • Multiple output formats (table, JSON, CSV)");
    println!("  • Query bookmarking system");
}

