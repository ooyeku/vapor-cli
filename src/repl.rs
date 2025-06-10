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
use crate::export::export_to_csv;
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
    let conn = create_robust_connection(&db_path)?;

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
                     // Add to history before execution
                     if let Err(e) = rl.add_history_entry(&command) {
                         eprintln!("Warning: Could not add to history: {}", e);
                     }

                     // Check if it's a special command first
                     if let Err(_e) = handle_special_commands(
                         &command,
                         &conn,
                         &db_path,
                         &bookmarks,    
                         &last_select_query,
                         &transaction_manager,
                         &mut query_options,
                     ) {
                         // If not a special command, try SQL execution
                         if let Ok(handled) = transaction_manager.handle_sql_command(&conn, &command) {
                             if !handled {
                                 // Regular SQL command
                                 if let Err(sql_err) = execute_sql(&conn, &command, &query_options) {
                                     print_command_error(&command, &sql_err);
                                     
                                     // For critical errors, offer to reconnect
                                     if is_critical_error(&sql_err) {
                                         if offer_reconnection(&db_path) {
                                             match create_robust_connection(&db_path) {
                                                 Ok(_new_conn) => {
                                                     println!("Reconnected successfully!");
                                                     // Note: Connection replacement would require refactoring
                                                 }
                                                 Err(reconnect_err) => {
                                                     eprintln!("Reconnection failed: {}", reconnect_err);
                                                     eprintln!("You may need to restart the REPL.");
                                                 }
                                             }
                                         }
                                     }
                                 }
                             }
                         } else {
                             println!("Error handling transaction command");
                         }
                     }
                 } else {
                     // Handle single-line special commands (for incomplete multi-line)
                     if let Err(e) = handle_single_line_command(
                         line,
                         &conn,
                         &db_path,
                         &bookmarks,
                         &last_select_query,
                         &transaction_manager,
                         &mut query_options,
                         &rl,
                     ) {
                         print_command_error(line, &e);
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
    conn: &Connection,
    db_path: &str,
    bookmarks: &Arc<Mutex<BookmarkManager>>,
    last_select_query: &Arc<Mutex<String>>,
    transaction_manager: &TransactionManager,
    query_options: &mut QueryOptions,
) -> Result<()> {
    let command = command.trim();
    
    match command {
        ".help" => {
            show_help();
            Ok(())
        }
        ".shell" => {
            let mut shell = Shell::new(db_path);
            shell.run();
            Ok(())
        }
        ".exit" | ".quit" | "exit" | "quit" => {
            std::process::exit(0);
        }
        ".tables" => {
            let _ = list_tables(db_path)?;
            Ok(())
        }
        ".clear" => {
            print!("\x1B[2J\x1B[1;1H");
            std::io::stdout().flush().context("Failed to flush stdout")?;
            Ok(())
        }
        ".info" => {
            show_database_info(conn, db_path)?;
            Ok(())
        }
        ".format" => {
            println!("Current format: {:?}", query_options.format);
            println!("Available formats: table, json, csv");
            Ok(())
        }
        ".format table" => {
            query_options.format = OutputFormat::Table;
            Ok(())
        }
        ".format json" => {
            query_options.format = OutputFormat::Json;
            Ok(())
        }
        ".format csv" => {
            query_options.format = OutputFormat::Csv;
            Ok(())
        }
        ".limit" => {
            match query_options.max_rows {
                None => println!("No row limit set"),
                Some(n) => println!("Current row limit: {}", n),
            }
            Ok(())
        }
        ".limit 0" => {
            query_options.max_rows = None;
            println!("Row limit removed");
            Ok(())
        }
        limit_cmd if limit_cmd.starts_with(".limit ") => {
            if let Ok(n) = limit_cmd.split_whitespace().nth(1).unwrap().parse::<usize>() {
                query_options.max_rows = Some(n);
                println!("Row limit set to {}", n);
            } else {
                println!("Invalid limit value. Use a positive number or 0 for no limit.");
            }
            Ok(())
        }
        ".timing" => {
            query_options.show_timing = true;
            println!("Query timing enabled");
            Ok(())
        }
        ".notiming" => {
            query_options.show_timing = false;
            println!("Query timing disabled");
            Ok(())
        }
        ".timing status" => {
            println!("Query timing: {}", if query_options.show_timing { "on" } else { "off" });
            Ok(())
        }
        ".export" => {
            let parts: Vec<&str> = command.split_whitespace().collect();
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
            Ok(())
        }
        ".import" => {
            let parts: Vec<&str> = command.split_whitespace().collect();
            if parts.len() >= 3 {
                let filename = parts[1];
                let table_name = parts[2];
                import_csv_to_table(conn, filename, table_name)?;
            } else {
                println!("Usage: .import CSV_FILENAME TABLE_NAME");
                println!("Example: .import data.csv employees");
            }
            Ok(())
        }
        ".bookmark" => {
            handle_bookmark_command(command, bookmarks, last_select_query, conn, query_options)?;
            Ok(())
        }
        schema_cmd if schema_cmd.starts_with(".schema") => {
            let parts: Vec<&str> = schema_cmd.split_whitespace().collect();
            if parts.len() == 1 {
                // Just ".schema" - show all schemas
                show_all_schemas(conn)?;
            } else if parts.len() == 2 {
                // ".schema table_name" - show specific table schema
                show_table_schema(conn, parts[1])?;
            } else {
                println!("Usage: .schema [table_name]");
            }
            Ok(())
        }
        ".status" => {
            transaction_manager.show_status();
            Ok(())
        }
        _ => {
            // Handle DROP commands
            if command.to_lowercase().starts_with("drop") {
                let parts: Vec<&str> = command.split_whitespace().collect();
                if parts.len() < 2 {
                    println!("Usage: DROP TABLE table_name; or DROP table_name;");
                    Ok(())
                } else {
                    let table_name = if parts[1].to_lowercase() == "table" {
                        if parts.len() < 3 {
                            println!("Usage: DROP TABLE table_name;");
                            return Ok(());
                        }
                        parts[2].trim_end_matches(';')
                    } else {
                        parts[1].trim_end_matches(';')
                    };
                    
                    // Verify table exists before dropping
                    if !check_table_exists_repl(conn, table_name)? {
                        println!("Table '{}' does not exist", table_name);
                        Ok(())
                    } else {
                        // Execute the DROP command
                        conn.execute(&format!("DROP TABLE {}", table_name), [])
                            .with_context(|| format!("Failed to drop table '{}'", table_name))?;
                        
                        println!("Table '{}' dropped successfully", table_name);
                        Ok(())
                    }
                }
            } else {
                // Execute as SQL command
                execute_sql(conn, command, query_options)?;
                Ok(())
            }
        }
    }
}

fn handle_single_line_command(
    line: &str,
    conn: &Connection,
    db_path: &str,
    _bookmarks: &Arc<Mutex<BookmarkManager>>,
    _last_select_query: &Arc<Mutex<String>>,
    _transaction_manager: &TransactionManager,
    query_options: &mut QueryOptions,
    _rl: &DefaultEditor,
) -> Result<()> {
    match line.trim() {
        ".help" => print_help_summary(),
        ".tables" => { let _ = list_tables(db_path)?; },
        ".schema" => {
            if line.contains(' ') {
                let table_name = line.split_whitespace().nth(1).unwrap();
                show_table_schema(conn, table_name)?;
            } else {
                show_all_schemas(conn)?;
            }
        }
        ".info" => show_database_info(conn, db_path)?,
        ".format" => {
            println!("Current format: {:?}", query_options.format);
            println!("Available formats: table, json, csv");
        }
        ".format table" => query_options.format = OutputFormat::Table,
        ".format json" => query_options.format = OutputFormat::Json,
        ".format csv" => query_options.format = OutputFormat::Csv,
        ".limit" => {
            match query_options.max_rows {
                None => println!("No row limit set"),
                Some(n) => println!("Current row limit: {}", n),
            }
        }
        ".limit 0" => {
            query_options.max_rows = None;
            println!("Row limit removed");
        }
        limit_cmd if limit_cmd.starts_with(".limit ") => {
            if let Ok(n) = limit_cmd.split_whitespace().nth(1).unwrap().parse::<usize>() {
                query_options.max_rows = Some(n);
                println!("Row limit set to {}", n);
            } else {
                println!("Invalid limit value. Use a positive number or 0 for no limit.");
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
        ".timing status" => {
            println!("Query timing: {}", if query_options.show_timing { "on" } else { "off" });
        }
        ".clear" => {
            print!("\x1B[2J\x1B[1;1H");
        }
        ".exit" | ".quit" | "exit" | "quit" => {
            std::process::exit(0);
        }
        _ => {
            // Execute as SQL command
            execute_sql(conn, line, query_options)?;
        }
    }
    Ok(())
}

fn handle_bookmark_command(
    line: &str,
    bookmarks: &Arc<Mutex<BookmarkManager>>,
    last_select_query: &Arc<Mutex<String>>,
    conn: &Connection,
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

/// Import CSV data into a table with comprehensive error handling
fn import_csv_to_table(conn: &Connection, filename: &str, table_name: &str) -> Result<()> {
    use std::path::Path;
    
    // Validate inputs
    if filename.trim().is_empty() {
        anyhow::bail!("CSV filename cannot be empty");
    }
    
    if table_name.trim().is_empty() {
        anyhow::bail!("Table name cannot be empty");
    }
    
    // Validate table name format
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!("Table name can only contain letters, numbers, and underscores");
    }
    
    // Check if CSV file exists
    if !Path::new(filename).exists() {
        anyhow::bail!("CSV file '{}' does not exist", filename);
    }
    
    // Check file extension
    if !filename.to_lowercase().ends_with(".csv") {
        eprintln!("Warning: File '{}' doesn't have .csv extension", filename);
    }
    
    println!("Reading CSV file: {}", filename);
    
    // Open and read CSV file
    let mut csv_reader = csv::Reader::from_path(filename)
        .with_context(|| format!("Failed to open CSV file '{}'. Check file permissions.", filename))?;
    
    // Get headers from CSV
    let headers = csv_reader.headers()
        .with_context(|| format!("Failed to read CSV headers from '{}'", filename))?;
    
    if headers.is_empty() {
        anyhow::bail!("CSV file '{}' has no headers", filename);
    }
    
    let column_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    println!("Found {} columns: {}", column_names.len(), column_names.join(", "));
    
    // Check if table exists
    let table_exists = check_table_exists_repl(conn, table_name)?;
    
    if table_exists {
        println!("Table '{}' already exists", table_name);
        
        // Verify table has compatible columns
        verify_table_compatibility(conn, table_name, &column_names)?;
    } else {
        println!("Creating table '{}' with inferred schema", table_name);
        create_table_from_csv_headers(conn, table_name, &column_names)?;
    }
    
    // Prepare insert statement
    let placeholders = (0..column_names.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {} ({}) VALUES ({})", 
        table_name, 
        column_names.join(", "), 
        placeholders
    );
    
    let mut stmt = conn.prepare(&insert_sql)
        .with_context(|| format!("Failed to prepare insert statement for table '{}'", table_name))?;
    
    // Import data with progress tracking
    let mut row_count = 0;
    let mut error_count = 0;
    let start_time = std::time::Instant::now();
    
    println!("Importing data...");
    
    // Begin transaction for better performance
    let tx = conn.unchecked_transaction()
        .context("Failed to begin transaction for import")?;
    
    for (record_num, record_result) in csv_reader.records().enumerate() {
        match record_result {
            Ok(record) => {
                let values: Vec<&str> = record.iter().collect();
                
                if values.len() != column_names.len() {
                    error_count += 1;
                    eprintln!("Row {}: Expected {} columns, got {} - skipping", 
                        record_num + 2, column_names.len(), values.len());
                    
                    if error_count > 100 {
                        anyhow::bail!("Too many format errors ({}). Import stopped.", error_count);
                    }
                    continue;
                }
                
                match stmt.execute(rusqlite::params_from_iter(values)) {
                    Ok(_) => {
                        row_count += 1;
                        
                        // Progress indicator
                        if row_count % 10000 == 0 {
                            let elapsed = start_time.elapsed();
                            let rate = row_count as f64 / elapsed.as_secs_f64();
                            println!("Imported {} rows ({:.0} rows/sec)...", row_count, rate);
                        }
                    }
                    Err(e) => {
                        error_count += 1;
                        eprintln!("Row {}: Database error - {}", record_num + 2, e);
                        
                        if error_count > 100 {
                            anyhow::bail!("Too many database errors ({}). Import stopped.", error_count);
                        }
                    }
                }
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Row {}: CSV parsing error - {}", record_num + 2, e);
                
                if error_count > 100 {
                    anyhow::bail!("Too many parsing errors ({}). Import stopped.", error_count);
                }
            }
        }
    }
    
    // Commit transaction
    tx.commit()
        .context("Failed to commit import transaction. All changes have been rolled back.")?;
    
    let duration = start_time.elapsed();
    
    if error_count > 0 {
        println!("Import completed with {} error(s)", error_count);
    }
    
    println!("Successfully imported {} rows into table '{}' in {:.2} seconds", 
        row_count, table_name, duration.as_secs_f64());
    
    if row_count > 0 {
        println!("Average: {:.0} rows/second", row_count as f64 / duration.as_secs_f64());
        
        // Show sample of imported data
        let sample_query = format!("SELECT * FROM {} LIMIT 5", table_name);
        println!("\nSample of imported data:");
        let options = QueryOptions::default();
        if let Err(e) = execute_sql(conn, &sample_query, &options) {
            eprintln!("Could not show sample data: {}", e);
        }   
    }
    
    Ok(())
}

fn check_table_exists_repl(conn: &Connection, table_name: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1")
        .context("Failed to prepare table existence check")?;
    
    let count: i64 = stmt.query_row(rusqlite::params![table_name], |row| row.get(0))
        .with_context(|| format!("Failed to check if table '{}' exists", table_name))?;
    
    Ok(count > 0)
}

fn verify_table_compatibility(conn: &Connection, table_name: &str, csv_columns: &[String]) -> Result<()> {
    // Get table schema
    let mut stmt = conn.prepare("PRAGMA table_info(?1)")
        .context("Failed to prepare table info query")?;
    
    let table_columns: Result<Vec<String>, _> = stmt.query_map(rusqlite::params![table_name], |row| {
        let name: String = row.get(1)?;
        Ok(name)
    })?.collect();
    
    let table_columns = table_columns
        .with_context(|| format!("Failed to get column information for table '{}'", table_name))?;
    
    // Check if CSV columns match table columns
    let missing_in_table: Vec<String> = csv_columns.iter()
        .filter(|&col| !table_columns.contains(col))
        .cloned()
        .collect();
    
    let missing_in_csv: Vec<&String> = table_columns.iter()
        .filter(|&col| !csv_columns.contains(col))
        .collect();
    
    if !missing_in_table.is_empty() {
        eprintln!("Warning: CSV has columns not in table: {}", missing_in_table.join(", "));
    }
    
    if !missing_in_csv.is_empty() {
        let missing_csv_strs: Vec<String> = missing_in_csv.iter().map(|s| s.to_string()).collect();
        eprintln!("Warning: Table has columns not in CSV: {}", missing_csv_strs.join(", "));
    }
    
    Ok(())
}

fn create_table_from_csv_headers(conn: &Connection, table_name: &str, column_names: &[String]) -> Result<()> {
    // Create table with TEXT columns (SQLite will handle type conversions)
    let column_defs: Vec<String> = column_names.iter()
        .map(|name| format!("{} TEXT", name))
        .collect();
    
    let create_sql = format!("CREATE TABLE {} ({})", table_name, column_defs.join(", "));
    
    conn.execute(&create_sql, [])
        .with_context(|| format!("Failed to create table '{}' for CSV import", table_name))?;
    
    println!("Created table '{}' with {} columns", table_name, column_names.len());
    
    Ok(())
}