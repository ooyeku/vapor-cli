//! # Explicit Transaction Management
//!
//! This module provides a stateful manager for handling database transactions explicitly.
//! It is designed to be used in interactive contexts like a REPL or shell, where users
//! can manually begin, commit, or roll back transactions.
//!
//! ## Core Components:
//! - `TransactionManager`: A thread-safe struct that tracks the current transaction state.
//! - `TransactionState`: An enum representing whether a transaction is `Active` or `None`.
//!
//! The manager ensures that users cannot start a new transaction while one is already
//! active and provides clear feedback about the transaction status. It also intercepts
//! transaction-related SQL keywords (`BEGIN`, `COMMIT`, `ROLLBACK`) to manage state correctly.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Represents the current state of a database transaction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    /// No transaction is currently active.
    None,
    /// A transaction is active and awaiting a `COMMIT` or `ROLLBACK`.
    Active,
}

/// Manages the state of database transactions in a thread-safe manner.
///
/// This struct wraps the `TransactionState` in an `Arc<Mutex<>>` to allow it to be
/// shared across different parts of the application, such as between the REPL and
/// other command handlers, while preventing race conditions.
pub struct TransactionManager {
    state: Arc<Mutex<TransactionState>>,
}

impl TransactionManager {
    /// Creates a new `TransactionManager` with an initial state of `None`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TransactionState::None)),
        }
    }

    /// Begins a new database transaction.
    ///
    /// If a transaction is already active, it prints a warning and does nothing.
    /// Otherwise, it executes a `BEGIN` statement and sets the state to `Active`.
    ///
    /// # Arguments
    /// * `conn` - A reference to the `rusqlite::Connection`.
    pub fn begin_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        match *state {
            TransactionState::Active => {
                println!("Warning: Transaction already active. Use COMMIT or ROLLBACK first.");
                return Ok(());
            }
            TransactionState::None => {
                conn.execute("BEGIN", [])?;
                *state = TransactionState::Active;
                println!("Transaction started.");
            }
        }

        Ok(())
    }

    /// Commits the active database transaction.
    ///
    /// If no transaction is active, it prints a message and does nothing.
    /// Otherwise, it executes a `COMMIT` statement and resets the state to `None`.
    ///
    /// # Arguments
    /// * `conn` - A reference to the `rusqlite::Connection`.
    pub fn commit_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        match *state {
            TransactionState::None => {
                println!("No active transaction to commit.");
                return Ok(());
            }
            TransactionState::Active => {
                conn.execute("COMMIT", [])?;
                *state = TransactionState::None;
                println!("Transaction committed.");
            }
        }

        Ok(())
    }

    /// Rolls back the active database transaction.
    ///
    /// If no transaction is active, it prints a message and does nothing.
    /// Otherwise, it executes a `ROLLBACK` statement and resets the state to `None`.
    ///
    /// # Arguments
    /// * `conn` - A reference to the `rusqlite::Connection`.
    pub fn rollback_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        match *state {
            TransactionState::None => {
                println!("No active transaction to rollback.");
                return Ok(());
            }
            TransactionState::Active => {
                conn.execute("ROLLBACK", [])?;
                *state = TransactionState::None;
                println!("Transaction rolled back.");
            }
        }

        Ok(())
    }

    /// Checks if a transaction is currently active.
    ///
    /// # Returns
    /// `true` if the transaction state is `Active`, `false` otherwise.
    pub fn is_active(&self) -> bool {
        matches!(*self.state.lock().unwrap(), TransactionState::Active)
    }

    /// Prints the current transaction status to the console.
    pub fn show_status(&self) {
        let state = self.state.lock().unwrap();
        match *state {
            TransactionState::None => println!("No active transaction."),
            TransactionState::Active => println!("Transaction is active."),
        }
    }

    /// Intercepts and handles transaction-related SQL commands.
    ///
    /// This method checks if the input SQL string matches known transaction control
    /// statements (`BEGIN`, `COMMIT`, `ROLLBACK`) or a `DROP` command. If a match is found,
    /// it calls the appropriate `TransactionManager` method and returns `Ok(true)`.
    /// For `DROP`, it adds extra validation.
    ///
    /// If the command is not a recognized transaction command, it returns `Ok(false)`,
    /// indicating that the command should be executed as a standard SQL query.
    ///
    /// # Arguments
    /// * `conn` - A reference to the `rusqlite::Connection`.
    /// * `sql` - The SQL command string to be processed.
    ///
    /// # Returns
    /// A `Result<bool>` which is `Ok(true)` if the command was handled, or `Ok(false)` if not.
    pub fn handle_sql_command(&self, conn: &Connection, sql: &str) -> Result<bool> {
        let sql_lower = sql.to_lowercase().trim().to_string();

        match sql_lower.as_str() {
            "begin" | "begin transaction" => {
                self.begin_transaction(conn)?;
                Ok(true) // Command was handled
            }
            "commit" | "commit transaction" => {
                self.commit_transaction(conn)?;
                Ok(true) // Command was handled
            }
            "rollback" | "rollback transaction" => {
                self.rollback_transaction(conn)?;
                Ok(true) // Command was handled
            }
            _ => {
                // Handle DROP commands
                if sql_lower.starts_with("drop") {
                    let parts: Vec<&str> = sql_lower.split_whitespace().collect();
                    if parts.len() < 2 {
                        println!("Usage: DROP TABLE table_name; or DROP table_name;");
                        return Ok(true);
                    }

                    let table_name = if parts[1] == "table" {
                        if parts.len() < 3 {
                            println!("Usage: DROP TABLE table_name;");
                            return Ok(true);
                        }
                        parts[2].trim_end_matches(';')
                    } else {
                        parts[1].trim_end_matches(';')
                    };

                    // Verify table exists before dropping
                    let mut stmt = conn
                        .prepare(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                        )
                        .context("Failed to prepare table existence check")?;

                    let count: i64 = stmt
                        .query_row(rusqlite::params![table_name], |row| row.get(0))
                        .with_context(||
                            format!("Failed to check if table '{}' exists", table_name)
                        )?;

                    if count == 0 {
                        println!("Table '{}' does not exist", table_name);
                        return Ok(true);
                    }

                    // Execute the DROP command
                    conn.execute(&format!("DROP TABLE {}", table_name), [])
                        .with_context(|| format!("Failed to drop table '{}'", table_name))?;

                    println!("Table '{}' dropped successfully", table_name);
                    return Ok(true);
                }
                Ok(false) // Command was not handled
            }
        }
    }
}
