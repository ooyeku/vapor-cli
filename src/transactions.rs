use anyhow::{Result};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    None,
    Active,
}

pub struct TransactionManager {
    state: Arc<Mutex<TransactionState>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TransactionState::None)),
        }
    }

    pub fn begin_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        
        match *state {
            TransactionState::Active => {
                println!("Warning: Transaction already active. Use COMMIT or ROLLBACK first.");
                return Ok(());
            },
            TransactionState::None => {
                conn.execute("BEGIN", [])?;
                *state = TransactionState::Active;
                println!("Transaction started.");
            }
        }
        
        Ok(())
    }

    pub fn commit_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        
        match *state {
            TransactionState::None => {
                println!("No active transaction to commit.");
                return Ok(());
            },
            TransactionState::Active => {
                conn.execute("COMMIT", [])?;
                *state = TransactionState::None;
                println!("Transaction committed.");
            }
        }
        
        Ok(())
    }

    pub fn rollback_transaction(&self, conn: &Connection) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        
        match *state {
            TransactionState::None => {
                println!("No active transaction to rollback.");
                return Ok(());
            },
            TransactionState::Active => {
                conn.execute("ROLLBACK", [])?;
                *state = TransactionState::None;
                println!("Transaction rolled back.");
            }
        }
        
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        matches!(*self.state.lock().unwrap(), TransactionState::Active)
    }

    pub fn show_status(&self) {
        let state = self.state.lock().unwrap();
        match *state {
            TransactionState::None => println!("No active transaction."),
            TransactionState::Active => println!("Transaction is active."),
        }
    }

    // Handle commands that might affect transaction state
    pub fn handle_sql_command(&self, conn: &Connection, sql: &str) -> Result<bool> {
        let sql_lower = sql.to_lowercase().trim().to_string();
        
        match sql_lower.as_str() {
            "begin" | "begin transaction" => {
                self.begin_transaction(conn)?;
                Ok(true) // Command was handled
            },
            "commit" | "commit transaction" => {
                self.commit_transaction(conn)?;
                Ok(true) // Command was handled
            },
            "rollback" | "rollback transaction" => {
                self.rollback_transaction(conn)?;
                Ok(true) // Command was handled
            },
            _ => Ok(false) // Command was not a transaction command
        }
    }
} 