//! # Interactive Shell Mode
//!
//! This module implements an interactive shell for `vapor-cli`, providing a different
//! mode of operation from the SQL REPL. The shell allows users to run standard system
//! commands and a few built-in commands, all while maintaining the context of the
//! connected database.
//!
//! ## Features:
//! - **System Command Execution**: Run any command available in the system's `PATH`.
//! - **Built-in Commands**: Includes `cd`, `pwd`, `history`, and `help`.
//! - **Database Context**: The shell is aware of the connected database, which can be referenced via `.dbinfo`.
//! - **REPL Integration**: Seamlessly switch back to the SQL REPL using the `.vrepl` command.
//! - **Command Completion**: Provides basic completion for built-in commands and file paths.
//! - **Persistent History**: Saves shell command history across sessions.

use crate::config;
use anyhow::{Context, Result};
use ctrlc;
use rustyline::completion::{Completer, FilenameCompleter};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Editor, Helper};
use std::env;
use std::io::{self};
use std::path::Path;
use std::process::Command;

/// Defines the possible actions that can be returned from the shell session.
/// This is used to signal whether the user wants to exit the application entirely
/// or switch back to the SQL REPL.
#[derive(Debug, PartialEq, Eq)]
pub enum ShellAction {
    Exit,
    SwitchToRepl,
}

const BUILTIN_COMMANDS: &[&str] = &["cd", "pwd", "history", "help", "exit", ".vrepl", ".dbinfo"];

struct ShellHelper {
    filename_completer: FilenameCompleter,
}

impl Helper for ShellHelper {}

impl Completer for ShellHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<String>)> {
        let line = &line[..pos];
        let words: Vec<&str> = line.split_whitespace().collect();

        if words.is_empty() {
            // Complete with built-in commands
            return Ok((0, BUILTIN_COMMANDS.iter().map(|&s| s.to_string()).collect()));
        }

        if words.len() == 1 {
            // First word - complete with commands
            let prefix = words[0];
            let candidates: Vec<String> = BUILTIN_COMMANDS
                .iter()
                .filter(|&&cmd| cmd.starts_with(prefix))
                .map(|&s| s.to_string())
                .collect();

            if !candidates.is_empty() {
                return Ok((0, candidates));
            }
        }

        // For other cases, use filename completion
        if line.trim_start().starts_with("cd ") || line.trim_start().starts_with("ls ") {
            let (start, pairs) = self.filename_completer.complete(line, pos, _ctx)?;
            let candidates: Vec<String> = pairs.into_iter().map(|p| p.replacement).collect();
            return Ok((start, candidates));
        }

        Ok((pos, Vec::new()))
    }
}

impl Highlighter for ShellHelper {}

impl Hinter for ShellHelper {
    type Hint = String;
}

impl Validator for ShellHelper {}

/// Represents the state of the interactive shell.
///
/// This struct holds the `rustyline` editor instance, the original directory from which
/// the shell was started, the path to the history file, and the path to the connected
/// database.
pub struct Shell {
    editor: Editor<ShellHelper, rustyline::history::FileHistory>,
    original_dir: std::path::PathBuf,
    history_path: std::path::PathBuf,
    db_path: String, // To store the database path
}

impl Shell {
        /// Creates a new `Shell` instance.
    ///
    /// This function initializes the `rustyline` editor, sets up the command completer,
    /// loads the command history, and establishes a Ctrl+C handler.
    ///
    /// # Arguments
    ///
    /// * `db_path` - The path to the database file, which is kept for context.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Shell` instance, or an `Err` if initialization fails.
    pub fn new(db_path: &str) -> Result<Self> {
        let helper = ShellHelper {
            filename_completer: FilenameCompleter::new(),
        };

        let mut editor = Editor::new().unwrap();
        editor.set_helper(Some(helper));

        let original_dir = env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());

        // Set up history path in home directory
        let history_path = config::get_shell_history_path()?;

        // Load history if available
        if history_path.exists() {
            if let Err(e) = editor.load_history(&history_path) {
                eprintln!("Warning: Could not load shell history: {}", e);
            }
        }

        // Set up Ctrl+C handler
        if let Err(e) = ctrlc::set_handler(move || {
            println!("\nUse 'exit' to return to the REPL");
        }) {
            eprintln!("Warning: Could not set up Ctrl+C handler: {}", e);
        }

        Ok(Shell {
            editor,
            original_dir,
            history_path,
            db_path: db_path.to_string(),
        })
    }

    fn get_prompt(&self) -> String {
        let cwd = env::current_dir().unwrap_or_default();
        let home = env::var("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_default();

        let display_path = if cwd == home {
            "~".to_string()
        } else if let Ok(stripped) = cwd.strip_prefix(&home) {
            format!("~/{}", stripped.display())
        } else {
            cwd.display().to_string()
        };

        format!("[vapor-shell {}]$ ", display_path)
    }

        /// Runs the main loop of the shell.
    ///
    /// This function displays the prompt, reads user input, and executes the entered
    /// commands. It handles both built-in commands and external system commands.
    /// The loop continues until the user enters `exit` or `.vrepl`.
    ///
    /// # Returns
    ///
    /// A `ShellAction` indicating the user's intent to either exit the application
    /// or switch back to the REPL.
    pub fn run(&mut self) -> ShellAction {
        println!("Welcome to Vapor Shell! Type 'exit' to return to the REPL.");
        println!("Type 'help' for available commands.");

        loop {
            let prompt = self.get_prompt();
            let readline = self.editor.readline(&prompt);
            match readline {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    if let Err(e) = self.editor.add_history_entry(line) {
                        eprintln!("Warning: Could not add to history: {}", e);
                    }

                    if line == "exit" {
                        return ShellAction::Exit;
                    }

                    if line == ".vrepl" {
                        return ShellAction::SwitchToRepl;
                    }

                    if line == ".dbinfo" {
                        println!("Connected to database: {}", self.db_path);
                        continue;
                    }

                    if line == "help" {
                        self.show_help();
                        continue;
                    }

                    self.execute_command(line);
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    return ShellAction::Exit; // Treat EOF as a normal exit
                }
                Err(err) => {
                    eprintln!("Input error: {}", err);
                    continue;
                }
            }
        }
    }

    fn execute_command(&mut self, command: &str) {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        match parts[0] {
            "cd" => {
                let path = if parts.len() > 1 {
                    let p = parts[1];
                    if p == "~" {
                        env::var("HOME").unwrap_or_else(|_| ".".to_string())
                    } else if p.starts_with("~/") {
                        env::var("HOME")
                            .map(|home| format!("{}/{}", home, &p[2..]))
                            .unwrap_or_else(|_| p.to_string())
                    } else {
                        p.to_string()
                    }
                } else {
                    env::var("HOME").unwrap_or_else(|_| ".".to_string())
                };

                if let Err(e) = env::set_current_dir(Path::new(&path)) {
                    eprintln!("cd: {}: {}", path, e);
                }
            }
            "pwd" => {
                if let Ok(current_dir) = env::current_dir() {
                    println!("{}", current_dir.display());
                }
            }
            "history" => {
                for (i, entry) in self.editor.history().iter().enumerate() {
                    println!("{}: {}", i + 1, entry);
                }
            }
            _ => {
                let status = Command::new(parts[0]).args(&parts[1..]).status();

                match status {
                    Ok(status) => {
                        if !status.success() {
                            if let Some(code) = status.code() {
                                eprintln!("Command failed with exit code: {}", code);
                            } else {
                                eprintln!("Command terminated by signal");
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error executing command: {}", e);
                        if e.kind() == io::ErrorKind::NotFound {
                            eprintln!("Command not found: {}", parts[0]);
                        }
                    }
                }
            }
        }
    }

    fn show_help(&self) {
        println!("Vapor Shell - Available Commands:");
        println!("  .vrepl         - Switch back to the SQL REPL");
        println!("  .dbinfo        - Show information about the connected database");
        println!("  cd <dir>       - Change directory");
        println!("  ls [dir]       - List directory contents");
        println!("  pwd            - Print working directory");
        println!("  history        - Show command history");
        println!("  help           - Show this help message");
        println!("  exit           - Exit the shell and return to the REPL");
    }

    fn save_history(&mut self) -> Result<()> {
        self.editor
            .save_history(&self.history_path)
            .context("Failed to save shell history")
    }
}

/// Starts the interactive shell mode.
///
/// This function initializes and runs the `Shell`. It ensures that the command history
/// is saved and the original working directory is restored after the shell session ends.
///
/// # Arguments
///
/// * `db_path` - The path to the database file, which provides context to the shell.
///
/// # Returns
///
/// A `Result` containing the `ShellAction` that indicates the next step for the calling code
/// (e.g., exit or switch to REPL).
pub fn shell_mode(db_path: &str) -> Result<ShellAction> {
    println!("Starting shell mode for database: {}", db_path);

    let mut shell = Shell::new(db_path)?;
    let action = shell.run();

    // Save history before exiting
    if let Err(e) = shell.save_history() {
        eprintln!("Warning: Could not save shell history: {}", e);
    }

    // Restore original directory
    if let Err(e) = env::set_current_dir(&shell.original_dir) {
        eprintln!("Warning: Could not restore original directory: {}", e);
    }

    Ok(action)
}