use std::io::{self};
use std::process::Command;
use std::env;
use std::path::Path;
use rustyline::error::ReadlineError;
use rustyline::{Editor, Helper};
use rustyline::completion::{Completer, FilenameCompleter};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use ctrlc;
use anyhow::Result;

const BUILTIN_COMMANDS: &[&str] = &["cd", "pwd", "history", "help", "exit", ".vrepl", ".dbinfo"];

struct ShellHelper {
    filename_completer: FilenameCompleter,
}

impl Helper for ShellHelper {}

#[derive(Debug, PartialEq, Eq)]
pub enum ShellAction {
    Exit,
    SwitchToRepl,
}

impl Completer for ShellHelper {
    type Candidate = String;

    fn complete(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> rustyline::Result<(usize, Vec<String>)> {
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

pub struct Shell {
    history: Vec<String>,
    editor: Editor<ShellHelper, rustyline::history::FileHistory>,
    original_dir: std::path::PathBuf,
    history_path: std::path::PathBuf,
    db_path: String, // To store the database path
}

impl Shell {
    pub fn new(db_path: &str) -> Self {
        let helper = ShellHelper {
            filename_completer: FilenameCompleter::new(),
        };
        
        let mut editor = Editor::new().unwrap();
        editor.set_helper(Some(helper));
        
        let original_dir = env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
        
        // Set up history path in home directory
        let history_path = match env::var("HOME") {
            Ok(home) => Path::new(&home).join(".vapor_shell_history"),
            Err(_) => Path::new(".vapor_shell_history").to_path_buf(),
        };

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

        Shell {
            history: Vec::new(),
            editor,
            original_dir,
            history_path,
            db_path: db_path.to_string(),
        }
    }

    fn get_prompt(&self) -> String {
        let cwd = env::current_dir().unwrap_or_default();
        let home = env::var("HOME").map(std::path::PathBuf::from).unwrap_or_default();

        let display_path = if cwd == home {
            "~".to_string()
        } else if let Ok(stripped) = cwd.strip_prefix(&home) {
            format!("~/{}", stripped.display())
        } else {
            cwd.display().to_string()
        };

        format!("[vapor-shell {}]$ ", display_path)
    }

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

                    // Add to history
                    self.history.push(line.to_string());
                    if let Err(e) = self.editor.add_history_entry(line) {
                        eprintln!("Warning: Could not add to history: {}", e);
                    }

                    // Handle exit command first
                    if line == "exit" {
                        println!("Returning to REPL...");
                        // Save history before exiting
                        if let Err(e) = self.editor.save_history(&self.history_path) {
                            eprintln!("Warning: Could not save shell history: {}", e);
                        }
                        // Restore original directory
                        if let Err(e) = env::set_current_dir(&self.original_dir) {
                            eprintln!("Warning: Could not restore original directory: {}", e);
                        }
                        return ShellAction::Exit;
                    }

                    if line == ".vrepl" {
                        println!("Switching to SQL REPL...");
                        // Save history before switching
                        if let Err(e) = self.editor.save_history(&self.history_path) {
                            eprintln!("Warning: Could not save shell history: {}", e);
                        }
                        // Restore original directory
                        if let Err(e) = env::set_current_dir(&self.original_dir) {
                            eprintln!("Warning: Could not restore original directory: {}", e);
                        }
                        return ShellAction::SwitchToRepl;
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
                    println!("EOF");
                    // Save history and restore dir before breaking
                    if let Err(e) = self.editor.save_history(&self.history_path) {
                        eprintln!("Warning: Could not save shell history: {}", e);
                    }
                    if let Err(e) = env::set_current_dir(&self.original_dir) {
                        eprintln!("Warning: Could not restore original directory: {}", e);
                    }
                    return ShellAction::Exit; // Treat EOF as a normal exit
                }
                Err(err) => {
                    eprintln!("Input error: {}", err);
                    continue;
                }
            }
        }

        // All paths that exit this loop (exit command, .vrepl command, EOF) 
        // now explicitly return a ShellAction. Therefore, the loop itself will not terminate
        // in a way that would cause execution to reach code after the loop.
        // The function is guaranteed to return a ShellAction via one of those paths.
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
                        env::var("HOME").map(|home| format!("{}/{}", home, &p[2..])).unwrap_or_else(|_| p.to_string())
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
                for (i, cmd) in self.history.iter().enumerate() {
                    println!("{}: {}", i + 1, cmd);
                }
            }
            "help" => self.show_help(),
            _ => {
                let status = Command::new(parts[0])
                    .args(&parts[1..])
                    .status();

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
        println!("\nBuilt-in Commands:");
        println!("  cd [dir]     Change directory (defaults to home if no dir specified)");
        println!("  pwd          Print working directory");
        println!("  history      Show command history");
        println!("  help         Show this help message");
        println!("  .vrepl       Exit shell and switch to SQL REPL for the current database");
        println!("  exit         Exit shell and return to the main CLI prompt (or OS shell if launched directly)");
        println!("\nSystem Commands:");
        println!("  All standard Unix/Linux commands are available");
        println!("  Command completion is available (press TAB)");
        println!("  File/directory completion is available (press TAB)");
        println!("\nFeatures:");
        println!("  • Command history with arrow keys");
        println!("  • Tab completion for commands and files");
        println!("  • Command history persistence");
        println!("  • Error handling and reporting");
    }
}

/// Start shell mode with database context
/// Start shell mode with database context
pub fn shell_mode(db_path: &str) -> Result<ShellAction> {
    println!("Starting shell mode for database: {}", db_path);
    
    let mut shell = Shell::new(db_path);
    let action = shell.run();
    
    Ok(action)
}
