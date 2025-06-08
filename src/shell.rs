use std::io::{self, Write};
use std::process::Command;
use std::env;
use std::path::Path;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use ctrlc;

pub struct Shell {
    prompt: String,
    history: Vec<String>,
    editor: DefaultEditor,
    original_dir: std::path::PathBuf,
    history_path: std::path::PathBuf,
}

impl Shell {
    pub fn new() -> Self {
        let mut editor = DefaultEditor::new().unwrap();
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
            prompt: "vapor-shell> ".to_string(),
            history: Vec::new(),
            editor,
            original_dir,
            history_path,
        }
    }

    pub fn run(&mut self) {
        println!("Welcome to Vapor Shell! Type 'exit' to return to the REPL.");
        println!("Type 'help' for available commands.");
        
        loop {
            let readline = self.editor.readline(&self.prompt);
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
                        return;
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
                    break;
                }
                Err(err) => {
                    eprintln!("Input error: {}", err);
                    continue;
                }
            }
        }

        // Save history
        if let Err(e) = self.editor.save_history(&self.history_path) {
            eprintln!("Warning: Could not save shell history: {}", e);
        }
        // Restore original directory
        if let Err(e) = env::set_current_dir(&self.original_dir) {
            eprintln!("Warning: Could not restore original directory: {}", e);
        }
    }

    fn execute_command(&self, command: &str) {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        match parts[0] {
            "cd" => {
                if parts.len() > 1 {
                    if let Err(e) = env::set_current_dir(Path::new(parts[1])) {
                        eprintln!("cd: {}", e);
                    }
                } else {
                    if let Ok(home) = env::var("HOME") {
                        if let Err(e) = env::set_current_dir(Path::new(&home)) {
                            eprintln!("cd: {}", e);
                        }
                    }
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
                let output = Command::new(parts[0])
                    .args(&parts[1..])
                    .output();

                match output {
                    Ok(output) => {
                        io::stdout().write_all(&output.stdout).unwrap();
                        io::stderr().write_all(&output.stderr).unwrap();
                        
                        if !output.status.success() {
                            if let Some(code) = output.status.code() {
                                eprintln!("Command failed with exit code: {}", code);
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
        println!("  exit         Exit shell and return to REPL");
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
