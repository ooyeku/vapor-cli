# Vapor CLI

A powerful command-line interface for SQLite database management with enhanced features for data manipulation, querying, testing, and integrated shell capabilities.

## Installation

```bash
cargo install vapor-cli
```

## Basic Usage

Initialize a new database:
```bash
vapor-cli init --name my_database
```

Connect to an existing database:
```bash
vapor-cli connect --path my_database.db
```

Create a new table:
```bash
vapor-cli create-table --db-path my_database.db --name users --columns "id INTEGER PRIMARY KEY, name TEXT, age INTEGER"
```

List all tables:
```bash
vapor-cli list-tables --db-path my_database.db
```

Start an interactive REPL:
```bash
vapor-cli repl --db-path my_database.db
```

Start shell mode with database context:
```bash
vapor-cli shell --db-path my_database.db
```

Populate database with test data:
```bash
vapor-cli populate --db-path my_database.db
```

## Features

### REPL Mode

The interactive REPL provides a comprehensive SQL environment with these features:

SQL Operations:
- Execute any valid SQL statement ending with semicolon
- Transaction control with begin/commit/rollback
- Multi-line input support

Database Information:
- List all tables
- View table schemas
- Show database statistics

Output Control:
- Multiple output formats (table, JSON, CSV)
- Configurable row limits
- Query timing
- Export results to CSV
- Import data from CSV

Query Management:
- Save frequently used queries as bookmarks
- List and manage saved bookmarks
- Execute saved queries

Session Features:
- Command history
- Transaction status indicator
- Screen clearing
- Help system

### Shell Mode

Integrated Unix shell with database context:

System Integration:
- Execute any system command (ls, grep, find, etc.)
- Built-in commands: `cd`, `pwd`, `history`, `help`, `exit`
- Tab completion for commands and file paths
- Dynamic prompt showing current working directory

Navigation:
- Full filesystem navigation with `cd` command
- Support for `~` and `~/` path expansion
- Real-time working directory display

History & Convenience:
- Command history stored in `~/.vapor_shell_history`
- Persistent across sessions
- Proper Ctrl+C handling

### Library API

Use vapor-cli as a library in your Rust projects:

```rust
use vapor_cli::VaporDB;

// Create or open a database
let mut db = VaporDB::create("my_database.db")?;

// Execute SQL
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
db.execute("INSERT INTO users (name) VALUES ('Alice')")?;

// Use transactions
db.begin_transaction()?;
db.execute("INSERT INTO users (name) VALUES ('Bob')")?;
db.commit_transaction()?;

// Export data
db.export_to_csv("users", "users.csv")?;

// Manage bookmarks
if let Some(bm) = db.bookmark_manager() {
    bm.save_bookmark("get_users".to_string(), "SELECT * FROM users".to_string(), None)?;
}
```

## Data Import/Export

Import CSV data:
```
.import data.csv table_name
```

Export query results:
```
.export results.csv
```

## Configuration

The tool automatically creates and manages:
- Command history in `.vapor_history`
- Bookmarks in `~/.vapor_bookmarks.json`
- Database files with `.db` extension

## Error Handling

The tool provides detailed error messages and suggestions for common issues:
- Database access problems
- SQL syntax errors
- File system issues
- Resource constraints

## Requirements

- Rust 1.70 or later
- SQLite 3
- Sufficient disk space for database operations 