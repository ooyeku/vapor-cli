# Vapor CLI

**Vapor CLI** is a powerful, interactive command-line interface for SQLite databases. It's designed to be a lightweight, fast, and user-friendly tool for developers, data analysts, and anyone who works with SQLite. Vapor CLI combines the power of a direct SQL interface with the convenience of a modern shell and data management utilities.

## Features

- **Interactive SQL REPL**: A robust Read-Eval-Print Loop for executing SQL queries, with multi-line input, command history, and auto-completion.
- **Interactive Shell**: A built-in shell for navigating the filesystem, running system commands, and managing your database environment without leaving the tool.
- **Direct SQL Execution**: Run SQL queries directly from your terminal for quick, one-off tasks.
- **CSV Import/Export**: Seamlessly import data from CSV files into tables or export query results to CSV.
- **Database Population**: Generate large volumes of synthetic data with configurable schemas, data types, and distributions for testing and development.
- **Query Bookmarks**: Save, manage, and reuse your frequently used SQL queries with a powerful bookmarking system.
- **Multiple Output Formats**: Display query results in different formats, including formatted tables, JSON, and CSV.
- **Explicit Transaction Management**: Manually control database transactions (`BEGIN`, `COMMIT`, `ROLLBACK`) within the REPL.

## Installation

1. **Clone the repository:**

    ```sh
    git clone https://github.com/ooyeku/vapor-cli.git
    cd vapor-cli
    ```

2. **Build the project using Cargo:**

    ```sh
    cargo build --release
    ```

    The executable will be located at `target/release/vapor-cli`.

3. **(Optional) Install it locally:**

    To make `vapor-cli` available from anywhere in your system, you can install it using Cargo:

    ```sh
    cargo install --path .
    ```

## Usage

### Initialize a Database

Create a new, empty SQLite database file.

```sh
vapor-cli init --name my_database.db
```

### Connect to a Database

Check the connection to an existing database file.

```sh
vapor-cli connect --path my_database.db
```

### Interactive SQL REPL

Start an interactive SQL Read-Eval-Print Loop to run queries against a database.

```sh
vapor-cli repl --db-path my_database.db
```

Inside the REPL, you can type SQL statements or use special dot-commands:

```sql
-- Select all users
SELECT * FROM users;

-- Special commands
.tables
.schema users
.exit
```

### Interactive Shell

Start an interactive shell session with the database context loaded.

```sh
vapor-cli shell --db-path my_database.db
```

From the shell, you can run system commands or switch back to the REPL:

```sh
# List files
ls -l

# Switch back to the REPL
.vrepl
```

### Manage Tables

**Create a new table:**

```sh
vapor-cli create-table --db-path my_database.db --name users --columns "id INTEGER PRIMARY KEY, name TEXT"
```

**List all tables in the database:**

```sh
vapor-cli list-tables --db-path my_database.db
```

### Populate Database

Populate the database with a large amount of sample data for testing purposes.

```sh
vapor-cli populate --db-path my_database.db
```

## Configuration

Vapor CLI stores its configuration and history in `~/.config/vapor/`.

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