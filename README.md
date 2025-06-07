# Vapor CLI

A command-line interface for SQLite database management with enhanced features for data manipulation, querying, and testing.

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

Populate database with test data:
```bash
vapor-cli populate --db-path my_database.db
```

## REPL Features

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