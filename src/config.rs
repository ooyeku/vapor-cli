//! # Application Configuration Management
//!
//! This module centralizes the logic for handling application-specific configuration,
//! such as file paths for storing user data, history, and bookmarks. It ensures that
//! all configuration files are stored in a consistent, conventional location within the
//! user's home directory.
//!
//! The primary location for all vapor-cli data is `~/.vapor/`.

use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

/// Returns the path to the main application directory (`~/.vapor`).
///
/// This function locates the user's home directory and appends `/.vapor` to it.
/// If the directory does not already exist, it is created.
///
/// # Returns
///
/// A `Result` containing the `PathBuf` for the `~/.vapor` directory, or an `Err` if
/// the home directory cannot be found or the directory cannot be created.
pub fn get_vapor_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().context("Could not find home directory")?;
    let vapor_dir = home_dir.join(".vapor");
    if !vapor_dir.exists() {
        fs::create_dir_all(&vapor_dir).with_context(|| {
            format!(
                "Failed to create .vapor directory at {}",
                vapor_dir.display()
            )
        })?;
    }
    Ok(vapor_dir)
}

/// Returns the full path to the bookmarks storage file.
///
/// This is typically `~/.vapor/bookmarks.json`.
///
/// # Returns
///
/// A `Result` containing the `PathBuf` for the bookmarks file.
pub fn get_bookmarks_path() -> Result<PathBuf> {
    Ok(get_vapor_dir()?.join("bookmarks.json"))
}

/// Returns the full path to the shell history file.
///
/// This is typically `~/.vapor/shell_history`.
///
/// # Returns
///
/// A `Result` containing the `PathBuf` for the shell history file.
pub fn get_shell_history_path() -> Result<PathBuf> {
    Ok(get_vapor_dir()?.join("shell_history"))
}

/// Returns the full path to the REPL history file.
///
/// This is typically `~/.vapor/repl_history`.
///
/// # Returns
///
/// A `Result` containing the `PathBuf` for the REPL history file.
pub fn get_repl_history_path() -> Result<PathBuf> {
    Ok(get_vapor_dir()?.join("repl_history"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_get_vapor_dir_creates_dir() -> Result<()> {
        let temp_dir = tempdir()?;
        // Temporarily override the home directory for this test.
        // This is a bit of a hack, but it's the most straightforward way
        // to test without mocking the entire filesystem.
        let original_home = std::env::var("HOME");
        std::env::set_var("HOME", temp_dir.path().to_str().unwrap());

        let vapor_dir = get_vapor_dir()?;
        assert!(vapor_dir.exists());
        assert!(vapor_dir.ends_with(".vapor"));

        // Restore the original HOME env var
        if let Ok(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }

        Ok(())
    }
}
