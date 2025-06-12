use anyhow::{Context, Result};
use prettytable::{row, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;

#[derive(Serialize, Deserialize, Clone)]
pub struct Bookmark {
    pub name: String,
    pub query: String,
    pub description: Option<String>,
    pub created_at: String,
    pub last_modified: String,
}

#[derive(Clone)]
pub struct BookmarkManager {
    bookmarks: HashMap<String, Bookmark>,
    file_path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl BookmarkManager {
    pub fn new() -> Result<Self> {
        // Use user's home directory for bookmarks file
        let home_dir = dirs::home_dir().context("Could not find home directory")?;
        let config_dir = home_dir.join(".vapor");
        let file_path = config_dir.join("bookmarks.json");

        // Create .vapor directory if it doesn't exist
        if !config_dir.exists() {
            fs::create_dir_all(&config_dir)
                .with_context(|| format!("Failed to create directory: {}", config_dir.display()))?;
        }

        let mut manager = Self {
            bookmarks: HashMap::new(),
            file_path,
            lock: Arc::new(Mutex::new(())),
        };

        // Load existing bookmarks, but don't fail if file doesn't exist
        manager
            .load_bookmarks()
            .with_context(|| "Failed to load bookmarks")?;

        Ok(manager)
    }

    pub fn save_bookmark(
        &mut self,
        name: String,
        query: String,
        description: Option<String>,
    ) -> Result<()> {
        // Validate inputs
        if name.trim().is_empty() {
            anyhow::bail!("Bookmark name cannot be empty");
        }
        if query.trim().is_empty() {
            anyhow::bail!("Bookmark query cannot be empty");
        }

        // Check for invalid characters in name
        if name.contains(|c: char| c.is_control() || "\\/:*?\"<>|".contains(c)) {
            anyhow::bail!("Bookmark name contains invalid characters");
        }

        // Check if name is too long
        if name.len() > 64 {
            anyhow::bail!("Bookmark name is too long (maximum 64 characters)");
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("System time error")?
            .as_secs();

        let timestamp = chrono::DateTime::from_timestamp(now as i64, 0)
            .context("Invalid timestamp")?
            .format("%Y-%m-%d %H:%M:%S UTC")
            .to_string();

        let bookmark = Bookmark {
            name: name.clone(),
            query,
            description,
            created_at: if let Some(existing) = self.bookmarks.get(&name) {
                existing.created_at.clone()
            } else {
                timestamp.clone()
            },
            last_modified: timestamp,
        };

        // Create backup before saving
        self.create_backup()?;

        // Use lock to prevent concurrent writes
        let _lock = self.lock.lock().unwrap();

        self.bookmarks.insert(name, bookmark);
        self.save_bookmarks()?;
        Ok(())
    }

    pub fn get_bookmark(&self, name: &str) -> Option<&Bookmark> {
        self.bookmarks.get(name)
    }

    pub fn list_bookmarks(&self) {
        if self.bookmarks.is_empty() {
            println!("No bookmarks saved.");
            return;
        }

        let mut table = Table::new();
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
        table.add_row(row![
            "Name",
            "Description",
            "Created",
            "Modified",
            "Query Preview"
        ]);

        let mut bookmarks: Vec<_> = self.bookmarks.values().collect();
        bookmarks.sort_by(|a, b| a.name.cmp(&b.name));

        for bookmark in bookmarks {
            let description = bookmark
                .description
                .as_deref()
                .unwrap_or("(no description)");
            let query_preview = if bookmark.query.len() > 50 {
                format!("{}...", &bookmark.query[..47])
            } else {
                bookmark.query.clone()
            };
            table.add_row(row![
                bookmark.name,
                description,
                bookmark.created_at,
                bookmark.last_modified,
                query_preview
            ]);
        }

        table.printstd();
    }

    pub fn delete_bookmark(&mut self, name: &str) -> Result<bool> {
        // Create backup before deletion
        self.create_backup()?;

        // Use lock to prevent concurrent writes
        let _lock = self.lock.lock().unwrap();

        if self.bookmarks.remove(name).is_some() {
            self.save_bookmarks()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn show_bookmark(&self, name: &str) -> Option<()> {
        if let Some(bookmark) = self.bookmarks.get(name) {
            println!("Bookmark: {}", bookmark.name);
            if let Some(desc) = &bookmark.description {
                println!("Description: {}", desc);
            }
            println!("Created: {}", bookmark.created_at);
            println!("Last Modified: {}", bookmark.last_modified);
            println!("Query:");
            println!("{}", bookmark.query);
            Some(())
        } else {
            None
        }
    }

    fn save_bookmarks(&self) -> Result<()> {
        let json_data = serde_json::to_string_pretty(&self.bookmarks)?;

        let parent_dir = self.file_path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "Bookmarks file path has no parent directory: {:?}",
                self.file_path
            )
        })?;

        // Explicitly create the parent directory
        fs::create_dir_all(parent_dir)
            .with_context(|| format!("Failed to create bookmarks directory: {:?}", parent_dir))?;

        // Create a named temporary file in the parent directory
        let mut temp_file = NamedTempFile::new_in(parent_dir).with_context(|| {
            format!(
                "Failed to create temporary bookmarks file in directory: {:?}",
                parent_dir
            )
        })?;

        // Write data to the temporary file
        use std::io::Write;
        temp_file
            .write_all(json_data.as_bytes())
            .context("Failed to write data to temporary bookmarks file")?;

        // Atomically replace the target file with the temporary file
        temp_file.persist(&self.file_path).map_err(|e| {
            // e is tempfile::PersistError, which contains the std::io::Error and the NamedTempFile.
            // We are interested in the underlying io::Error for the message.
            anyhow::anyhow!(
                "Failed to save bookmarks file '{}' (source: {:?}, dest: {:?}): {}",
                self.file_path.display(),
                e.file.path(),  // Path of the temporary file that failed to persist
                self.file_path, // Target path for persist
                e.error
            ) // The std::io::Error
        })?;

        Ok(())
    }

    fn load_bookmarks(&mut self) -> Result<()> {
        if !self.file_path.exists() {
            return Ok(()); // No bookmarks file yet
        }

        let json_data =
            fs::read_to_string(&self.file_path).context("Failed to read bookmarks file")?;

        // Try to parse the JSON
        match serde_json::from_str(&json_data) {
            Ok(bookmarks) => {
                self.bookmarks = bookmarks;
                Ok(())
            }
            Err(e) => {
                // If parsing fails, try to load from backup
                if let Ok(backup_data) = self.load_backup() {
                    self.bookmarks = serde_json::from_str(&backup_data)
                        .context("Failed to parse backup bookmarks file")?;
                    Ok(())
                } else {
                    Err(e).context("Failed to parse bookmarks file and no valid backup found")
                }
            }
        }
    }

    fn create_backup(&self) -> Result<()> {
        if !self.file_path.exists() {
            return Ok(());
        }

        let backup_path = self.file_path.with_extension("json.bak");
        fs::copy(&self.file_path, &backup_path).context("Failed to create bookmarks backup")?;
        Ok(())
    }

    fn load_backup(&self) -> Result<String> {
        let backup_path = self.file_path.with_extension("json.bak");
        fs::read_to_string(&backup_path)
            .context("Failed to read bookmarks backup file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};

    // Helper to create a BookmarkManager in a temporary directory
    fn setup_test_manager() -> (BookmarkManager, TempDir) {
        let dir = tempdir().unwrap();
        let bookmarks_path = dir.path().join("bookmarks.json");
        let manager = BookmarkManager {
            bookmarks: HashMap::new(),
            file_path: bookmarks_path.clone(),
            lock: Arc::new(Mutex::new(())),
        };
        (manager, dir)
    }

    #[test]
    fn test_save_and_get_bookmark() -> Result<()> {
        let (mut manager, _dir) = setup_test_manager();

        let name = "test_bookmark".to_string();
        let query = "SELECT * FROM users".to_string();
        let description = Some("A test query".to_string());

        manager.save_bookmark(name.clone(), query.clone(), description.clone())?;

        let bookmark = manager.get_bookmark(&name).unwrap();
        assert_eq!(bookmark.name, name);
        assert_eq!(bookmark.query, query);
        assert_eq!(bookmark.description, description);

        Ok(())
    }

    #[test]
    fn test_update_bookmark() -> Result<()> {
        let (mut manager, _dir) = setup_test_manager();
        let name = "test_update".to_string();
        let initial_query = "SELECT 1".to_string();
        manager.save_bookmark(name.clone(), initial_query, None)?;

        let updated_query = "SELECT 2".to_string();
        manager.save_bookmark(name.clone(), updated_query.clone(), Some("Updated".to_string()))?;

        let bookmark = manager.get_bookmark(&name).unwrap();
        assert_eq!(bookmark.query, updated_query);
        assert_eq!(bookmark.description, Some("Updated".to_string()));

        Ok(())
    }

    #[test]
    fn test_delete_bookmark() -> Result<()> {
        let (mut manager, _dir) = setup_test_manager();
        let name = "to_delete".to_string();
        manager.save_bookmark(name.clone(), "DELETE ME".to_string(), None)?;

        assert!(manager.get_bookmark(&name).is_some());
        manager.delete_bookmark(&name)?;
        assert!(manager.get_bookmark(&name).is_none());

        Ok(())
    }

    #[test]
    fn test_save_bookmark_invalid_name() {
        let (mut manager, _dir) = setup_test_manager();
        assert!(manager.save_bookmark("".to_string(), "q".to_string(), None).is_err());
        assert!(manager.save_bookmark(" ".to_string(), "q".to_string(), None).is_err());
        assert!(manager.save_bookmark("a/b".to_string(), "q".to_string(), None).is_err());
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let (mut manager, _dir) = setup_test_manager();
        let name = "persistent_bookmark".to_string();
        let query = "SELECT 'hello'".to_string();

        manager.save_bookmark(name.clone(), query.clone(), None)?;

        // Create a new manager instance that loads from the same file
        let mut new_manager = BookmarkManager {
            bookmarks: HashMap::new(),
            file_path: manager.file_path.clone(),
            lock: Arc::new(Mutex::new(())),
        };
        new_manager.load_bookmarks()?;

        let bookmark = new_manager.get_bookmark(&name).unwrap();
        assert_eq!(bookmark.name, name);
        assert_eq!(bookmark.query, query);

        Ok(())
    }

    #[test]
    fn test_backup_and_recovery() -> Result<()> {
        let (mut manager, _dir) = setup_test_manager();
        
        // Save a first bookmark. This creates bookmarks.json.
        let first_name = "first_bookmark".to_string();
        manager.save_bookmark(first_name.clone(), "SELECT 1".to_string(), None)?;

        // Save a second bookmark. This will create a backup of the file with only the first bookmark.
        let second_name = "second_bookmark".to_string();
        manager.save_bookmark(second_name.clone(), "SELECT 2".to_string(), None)?;

        // Now, corrupt the main bookmarks file (which contains both bookmarks).
        fs::write(&manager.file_path, "invalid json")?;

        // Try to load the bookmarks. It should recover from the backup.
        let mut recovered_manager = BookmarkManager {
            bookmarks: HashMap::new(),
            file_path: manager.file_path.clone(),
            lock: Arc::new(Mutex::new(())),
        };
        recovered_manager.load_bookmarks()?;

        // The recovered manager should have the state from the backup.
        // It should contain the first bookmark but not the second.
        assert!(recovered_manager.get_bookmark(&first_name).is_some());
        assert!(recovered_manager.get_bookmark(&second_name).is_none());

        Ok(())
    }
}
