use anyhow::{Context, Result};
use prettytable::{row, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
        let home_dir = dirs::home_dir()
            .context("Could not find home directory")?;
        let file_path = home_dir.join(".vapor_bookmarks.json");
        
        let mut manager = Self {
            bookmarks: HashMap::new(),
            file_path,
            lock: Arc::new(Mutex::new(())),
        };
        
        // Load existing bookmarks, but don't fail if file doesn't exist
        manager.load_bookmarks().with_context(|| "Failed to load bookmarks")?;
        
        Ok(manager)
    }

    pub fn save_bookmark(&mut self, name: String, query: String, description: Option<String>) -> Result<()> {
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
        table.add_row(row!["Name", "Description", "Created", "Modified", "Query Preview"]);

        let mut bookmarks: Vec<_> = self.bookmarks.values().collect();
        bookmarks.sort_by(|a, b| a.name.cmp(&b.name));

        for bookmark in bookmarks {
            let description = bookmark.description.as_deref().unwrap_or("(no description)");
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
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = self.file_path.parent() {
            fs::create_dir_all(parent)
                .context("Failed to create bookmarks directory")?;
        }
        
        // Write to temporary file first
        let temp_path = self.file_path.with_extension("json.tmp");
        fs::write(&temp_path, json_data)
            .context("Failed to write bookmarks to temporary file")?;
            
        // Atomic rename
        fs::rename(&temp_path, &self.file_path)
            .context("Failed to save bookmarks file")?;
            
        Ok(())
    }

    fn load_bookmarks(&mut self) -> Result<()> {
        if !self.file_path.exists() {
            return Ok(()); // No bookmarks file yet
        }

        let json_data = fs::read_to_string(&self.file_path)
            .context("Failed to read bookmarks file")?;
            
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
        fs::copy(&self.file_path, &backup_path)
            .context("Failed to create bookmarks backup")?;
        Ok(())
    }
    
    fn load_backup(&self) -> Result<String> {
        let backup_path = self.file_path.with_extension("json.bak");
        fs::read_to_string(&backup_path)
            .context("Failed to read bookmarks backup file")
    }
} 