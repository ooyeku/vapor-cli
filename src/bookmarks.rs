use anyhow::{Context, Result};
use prettytable::{row, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone)]
pub struct Bookmark {
    pub name: String,
    pub query: String,
    pub description: Option<String>,
    pub created_at: String,
}

pub struct BookmarkManager {
    bookmarks: HashMap<String, Bookmark>,
    file_path: String,
}

impl BookmarkManager {
    pub fn new() -> Self {
        let file_path = ".vapor_bookmarks.json".to_string();
        let mut manager = Self {
            bookmarks: HashMap::new(),
            file_path,
        };
        manager.load_bookmarks().unwrap_or_else(|_| {
            // If loading fails, start with empty bookmarks
        });
        manager
    }

    pub fn save_bookmark(&mut self, name: String, query: String, description: Option<String>) -> Result<()> {
        let bookmark = Bookmark {
            name: name.clone(),
            query,
            description,
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        };
        
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
        table.add_row(row!["Name", "Description", "Created", "Query Preview"]);

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
                query_preview
            ]);
        }

        table.printstd();
    }

    pub fn delete_bookmark(&mut self, name: &str) -> Result<bool> {
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
            println!("Query:");
            println!("{}", bookmark.query);
            Some(())
        } else {
            None
        }
    }

    fn save_bookmarks(&self) -> Result<()> {
        let json_data = serde_json::to_string_pretty(&self.bookmarks)?;
        fs::write(&self.file_path, json_data)
            .context("Failed to save bookmarks file")?;
        Ok(())
    }

    fn load_bookmarks(&mut self) -> Result<()> {
        if !Path::new(&self.file_path).exists() {
            return Ok(()); // No bookmarks file yet
        }

        let json_data = fs::read_to_string(&self.file_path)
            .context("Failed to read bookmarks file")?;
        self.bookmarks = serde_json::from_str(&json_data)
            .context("Failed to parse bookmarks file")?;
        Ok(())
    }
} 