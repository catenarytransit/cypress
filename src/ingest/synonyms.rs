use anyhow::{Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::{info, warn};
use walkdir::WalkDir;

/// Manages synonym mappings for text normalization/expansion.
#[derive(Debug, Clone)]
pub struct SynonymService {
    /// Token -> Normalized Text replacement
    replacements: HashMap<String, String>,
}

impl SynonymService {
    pub fn new() -> Self {
        Self {
            replacements: HashMap::new(),
        }
    }

    /// Load synonyms from a directory, recursively.
    pub fn load_from_dir<P: AsRef<Path>>(&mut self, dir: P) -> Result<()> {
        let dir = dir.as_ref();
        if !dir.exists() {
            warn!("Synonym directory not found: {}", dir.display());
            return Ok(());
        }

        info!("Loading synonyms from {}", dir.display());

        for entry in WalkDir::new(dir).follow_links(true) {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            // Skip specific files/dirs
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name == "custom_name.txt" {
                    continue;
                }
                if !file_name.ends_with(".txt") {
                    continue;
                }
            }

            if path.components().any(|c| c.as_os_str() == "punctuation") {
                continue;
            }

            self.load_file(path)?;
        }

        info!("Loaded {} synonym mappings", self.replacements.len());
        Ok(())
    }

    fn load_file(&mut self, path: &Path) -> Result<()> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read synonym file: {}", path.display()))?;

        // Regexes for parsing
        // We compile them here for simplicity, or could move to struct/lazy_static if perf critical
        let comment_regex = Regex::new(r"#.*").unwrap();
        let whitespace_regex = Regex::new(r"\s+").unwrap();
        let arrow_regex = Regex::new(r"=>").unwrap();
        let comma_regex = Regex::new(r",").unwrap();

        for line in content.lines() {
            // Clean line: remove comments, trim, lowercase
            let line = comment_regex.replace(line, "");
            let line = line.trim().to_lowercase();

            if line.is_empty() {
                continue;
            }

            // Squash double spaces
            let line = whitespace_regex.replace_all(&line, " ");

            // Parse
            if line.contains("=>") {
                // explicit mapping: left => right
                let parts: Vec<&str> = arrow_regex.split(&line).collect();
                if parts.len() == 2 {
                    let lefts: Vec<&str> = comma_regex.split(parts[0]).collect();
                    let rights: Vec<&str> = comma_regex.split(parts[1]).collect();

                    // Assume first item on right is canonical
                    if let Some(target) = rights.first() {
                        let target = target.trim().to_string();
                        for src in lefts {
                            let src = src.trim();
                            if !src.is_empty() && src != target {
                                self.replacements.insert(src.to_string(), target.clone());
                            }
                        }
                    }
                }
            } else {
                // equivalent list: "a, b, c" => map b->a, c->a
                let parts: Vec<&str> = comma_regex.split(&line).collect();
                if let Some(canon) = parts.first() {
                    let canon = canon.trim().to_string();
                    for variant in parts.iter().skip(1) {
                        let variant = variant.trim();
                        if !variant.is_empty() && *variant != canon {
                            self.replacements.insert(variant.to_string(), canon.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply synonyms to a text string.
    pub fn normalize(&self, text: &str) -> String {
        let mut result = Vec::new();
        // Simple whitespace split for now
        for token in text.split_whitespace() {
            // strip punctuation
            let clean_token = token
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase();

            if let Some(replacement) = self.replacements.get(&clean_token) {
                result.push(replacement.as_str());
            } else {
                result.push(token);
            }
        }
        result.join(" ")
    }
}
