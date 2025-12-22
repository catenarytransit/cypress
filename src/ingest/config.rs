use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub global: GlobalConfig,
    pub regions: Vec<RegionConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GlobalConfig {
    pub es_url: String,
    pub tmp_dir: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RegionConfig {
    pub name: String,
    pub url: String,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path).context("Failed to read config file")?;
        let config: Config = toml::from_str(&content).context("Failed to parse config file")?;
        Ok(config)
    }
}
