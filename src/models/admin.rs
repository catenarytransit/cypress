//! Administrative hierarchy types for PIP lookup.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OSM admin_level mapping to semantic level names.
/// See: https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AdminLevel {
    /// Country (admin_level=2)
    Country,
    /// Macro region / federal district (admin_level=3)
    MacroRegion,
    /// Region / state / province (admin_level=4)
    Region,
    /// Macro county (admin_level=5)
    MacroCounty,
    /// County / district (admin_level=6)
    County,
    /// Local admin / municipality (admin_level=7)
    LocalAdmin,
    /// Locality / city / town / village (admin_level=8)
    Locality,
    /// Borough / city district (admin_level=9)
    Borough,
    /// Neighbourhood / suburb (admin_level=10)
    Neighbourhood,
}

impl AdminLevel {
    /// Convert OSM admin_level number to AdminLevel
    pub fn from_osm_level(level: u8) -> Option<Self> {
        match level {
            2 => Some(AdminLevel::Country),
            3 => Some(AdminLevel::MacroRegion),
            4 => Some(AdminLevel::Region),
            5 => Some(AdminLevel::MacroCounty),
            6 => Some(AdminLevel::County),
            7 => Some(AdminLevel::LocalAdmin),
            8 => Some(AdminLevel::Locality),
            9 => Some(AdminLevel::Borough),
            10 | 11 => Some(AdminLevel::Neighbourhood),
            _ => None,
        }
    }

    /// Get the OSM admin_level number
    pub fn to_osm_level(&self) -> u8 {
        match self {
            AdminLevel::Country => 2,
            AdminLevel::MacroRegion => 3,
            AdminLevel::Region => 4,
            AdminLevel::MacroCounty => 5,
            AdminLevel::County => 6,
            AdminLevel::LocalAdmin => 7,
            AdminLevel::Locality => 8,
            AdminLevel::Borough => 9,
            AdminLevel::Neighbourhood => 10,
        }
    }

    /// Get all admin levels in hierarchical order (country first)
    pub fn all() -> &'static [AdminLevel] {
        &[
            AdminLevel::Country,
            AdminLevel::MacroRegion,
            AdminLevel::Region,
            AdminLevel::MacroCounty,
            AdminLevel::County,
            AdminLevel::LocalAdmin,
            AdminLevel::Locality,
            AdminLevel::Borough,
            AdminLevel::Neighbourhood,
        ]
    }

    /// Get the field name for this level
    pub fn field_name(&self) -> &'static str {
        match self {
            AdminLevel::Country => "country",
            AdminLevel::MacroRegion => "macro_region",
            AdminLevel::Region => "region",
            AdminLevel::MacroCounty => "macro_county",
            AdminLevel::County => "county",
            AdminLevel::LocalAdmin => "local_admin",
            AdminLevel::Locality => "locality",
            AdminLevel::Borough => "borough",
            AdminLevel::Neighbourhood => "neighbourhood",
        }
    }
}

/// An administrative area with multilingual names
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminArea {
    /// OSM relation ID
    pub osm_id: i64,

    /// Admin level
    pub level: AdminLevel,

    /// Wikidata ID if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wikidata_id: Option<String>,

    /// Multilingual names: {"default": "...", "de": "...", "fr": "..."}
    pub name: HashMap<String, String>,

    /// Bounding box
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<super::place::GeoBbox>,

    /// Abbreviation (e.g., "CH" for Switzerland)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abbr: Option<String>,
}

impl AdminArea {
    pub fn new(osm_id: i64, level: AdminLevel) -> Self {
        Self {
            osm_id,
            level,
            wikidata_id: None,
            name: HashMap::new(),
            bbox: None,
            abbr: None,
        }
    }

    /// Get default name
    pub fn default_name(&self) -> Option<&String> {
        self.name.get("default")
    }
}

/// Single admin level entry in the hierarchy.
///
/// # Serialization Notes
/// - **Elasticsearch**: Only `name`, `abbr`, `id`, and `bbox` fields are indexed.
///   The `names` field is skipped during serialization to avoid conflicts with
///   language codes like "id" (Indonesian) that would overwrite the numeric `id` field.
/// - **ScyllaDB**: Use `to_scylla_json()` to get full multilingual names preserved
///   as a nested `names` object for later retrieval.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminEntry {
    /// Default name (concatenated from all language variants for Elasticsearch full-text search)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Abbreviation (e.g., "AT" for Austria)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abbr: Option<String>,

    /// OSM relation ID (used as stable ID)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,

    /// Bounding box
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<super::place::GeoBbox>,

    /// Multilingual names: {"default": "Austria", "de": "Österreich", "id": "Austria", ...}
    /// Skipped in default serialization (for Elasticsearch) to prevent language codes
    /// like "id" (Indonesian) from conflicting with the `id` field.
    /// Use `to_scylla_json()` to include this field for ScyllaDB storage.
    #[serde(skip)]
    pub names: HashMap<String, String>,
}

/// ScyllaDB-specific representation that includes the full `names` HashMap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminEntryScylla {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abbr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<super::place::GeoBbox>,
    /// Full multilingual names preserved for ScyllaDB
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub names: HashMap<String, String>,
}

impl AdminEntry {
    /// Serialize to JSON for ScyllaDB storage (preserves full multilingual `names`).
    pub fn to_scylla_json(&self) -> Result<String, serde_json::Error> {
        let scylla_entry = AdminEntryScylla {
            name: self.name.clone(),
            abbr: self.abbr.clone(),
            id: self.id,
            bbox: self.bbox.clone(),
            names: self.names.clone(),
        };
        serde_json::to_string(&scylla_entry)
    }

    /// Deserialize from ScyllaDB JSON (includes multilingual `names`).
    pub fn from_scylla(scylla_entry: AdminEntryScylla) -> Self {
        Self {
            name: scylla_entry.name,
            abbr: scylla_entry.abbr,
            id: scylla_entry.id,
            bbox: scylla_entry.bbox,
            names: scylla_entry.names,
        }
    }

    pub fn from_area(area: &AdminArea) -> Self {
        // Use only the default name (or first available) for the name field.
        // All multilingual variants are stored in the names HashMap.
        let default_name = area
            .name
            .get("default")
            .or_else(|| area.name.values().next())
            .cloned();

        Self {
            name: default_name,
            abbr: area.abbr.clone(),
            id: Some(area.osm_id),
            bbox: area.bbox.clone(),
            names: area.name.clone(),
        }
    }
}

/// Denormalized admin hierarchy stored on each place document.
///
/// This follows Pelias conventions where parent data is flattened onto the document.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminHierarchy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub macro_region: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub macro_county: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_admin: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub borough: Option<AdminEntry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<AdminEntry>,
}

impl AdminHierarchy {
    /// Set an admin entry for a given level
    pub fn set(&mut self, level: AdminLevel, entry: AdminEntry) {
        match level {
            AdminLevel::Country => self.country = Some(entry),
            AdminLevel::MacroRegion => self.macro_region = Some(entry),
            AdminLevel::Region => self.region = Some(entry),
            AdminLevel::MacroCounty => self.macro_county = Some(entry),
            AdminLevel::County => self.county = Some(entry),
            AdminLevel::LocalAdmin => self.local_admin = Some(entry),
            AdminLevel::Locality => self.locality = Some(entry),
            AdminLevel::Borough => self.borough = Some(entry),
            AdminLevel::Neighbourhood => self.neighbourhood = Some(entry),
        }
    }

    /// Get an admin entry for a given level
    pub fn get(&self, level: AdminLevel) -> Option<&AdminEntry> {
        match level {
            AdminLevel::Country => self.country.as_ref(),
            AdminLevel::MacroRegion => self.macro_region.as_ref(),
            AdminLevel::Region => self.region.as_ref(),
            AdminLevel::MacroCounty => self.macro_county.as_ref(),
            AdminLevel::County => self.county.as_ref(),
            AdminLevel::LocalAdmin => self.local_admin.as_ref(),
            AdminLevel::Locality => self.locality.as_ref(),
            AdminLevel::Borough => self.borough.as_ref(),
            AdminLevel::Neighbourhood => self.neighbourhood.as_ref(),
        }
    }
}
