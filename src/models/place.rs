//! Place document structure for Elasticsearch indexing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::AdminHierarchy;

/// Type of OSM object
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OsmType {
    Node,
    Way,
    Relation,
}

impl std::fmt::Display for OsmType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OsmType::Node => write!(f, "node"),
            OsmType::Way => write!(f, "way"),
            OsmType::Relation => write!(f, "relation"),
        }
    }
}

/// Layer/type of the place (similar to Pelias layers)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Layer {
    /// Points of interest (restaurants, shops, etc.)
    Venue,
    /// Street addresses
    Address,
    /// Streets/roads
    Street,
    /// Administrative boundaries
    Admin,
    /// Neighbourhoods
    Neighbourhood,
    /// Localities (cities, towns, villages)
    Locality,
    /// Regions (states, provinces)
    Region,
    /// Countries
    Country,
}

/// Geographic point (lat/lon)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

/// Bounding box envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoBbox {
    #[serde(rename = "type")]
    pub geo_type: String,
    pub coordinates: [[f64; 2]; 2], // [[minLon, maxLat], [maxLon, minLat]]
}

impl GeoBbox {
    pub fn new(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Self {
        Self {
            geo_type: "envelope".to_string(),
            coordinates: [[min_lon, max_lat], [max_lon, min_lat]],
        }
    }
}

/// Address components
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Address {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub housenumber: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
}

/// Main place document indexed into Elasticsearch.
///
/// This structure follows Pelias conventions with denormalized admin hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Place {
    /// Unique source identifier: "{osm_type}/{osm_id}"
    pub source_id: String,

    /// Source file name for refresh tracking
    pub source_file: String,

    /// Import timestamp for refresh tracking
    pub import_timestamp: DateTime<Utc>,

    /// OSM object type
    pub osm_type: OsmType,

    /// OSM object ID
    pub osm_id: i64,

    /// Wikidata Q-ID if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wikidata_id: Option<String>,

    /// Importance ranking (0.0 to 1.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub importance: Option<f64>,

    /// Layer/type classification
    pub layer: Layer,

    /// Categories from OSM tags (e.g., ["amenity:restaurant", "cuisine:italian"])
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,

    /// Multilingual names: {"default": "...", "de": "...", "fr": "..."}
    pub name: HashMap<String, String>,

    /// Phrase field for exact matching (copy of default name)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phrase: Option<String>,

    /// Address components
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,

    /// Center point for geospatial queries
    pub center_point: GeoPoint,

    /// Bounding box (for larger features)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<GeoBbox>,

    /// Denormalized parent admin hierarchy from PIP lookup
    pub parent: AdminHierarchy,
}

impl Place {
    /// Create a new place with minimal required fields
    pub fn new(
        osm_type: OsmType,
        osm_id: i64,
        layer: Layer,
        center: GeoPoint,
        source_file: &str,
    ) -> Self {
        Self {
            source_id: format!("{}/{}", osm_type, osm_id),
            source_file: source_file.to_string(),
            import_timestamp: Utc::now(),
            osm_type,
            osm_id,
            wikidata_id: None,
            importance: None,
            layer,
            categories: Vec::new(),
            name: HashMap::new(),
            phrase: None,
            address: None,
            center_point: center,
            bbox: None,
            parent: AdminHierarchy::default(),
        }
    }

    /// Add a name in a specific language
    pub fn add_name(&mut self, lang: &str, name: String) {
        if lang == "default" || lang.is_empty() {
            self.phrase = Some(name.clone());
        }
        self.name.insert(lang.to_string(), name);
    }

    /// Add a category from OSM tags
    pub fn add_category(&mut self, key: &str, value: &str) {
        self.categories.push(format!("{}:{}", key, value));
    }
}
