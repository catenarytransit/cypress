use chrono::{DateTime, Utc};
use serde::Serialize;

use cypress::elasticsearch::EsDocument;
use cypress::models::{Address, AdminHierarchy, GeoBbox, GeoPoint, Layer, OsmType, Place};

/// Normalized Place document for Elasticsearch (excludes `name` map)
#[derive(Debug, Clone, Serialize)]
pub struct EsPlaceDoc {
    pub source_id: String,
    pub source_file: String,
    pub import_timestamp: DateTime<Utc>,
    pub osm_type: OsmType,
    pub osm_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wikidata_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub importance: Option<f64>,
    pub layer: Layer,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    // name field excluded
    pub name_all: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phrase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,
    pub center_point: GeoPoint,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<GeoBbox>,
    pub parent: AdminHierarchy,
}

impl EsDocument for EsPlaceDoc {
    fn id(&self) -> &str {
        &self.source_id
    }
}

impl From<&Place> for EsPlaceDoc {
    fn from(place: &Place) -> Self {
        Self {
            source_id: place.source_id.clone(),
            source_file: place.source_file.clone(),
            import_timestamp: place.import_timestamp,
            osm_type: place.osm_type,
            osm_id: place.osm_id,
            wikidata_id: place.wikidata_id.clone(),
            importance: place.importance,
            layer: place.layer,
            categories: place.categories.clone(),
            name_all: place.name_all.clone(),
            phrase: place.phrase.clone(),
            address: place.address.clone(),
            center_point: place.center_point,
            bbox: place.bbox.clone(),
            parent: place.parent.clone(),
        }
    }
}
