use serde::{Deserialize, Serialize};

use super::place::{Address, GeoBbox, GeoPoint, Layer, OsmType};
use super::{AdminLevel, Place};

/// Normalized version of Place for ScyllaDB storage.
/// Reduces duplication by referencing admin areas by ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedPlace {
    pub source_id: String,
    pub source_file: String,
    pub import_timestamp: chrono::DateTime<chrono::Utc>,
    pub osm_type: OsmType,
    pub osm_id: i64,
    pub wikidata_id: Option<String>,
    pub importance: Option<f64>,
    pub layer: Layer,
    pub categories: Vec<String>,
    pub name: std::collections::HashMap<String, String>,
    pub phrase: Option<String>,
    pub address: Option<Address>,
    pub center_point: GeoPoint,
    pub bbox: Option<GeoBbox>,
    pub parent: AdminHierarchyIds,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminHierarchyIds {
    pub country: Option<String>,
    pub macro_region: Option<String>,
    pub region: Option<String>,
    pub macro_county: Option<String>,
    pub county: Option<String>,
    pub local_admin: Option<String>,
    pub locality: Option<String>,
    pub borough: Option<String>,
    pub neighbourhood: Option<String>,
}

impl NormalizedPlace {
    pub fn from_place(place: Place) -> Self {
        let parent = AdminHierarchyIds {
            country: place
                .parent
                .country
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            macro_region: place
                .parent
                .macro_region
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            region: place
                .parent
                .region
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            macro_county: place
                .parent
                .macro_county
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            county: place
                .parent
                .county
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            local_admin: place
                .parent
                .local_admin
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            locality: place
                .parent
                .locality
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            borough: place
                .parent
                .borough
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
            neighbourhood: place
                .parent
                .neighbourhood
                .and_then(|e| e.id.map(|id| format!("relation/{}", id))),
        };

        Self {
            source_id: place.source_id,
            source_file: place.source_file,
            import_timestamp: place.import_timestamp,
            osm_type: place.osm_type,
            osm_id: place.osm_id,
            wikidata_id: place.wikidata_id,
            importance: place.importance,
            layer: place.layer,
            categories: place.categories,
            name: place.name,
            phrase: place.phrase,
            address: place.address,
            center_point: place.center_point,
            bbox: place.bbox,
            parent,
        }
    }
}
