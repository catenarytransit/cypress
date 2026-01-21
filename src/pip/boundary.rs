//! Admin boundary extraction from OSM data.

use geo::MultiPolygon;
use osmpbfreader::{OsmObj, OsmPbfReader};
use tracing::{debug, info};

use crate::models::{AdminArea, AdminLevel};
use crate::pip::geometry::GeometryResolver;

/// A single admin boundary polygon with metadata
#[derive(Debug, Clone)]
pub struct AdminBoundary {
    pub area: AdminArea,
    pub geometry: MultiPolygon<f64>,
}

impl AdminBoundary {
    /// Get the bounding box of this boundary
    pub fn bbox(&self) -> Option<(f64, f64, f64, f64)> {
        use geo::BoundingRect;
        self.geometry
            .bounding_rect()
            .map(|rect| (rect.min().x, rect.min().y, rect.max().x, rect.max().y))
    }
}

/// Extract admin boundaries from OSM PBF file
///
/// Uses the provided GeometryResolver to build geometries.
pub fn extract_admin_boundaries<R: std::io::Read + std::io::Seek>(
    reader: &mut OsmPbfReader<R>,
    resolver: &GeometryResolver,
) -> anyhow::Result<Vec<AdminBoundary>> {
    info!("Extracting admin boundaries...");

    reader.rewind()?;

    let mut boundaries = Vec::new();

    for obj in reader.iter() {
        let obj = obj?;

        // We only care about Relations for admin boundaries (ways are border segments)
        let (r, tags) = match &obj {
            OsmObj::Relation(r) => (r, &r.tags),
            _ => continue,
        };

        // Check if this is an admin boundary
        let is_admin = tags
            .get("boundary")
            .map(|v| v == "administrative")
            .unwrap_or(false);

        if !is_admin {
            continue;
        }

        // Must be type=boundary or type=multipolygon
        let type_tag = tags.get("type").map(|v| v.as_str());
        if !matches!(type_tag, Some("boundary") | Some("multipolygon")) {
            continue;
        }

        // Get admin level
        let level_str = match tags.get("admin_level") {
            Some(l) => l,
            None => continue,
        };

        let level_num: u8 = match level_str.parse() {
            Ok(l) => l,
            Err(_) => continue,
        };

        let level = match AdminLevel::from_osm_level(level_num) {
            Some(l) => l,
            None => continue,
        };

        // Strict Country Check: Must have ISO3166 code
        if level == AdminLevel::Country {
            let has_iso = tags.contains_key("ISO3166-1")
                || tags.contains_key("ISO3166-1:alpha2")
                || tags.contains_key("ISO3166-1:alpha3");

            if !has_iso {
                continue;
            }
        }

        // Build AdminArea with multilingual names
        let id = r.id.0;

        let mut area = AdminArea::new(id, level);

        for (key, value) in tags.iter() {
            if key == "name" {
                area.name.insert("default".to_string(), value.to_string());
            } else if let Some(lang) = key.strip_prefix("name:") {
                area.name.insert(lang.to_string(), value.to_string());
            } else if key == "short_name" || key == "ISO3166-1:alpha2" || key == "ISO3166-1:alpha3"
            {
                area.abbr = Some(value.to_string());
            } else if key == "wikidata" {
                area.wikidata_id = Some(value.to_string());
            }
        }

        // Skip if no name
        if area.name.is_empty() {
            continue;
        }

        // Resolve geometry
        if let Some(geometry) = resolver.resolve_boundary(&obj) {
            use geo::BoundingRect;
            if let Some(rect) = geometry.bounding_rect() {
                area.bbox = Some(crate::models::place::GeoBbox::new(
                    rect.min().x,
                    rect.min().y,
                    rect.max().x,
                    rect.max().y,
                ));
            }
            boundaries.push(AdminBoundary { area, geometry });
        } else {
            debug!("Could not resolve geometry for admin boundary {}", id);
        }
    }

    info!("Found {} admin boundaries", boundaries.len());

    // Sort by admin level (country first)
    boundaries.sort_by(|a, b| a.area.level.cmp(&b.area.level));

    Ok(boundaries)
}
