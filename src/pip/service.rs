//! PIP service for looking up admin hierarchy for a point.

use std::sync::Arc;
use tracing::debug;

use super::{AdminBoundary, AdminSpatialIndex};
use crate::models::{AdminEntry, AdminHierarchy, AdminLevel};
use geo::Area;

/// Point-in-Polygon lookup service
pub struct PipService {
    index: AdminSpatialIndex,
}

impl PipService {
    /// Create a new PIP service from a spatial index
    pub fn new(index: AdminSpatialIndex) -> Self {
        Self { index }
    }

    /// Build the admin hierarchy for a point
    pub fn lookup(&self, lon: f64, lat: f64, limit_level: Option<AdminLevel>) -> AdminHierarchy {
        let mut hierarchy = AdminHierarchy::default();

        // Find all containing boundaries
        let mut boundaries = self.index.lookup(lon, lat);

        // Filter out boundaries that are at or below the limit level (if provided)
        if let Some(limit) = limit_level {
            boundaries.retain(|b| b.area.level < limit);
        }

        debug!(
            "PIP lookup at ({}, {}): found {} boundaries after filtering",
            lon,
            lat,
            boundaries.len()
        );

        // Deduce country from the most specific boundary (highest admin level)
        // that has an iso_country_code.
        let mut forced_country_code = None;

        // Sort by level descending to find the most specific hint
        let mut boundaries_by_level: Vec<&Arc<AdminBoundary>> = boundaries.iter().collect();
        boundaries_by_level.sort_by(|a, b| b.area.level.cmp(&a.area.level));

        for b in &boundaries_by_level {
            if let Some(code) = &b.area.iso_country_code {
                forced_country_code = Some(code.clone());
                break;
            }
        }

        if let Some(ref code) = forced_country_code {
            debug!("Forcing country code to: {}", code);
        }

        // Group by level and take the smallest (most specific) at each level
        for level in AdminLevel::all() {
            // Find boundaries at this level
            let mut at_level: Vec<&Arc<AdminBoundary>> = boundaries
                .iter()
                .filter(|b| b.area.level == *level)
                .collect();

            // Filter Country level if specific code is enforced
            if *level == AdminLevel::Country {
                if let Some(ref code) = forced_country_code {
                    at_level.retain(|b| {
                        // Match against iso_country_code OR abbr
                        let match_iso = b
                            .area
                            .iso_country_code
                            .as_ref()
                            .map(|c| c == code)
                            .unwrap_or(false);
                        let match_abbr = b.area.abbr.as_ref().map(|c| c == code).unwrap_or(false);
                        match_iso || match_abbr
                    });
                }
            }

            // Sort by area (smallest first) to handle enclaves correctly
            // e.g. Vatican City (small) vs Rome (large) both contain a point in Vatican City.
            // We want the most specific (smallest) one.
            at_level.sort_by(|a, b| {
                let area_a = a.geometry.unsigned_area();
                let area_b = b.geometry.unsigned_area();
                area_a
                    .partial_cmp(&area_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            if let Some(boundary) = at_level.first() {
                let entry = AdminEntry::from_area(&boundary.area);
                hierarchy.set(*level, entry);
            }
        }

        hierarchy
    }

    /// Get the spatial index (for stats/debugging)
    pub fn index(&self) -> &AdminSpatialIndex {
        &self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_hierarchy() {
        let index = AdminSpatialIndex::build(vec![]);
        let service = PipService::new(index);
        let hierarchy = service.lookup(8.5, 47.4, None);
        assert!(hierarchy.country.is_none());
    }

    #[test]
    fn test_country_enforcement() {
        use crate::models::AdminArea;
        use geo::{MultiPolygon, Rect};

        // Helper to make a simple box polygon
        fn make_poly(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> MultiPolygon<f64> {
            let rect = Rect::new(
                geo::Coord { x: min_x, y: min_y },
                geo::Coord { x: max_x, y: max_y },
            );
            MultiPolygon::new(vec![rect.to_polygon()])
        }

        // 1. Create Boundaries
        // Overlapping Countries: US and CA both cover 0,0 to 10,10
        let mut us_area = AdminArea::new(1, AdminLevel::Country);
        us_area.abbr = Some("US".into());
        let us_boundary = AdminBoundary {
            area: us_area,
            geometry: make_poly(0.0, 0.0, 10.0, 10.0),
        };

        let mut ca_area = AdminArea::new(2, AdminLevel::Country);
        ca_area.abbr = Some("CA".into());
        let ca_boundary = AdminBoundary {
            area: ca_area,
            geometry: make_poly(0.0, 0.0, 10.0, 10.0),
        };

        // Ontario: Covers 5,5 to 6,6. iso_country_code = "CA"
        let mut on_area = AdminArea::new(3, AdminLevel::Region);
        on_area.iso_country_code = Some("CA".into());
        let on_boundary = AdminBoundary {
            area: on_area,
            geometry: make_poly(5.0, 5.0, 6.0, 6.0),
        };

        // New York: Covers 1,1 to 2,2. iso_country_code = "US"
        let mut ny_area = AdminArea::new(4, AdminLevel::Region);
        ny_area.iso_country_code = Some("US".into());
        let ny_boundary = AdminBoundary {
            area: ny_area,
            geometry: make_poly(1.0, 1.0, 2.0, 2.0),
        };

        let index =
            AdminSpatialIndex::build(vec![us_boundary, ca_boundary, on_boundary, ny_boundary]);
        let service = PipService::new(index);

        // Case 1: Point in Ontario (5.5, 5.5). Should match Ontario AND Canada (forced by Ontario)
        // Even though US also covers this point geometrically.
        let hierarchy = service.lookup(5.5, 5.5, None);
        assert_eq!(hierarchy.region.as_ref().unwrap().id, Some(3));
        assert_eq!(
            hierarchy.country.as_ref().unwrap().abbr.as_deref(),
            Some("CA")
        );

        // Case 2: Point in New York (1.5, 1.5). Should match NY AND US
        let hierarchy = service.lookup(1.5, 1.5, None);
        assert_eq!(hierarchy.region.as_ref().unwrap().id, Some(4));
        assert_eq!(
            hierarchy.country.as_ref().unwrap().abbr.as_deref(),
            Some("US")
        );
    }
}
