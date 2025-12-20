//! PIP service for looking up admin hierarchy for a point.

use std::sync::Arc;
use tracing::debug;

use super::{AdminBoundary, AdminSpatialIndex};
use crate::models::{AdminEntry, AdminHierarchy, AdminLevel};

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

        // Group by level and take the smallest (most specific) at each level
        for level in AdminLevel::all() {
            // Find boundaries at this level
            let at_level: Vec<&Arc<AdminBoundary>> = boundaries
                .iter()
                .filter(|b| b.area.level == *level)
                .collect();

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
}
