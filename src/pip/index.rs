//! Spatial index for fast admin boundary lookups.

use geo::{Contains, Point};
use rstar::{RTree, RTreeObject, AABB};
use std::sync::Arc;
use tracing::info;

use super::AdminBoundary;
use crate::models::AdminLevel;

/// Wrapper for R-tree indexing of admin boundaries
#[derive(Clone)]
pub struct IndexedBoundary {
    pub boundary: Arc<AdminBoundary>,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedBoundary {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl IndexedBoundary {
    pub fn new(boundary: AdminBoundary) -> Option<Self> {
        let (min_x, min_y, max_x, max_y) = boundary.bbox()?;
        Some(Self {
            boundary: Arc::new(boundary),
            envelope: AABB::from_corners([min_x, min_y], [max_x, max_y]),
        })
    }
}

/// Spatial index for admin boundaries using R-tree
pub struct AdminSpatialIndex {
    tree: RTree<IndexedBoundary>,
    /// Boundaries sorted by level for hierarchical lookup
    by_level: Vec<(AdminLevel, Vec<Arc<AdminBoundary>>)>,
}

impl AdminSpatialIndex {
    /// Build spatial index from admin boundaries
    pub fn build(boundaries: Vec<AdminBoundary>) -> Self {
        info!(
            "Building spatial index for {} boundaries...",
            boundaries.len()
        );

        // Build R-tree
        let indexed: Vec<IndexedBoundary> = boundaries
            .into_iter()
            .filter_map(IndexedBoundary::new)
            .collect();

        // Group by level
        let mut by_level: std::collections::BTreeMap<AdminLevel, Vec<Arc<AdminBoundary>>> =
            std::collections::BTreeMap::new();

        for ib in &indexed {
            by_level
                .entry(ib.boundary.area.level)
                .or_default()
                .push(Arc::clone(&ib.boundary));
        }

        let tree = RTree::bulk_load(indexed);

        info!("Spatial index built with {} entries", tree.size());
        for (level, bounds) in &by_level {
            info!("  {:?}: {} boundaries", level, bounds.len());
        }

        Self {
            tree,
            by_level: by_level.into_iter().collect(),
        }
    }

    /// Find all admin boundaries containing a point
    pub fn lookup(&self, lon: f64, lat: f64) -> Vec<Arc<AdminBoundary>> {
        let point = Point::new(lon, lat);
        let query_envelope = AABB::from_point([lon, lat]);

        // Use R-tree to get candidates via envelope intersection, then filter with exact containment
        self.tree
            .locate_in_envelope_intersecting(&query_envelope)
            .filter(|ib| ib.boundary.geometry.contains(&point))
            .map(|ib| Arc::clone(&ib.boundary))
            .collect()
    }

    /// Find admin boundaries at a specific level containing a point
    pub fn lookup_at_level(
        &self,
        lon: f64,
        lat: f64,
        level: AdminLevel,
    ) -> Option<Arc<AdminBoundary>> {
        let point = Point::new(lon, lat);
        let query_envelope = AABB::from_point([lon, lat]);

        self.tree
            .locate_in_envelope_intersecting(&query_envelope)
            .filter(|ib| ib.boundary.area.level == level)
            .find(|ib| ib.boundary.geometry.contains(&point))
            .map(|ib| Arc::clone(&ib.boundary))
    }

    /// Get all boundaries for a level (e.g., for debugging)
    pub fn boundaries_at_level(&self, level: AdminLevel) -> &[Arc<AdminBoundary>] {
        for (l, bounds) in &self.by_level {
            if *l == level {
                return bounds;
            }
        }
        &[]
    }

    /// Get total number of indexed boundaries
    pub fn len(&self) -> usize {
        self.tree.size()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.size() == 0
    }

    /// Iterate over all indexed boundaries
    pub fn boundaries(&self) -> impl Iterator<Item = &Arc<AdminBoundary>> {
        self.tree.iter().map(|ib| &ib.boundary)
    }
}
