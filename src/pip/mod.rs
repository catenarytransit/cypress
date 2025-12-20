//! Point-in-Polygon (PIP) admin lookup service.
//!
//! Extracts admin boundaries from OSM and provides fast PIP lookups
//! using an R-tree spatial index.

mod boundary;
pub mod geometry;
mod index;
mod service;

pub use boundary::{extract_admin_boundaries, AdminBoundary};
pub use geometry::GeometryResolver;
pub use index::AdminSpatialIndex;
pub use service::PipService;
