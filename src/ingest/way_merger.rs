//! Merges adjacent road ways with the same name to reduce disk space.
//!
//! This module groups OSM ways that represent continuous roads with the same name,
//! merging them into single indexed entries. This significantly reduces:
//! - Elasticsearch storage requirements
//! - Number of indexed documents
//! - Search result duplication

use geo::{BoundingRect, Centroid, Coord, LineString, MultiLineString};
use hashbrown::HashMap;
use osmpbfreader::{Tags, WayId};
use std::sync::Arc;
use tracing::info;

use cypress::models::{GeoBbox, GeoPoint, Layer, OsmType, Place};
use cypress::pip::GeometryResolver;

/// Represents a road way eligible for merging
#[derive(Debug, Clone)]
pub struct RoadWay {
    pub way_id: WayId,
    pub tags: Tags,
    pub nodes: Vec<i64>,
}

/// A merged group of road ways
#[derive(Debug)]
pub struct MergedRoad {
    /// All way IDs that were merged into this road
    pub way_ids: Vec<WayId>,
    /// The combined geometry
    pub line_strings: Vec<LineString<f64>>,
    /// Tags from the first way (they should all be the same)
    pub tags: Tags,
}

/// Manages the merging of adjacent road ways with the same name
pub struct WayMerger {
    /// Roads grouped by name and highway type
    roads_by_name: HashMap<String, Vec<RoadWay>>,
    /// Geometry resolver for coordinate lookup
    resolver: Arc<GeometryResolver>,
}

impl WayMerger {
    /// Create a new WayMerger
    pub fn new(resolver: Arc<GeometryResolver>) -> Self {
        Self {
            roads_by_name: HashMap::new(),
            resolver,
        }
    }

    /// Add a road way to be considered for merging
    pub fn add_road(&mut self, way_id: WayId, tags: Tags, nodes: Vec<i64>) {
        // Get the name for grouping
        if let Some(name) = Self::get_merge_key(&tags) {
            self.roads_by_name
                .entry(name)
                .or_insert_with(Vec::new)
                .push(RoadWay {
                    way_id,
                    tags,
                    nodes,
                });
        }
    }

    /// Generate a merge key from tags (name + highway type)
    fn get_merge_key(tags: &Tags) -> Option<String> {
        let name = tags.get("name")?;
        let highway = tags.get("highway")?;

        // Only merge certain highway types (not motorways or links)
        match highway.as_str() {
            "motorway" | "motorway_link" | "trunk_link" | "primary_link" | "secondary_link"
            | "tertiary_link" => return None,
            _ => {}
        }

        Some(format!("{}|{}", name, highway))
    }

    /// Merge adjacent ways and return the merged roads
    pub fn merge(mut self) -> Vec<MergedRoad> {
        info!("Merging roads with same names...");

        let mut merged_roads = Vec::new();
        let mut total_ways = 0;
        let mut _merged_ways = 0;

        // Extract resolver to avoid borrow issues
        let resolver = self.resolver.clone();

        for (_name, mut ways) in self.roads_by_name.drain() {
            total_ways += ways.len();

            if ways.is_empty() {
                continue;
            }

            // If only one way with this name, no merging needed
            if ways.len() == 1 {
                let way = ways.remove(0);
                merged_roads.push(MergedRoad {
                    way_ids: vec![way.way_id],
                    line_strings: vec![Self::get_linestring_static(&resolver, &way)],
                    tags: way.tags,
                });
                continue;
            }

            // Build connectivity graph
            let groups = Self::group_connected_ways_static(&mut ways);

            for group in groups {
                if group.len() > 1 {
                    _merged_ways += group.len();
                }

                let way_ids: Vec<_> = group.iter().map(|w| w.way_id).collect();
                let line_strings: Vec<_> = group
                    .iter()
                    .map(|w| Self::get_linestring_static(&resolver, w))
                    .collect();
                let tags = group[0].tags.clone();

                merged_roads.push(MergedRoad {
                    way_ids,
                    line_strings,
                    tags,
                });
            }
        }

        info!(
            "Merged {} ways into {} road segments (saved {} entries)",
            total_ways,
            merged_roads.len(),
            total_ways - merged_roads.len()
        );

        merged_roads
    }

    /// Group ways that are physically connected
    fn group_connected_ways_static(ways: &mut [RoadWay]) -> Vec<Vec<RoadWay>> {
        if ways.is_empty() {
            return vec![];
        }

        let mut remaining: Vec<RoadWay> = ways.iter().cloned().collect();
        let mut groups = Vec::new();

        while !remaining.is_empty() {
            let mut current_group = vec![remaining.remove(0)];
            let mut changed = true;

            // Keep trying to add connected ways
            while changed && !remaining.is_empty() {
                changed = false;

                for i in (0..remaining.len()).rev() {
                    if Self::is_connected_to_group(&current_group, &remaining[i]) {
                        current_group.push(remaining.remove(i));
                        changed = true;
                    }
                }
            }

            groups.push(current_group);
        }

        groups
    }

    /// Check if a way is connected to any way in the group
    fn is_connected_to_group(group: &[RoadWay], way: &RoadWay) -> bool {
        let way_start = way.nodes.first();
        let way_end = way.nodes.last();

        for group_way in group {
            let group_start = group_way.nodes.first();
            let group_end = group_way.nodes.last();

            // Check if endpoints match
            if way_start == group_start
                || way_start == group_end
                || way_end == group_start
                || way_end == group_end
            {
                return true;
            }
        }

        false
    }

    /// Convert a RoadWay to a LineString (static version)
    fn get_linestring_static(resolver: &Arc<GeometryResolver>, way: &RoadWay) -> LineString<f64> {
        let coords: Vec<Coord<f64>> = way
            .nodes
            .iter()
            .filter_map(|&node_id| {
                // Access the resolver's get_node_coords method
                // We need to convert i64 to NodeId
                let node_id = osmpbfreader::NodeId(node_id);
                resolver.get_node_coords(node_id)
            })
            .collect();

        LineString::new(coords)
    }
}

impl MergedRoad {
    /// Convert the merged road into a Place for indexing
    pub fn to_place(&self, source_file: &str) -> Option<Place> {
        // Create MultiLineString from all segments
        let multi_line = MultiLineString::new(self.line_strings.clone());

        // Calculate centroid
        let center = multi_line.centroid().map(|p| GeoPoint {
            lat: p.y(),
            lon: p.x(),
        })?;

        // Calculate bounding box
        let bbox = multi_line
            .bounding_rect()
            .map(|rect| GeoBbox::new(rect.min().x, rect.min().y, rect.max().x, rect.max().y));

        // Use the first way ID as the representative
        let osm_id = self.way_ids[0].0;

        // Create the place
        let mut place = Place::new(OsmType::Way, osm_id, Layer::Street, center, source_file);
        place.bbox = bbox;

        // If multiple ways were merged, add a note in categories
        if self.way_ids.len() > 1 {
            place
                .categories
                .push(format!("merged_ways:{}", self.way_ids.len()));
        }

        Some(place)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_key_generation() {
        let mut tags = Tags::new();
        tags.insert("name".into(), "Main Street".into());
        tags.insert("highway".into(), "residential".into());

        let key = WayMerger::get_merge_key(&tags);
        assert_eq!(key, Some("Main Street|residential".to_string()));
    }

    #[test]
    fn test_merge_key_no_name() {
        let mut tags = Tags::new();
        tags.insert("highway".into(), "residential".into());

        let key = WayMerger::get_merge_key(&tags);
        assert_eq!(key, None);
    }

    #[test]
    fn test_merge_key_motorway_excluded() {
        let mut tags = Tags::new();
        tags.insert("name".into(), "Highway 1".into());
        tags.insert("highway".into(), "motorway".into());

        let key = WayMerger::get_merge_key(&tags);
        assert_eq!(key, None);
    }

    #[test]
    fn test_is_connected() {
        let way1 = RoadWay {
            way_id: WayId(1),
            tags: Tags::new(),
            nodes: vec![1, 2, 3],
        };

        let way2 = RoadWay {
            way_id: WayId(2),
            tags: Tags::new(),
            nodes: vec![3, 4, 5], // Connected at node 3
        };

        assert!(WayMerger::is_connected_to_group(&[way1], &way2));
    }

    #[test]
    fn test_not_connected() {
        let way1 = RoadWay {
            way_id: WayId(1),
            tags: Tags::new(),
            nodes: vec![1, 2, 3],
        };

        let way2 = RoadWay {
            way_id: WayId(2),
            tags: Tags::new(),
            nodes: vec![10, 11, 12], // Not connected
        };

        assert!(!WayMerger::is_connected_to_group(&[way1], &way2));
    }
}
