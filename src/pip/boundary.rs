//! Admin boundary extraction from OSM data.

use geo::{Coord, LineString, MultiPolygon, Polygon};
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, RelationId, WayId};
use std::collections::HashMap;
use std::io::{Read, Seek};
use tracing::{debug, info};

use crate::models::{AdminArea, AdminLevel};

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
/// This performs a two-pass read:
/// 1. First pass: collect admin relations and their way/node references
/// 2. Second pass: resolve geometries
pub fn extract_admin_boundaries<R: Read + Seek>(
    reader: &mut OsmPbfReader<R>,
) -> anyhow::Result<Vec<AdminBoundary>> {
    info!("Extracting admin boundaries from OSM data...");

    // First pass: collect admin relations
    let mut admin_relations: HashMap<RelationId, AdminRelationData> = HashMap::new();
    let mut way_refs: HashMap<WayId, Vec<NodeId>> = HashMap::new();
    let mut needed_ways: hashbrown::HashSet<WayId> = hashbrown::HashSet::new();
    let mut needed_nodes: hashbrown::HashSet<NodeId> = hashbrown::HashSet::new();

    info!("Pass 1: Collecting admin relations...");
    reader.rewind()?;

    for obj in reader.iter() {
        let obj = obj?;
        if let OsmObj::Relation(rel) = obj {
            // Check if this is an admin boundary
            let is_admin = rel
                .tags
                .get("boundary")
                .map(|v| v == "administrative")
                .unwrap_or(false);

            if !is_admin {
                continue;
            }

            // Get admin level
            let level_str = match rel.tags.get("admin_level") {
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

            // Build AdminArea with multilingual names
            let mut area = AdminArea::new(rel.id.0, level);

            for (key, value) in rel.tags.iter() {
                if key == "name" {
                    area.name.insert("default".to_string(), value.to_string());
                } else if let Some(lang) = key.strip_prefix("name:") {
                    area.name.insert(lang.to_string(), value.to_string());
                } else if key == "short_name"
                    || key == "ISO3166-1:alpha2"
                    || key == "ISO3166-1:alpha3"
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

            // Collect way references (outer members)
            let mut ways = Vec::new();
            for member in &rel.refs {
                if let osmpbfreader::OsmId::Way(way_id) = member.member {
                    if member.role == "outer" || member.role == "" {
                        ways.push(way_id);
                        needed_ways.insert(way_id);
                    }
                }
            }

            admin_relations.insert(rel.id, AdminRelationData { area, ways });
        }
    }

    info!(
        "Found {} admin relations, need {} ways",
        admin_relations.len(),
        needed_ways.len()
    );

    // Second pass: collect way geometries
    info!("Pass 2: Collecting way geometries...");
    reader.rewind()?;

    for obj in reader.iter() {
        let obj = obj?;
        if let OsmObj::Way(way) = obj {
            if needed_ways.contains(&way.id) {
                let nodes: Vec<NodeId> = way.nodes.clone();
                for node_id in &nodes {
                    needed_nodes.insert(*node_id);
                }
                way_refs.insert(way.id, nodes);
            }
        }
    }

    info!(
        "Collected {} ways, need {} nodes",
        way_refs.len(),
        needed_nodes.len()
    );

    // Third pass: collect node coordinates
    info!("Pass 3: Collecting node coordinates...");
    reader.rewind()?;

    let mut node_coords: HashMap<NodeId, (f64, f64)> = HashMap::new();

    for obj in reader.iter() {
        let obj = obj?;
        if let OsmObj::Node(node) = obj {
            if needed_nodes.contains(&node.id) {
                node_coords.insert(node.id, (node.lon(), node.lat()));
            }
        }
    }

    info!("Collected {} node coordinates", node_coords.len());

    // Build geometries
    info!("Building boundary geometries...");
    let mut boundaries = Vec::new();

    for (rel_id, data) in admin_relations {
        // Collect all coordinates for outer ways
        let mut rings: Vec<Vec<Coord<f64>>> = Vec::new();

        for way_id in &data.ways {
            if let Some(node_ids) = way_refs.get(way_id) {
                let coords: Vec<Coord<f64>> = node_ids
                    .iter()
                    .filter_map(|nid| node_coords.get(nid))
                    .map(|(lon, lat)| Coord { x: *lon, y: *lat })
                    .collect();

                if coords.len() >= 3 {
                    rings.push(coords);
                }
            }
        }

        if rings.is_empty() {
            debug!("No valid rings for relation {}", rel_id.0);
            continue;
        }

        // Try to merge rings into closed polygons
        let polygons = merge_rings_to_polygons(rings);

        if polygons.is_empty() {
            debug!("Could not create polygons for relation {}", rel_id.0);
            continue;
        }

        let multi_polygon = MultiPolygon::new(polygons);

        boundaries.push(AdminBoundary {
            area: data.area,
            geometry: multi_polygon,
        });
    }

    info!("Built {} admin boundaries", boundaries.len());

    // Sort by admin level (country first)
    boundaries.sort_by(|a, b| a.area.level.cmp(&b.area.level));

    Ok(boundaries)
}

struct AdminRelationData {
    area: AdminArea,
    ways: Vec<WayId>,
}

/// Merge disconnected rings into closed polygons
fn merge_rings_to_polygons(rings: Vec<Vec<Coord<f64>>>) -> Vec<Polygon<f64>> {
    let mut result = Vec::new();
    let mut remaining: Vec<Vec<Coord<f64>>> = rings;

    while !remaining.is_empty() {
        let mut current = remaining.remove(0);

        // Check if already closed
        if current.first() == current.last() && current.len() >= 4 {
            let line_string = LineString::new(current);
            result.push(Polygon::new(line_string, vec![]));
            continue;
        }

        // Try to merge with other rings
        let mut merged = true;
        while merged && !remaining.is_empty() {
            merged = false;

            let current_start = current.first().cloned();
            let current_end = current.last().cloned();

            for i in 0..remaining.len() {
                let ring = &remaining[i];
                let ring_start = ring.first().cloned();
                let ring_end = ring.last().cloned();

                // Check if can connect
                if current_end == ring_start {
                    let mut ring = remaining.remove(i);
                    ring.remove(0); // Remove duplicate point
                    current.extend(ring);
                    merged = true;
                    break;
                } else if current_end == ring_end {
                    let mut ring = remaining.remove(i);
                    ring.reverse();
                    ring.remove(0);
                    current.extend(ring);
                    merged = true;
                    break;
                } else if current_start == ring_end {
                    let mut ring = remaining.remove(i);
                    ring.pop();
                    ring.extend(current);
                    current = ring;
                    merged = true;
                    break;
                } else if current_start == ring_start {
                    let mut ring = remaining.remove(i);
                    ring.reverse();
                    ring.pop();
                    ring.extend(current);
                    current = ring;
                    merged = true;
                    break;
                }
            }
        }

        // Close the ring if possible
        if current.len() >= 3 {
            if current.first() != current.last() {
                current.push(current[0]);
            }
            if current.len() >= 4 {
                let line_string = LineString::new(current);
                result.push(Polygon::new(line_string, vec![]));
            }
        }
    }

    result
}
