use anyhow::Result;
use geo::{Coord, LineString, MultiPolygon, Polygon};
use hashbrown::{HashMap, HashSet};
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, RelationId, WayId};
use sled::Db;
use std::io::{Read, Seek};
use tempfile::Builder;
use tracing::info;

/// Manages geometry resolution for Ways and Relations
pub struct GeometryResolver {
    node_db: Db,
    way_nodes: HashMap<WayId, Vec<NodeId>>,
    relation_members: HashMap<RelationId, Vec<WayId>>,
}

impl GeometryResolver {
    /// Build the resolver by scanning the file
    pub fn build<R: Read + Seek, F>(reader: &mut OsmPbfReader<R>, filter: F) -> Result<Self>
    where
        F: Fn(&osmpbfreader::Tags) -> bool,
    {
        info!("Building geometry index...");

        // Sets of things we need
        let mut needed_relations = HashSet::new();
        let mut needed_ways = HashSet::new();
        let mut needed_nodes = HashSet::new();

        let mut relation_members_map = HashMap::new();
        let mut way_nodes_map = HashMap::new();

        // Pass 1: Scan for relevant Relations
        info!("Pass 1/3: Identifying relevant relations...");
        reader.rewind()?;
        for obj in reader.iter() {
            let obj = obj?;
            if let OsmObj::Relation(rel) = obj {
                if filter(&rel.tags) {
                    needed_relations.insert(rel.id);
                    let mut ways = Vec::new();
                    for member in &rel.refs {
                        if let osmpbfreader::OsmId::Way(way_id) = member.member {
                            if member.role == "outer" || member.role == "" {
                                ways.push(way_id);
                                needed_ways.insert(way_id);
                            }
                        }
                    }
                    relation_members_map.insert(rel.id, ways);
                }
            }
        }
        info!("Found {} relevant relations", needed_relations.len());

        // Pass 2: Ways
        info!("Pass 2/3: Identifying relevant ways...");
        reader.rewind()?;

        for obj in reader.iter() {
            let obj = obj?;
            match obj {
                OsmObj::Way(way) => {
                    // Include if it's needed by a relation OR matches filter itself
                    if needed_ways.contains(&way.id) || filter(&way.tags) {
                        needed_ways.insert(way.id);
                        way_nodes_map.insert(way.id, way.nodes.clone());
                        for node in &way.nodes {
                            needed_nodes.insert(*node);
                        }
                    }
                }
                _ => {}
            }
        }

        info!(
            "Found {} relevant ways (total), referencing {} nodes",
            needed_ways.len(),
            needed_nodes.len()
        );

        // Pass 3: Store node coordinates
        info!("Pass 3/3: Storing node coordinates...");
        reader.rewind()?;

        let temp_dir = Builder::new().prefix("cypress-geo-").tempdir()?;
        let db = sled::open(temp_dir.path())?;

        let mut stored_count = 0;

        for obj in reader.iter() {
            let obj = obj?;
            if let OsmObj::Node(node) = obj {
                if needed_nodes.contains(&node.id) {
                    let key = node.id.0.to_be_bytes();
                    let mut value = [0u8; 16];
                    value[0..8].copy_from_slice(&node.lon().to_be_bytes());
                    value[8..16].copy_from_slice(&node.lat().to_be_bytes());
                    db.insert(key, &value)?;
                    stored_count += 1;
                }
            }
        }

        db.flush()?;
        info!("Stored {} node coordinates", stored_count);

        Ok(Self {
            node_db: db,
            way_nodes: way_nodes_map,
            relation_members: relation_members_map,
        })
    }

    /// Resolve geometry for an OSM object (Relation or Way)
    pub fn resolve_boundary(&self, obj: &OsmObj) -> Option<MultiPolygon<f64>> {
        match obj {
            OsmObj::Relation(rel) => self.resolve_relation(rel.id),
            OsmObj::Way(way) => self.resolve_way(way.id).map(|p| MultiPolygon::new(vec![p])),
            _ => None,
        }
    }

    /// Resolve geometry for a Relation (Multipolygon)
    pub fn resolve_relation(&self, rel_id: RelationId) -> Option<MultiPolygon<f64>> {
        let member_ways = self.relation_members.get(&rel_id)?;

        let mut rings: Vec<Vec<Coord<f64>>> = Vec::new();

        for way_id in member_ways {
            if let Some(nodes) = self.way_nodes.get(way_id) {
                let coords: Vec<Coord<f64>> = nodes
                    .iter()
                    .filter_map(|nid| {
                        let key = nid.0.to_be_bytes();
                        match self.node_db.get(key) {
                            Ok(Some(bytes)) => {
                                if bytes.len() == 16 {
                                    let lon = f64::from_be_bytes(bytes[0..8].try_into().unwrap());
                                    let lat = f64::from_be_bytes(bytes[8..16].try_into().unwrap());
                                    Some(Coord { x: lon, y: lat })
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                if coords.len() >= 2 {
                    rings.push(coords);
                }
            }
        }

        if rings.is_empty() {
            return None;
        }

        let polygons = merge_rings_to_polygons(rings);
        if polygons.is_empty() {
            return None;
        }

        Some(MultiPolygon::new(polygons))
    }

    /// Resolve geometry for a Way
    pub fn resolve_way(&self, way_id: WayId) -> Option<Polygon<f64>> {
        let nodes = self.way_nodes.get(&way_id)?;

        let coords: Vec<Coord<f64>> = nodes
            .iter()
            .filter_map(|nid| {
                let key = nid.0.to_be_bytes();
                match self.node_db.get(key) {
                    Ok(Some(bytes)) => {
                        if bytes.len() == 16 {
                            let lon = f64::from_be_bytes(bytes[0..8].try_into().unwrap());
                            let lat = f64::from_be_bytes(bytes[8..16].try_into().unwrap());
                            Some(Coord { x: lon, y: lat })
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .collect();

        if coords.len() < 3 {
            return None;
        }

        // Close the ring if needed
        let mut ring = coords;
        if ring.first() != ring.last() {
            ring.push(ring[0]);
        }

        if ring.len() < 4 {
            return None;
        }

        Some(Polygon::new(LineString::new(ring), vec![]))
    }

    /// Get centroid for a Way
    pub fn resolve_centroid(&self, way_id: WayId) -> Option<(f64, f64)> {
        use geo::Centroid;
        let poly = self.resolve_way(way_id)?;
        poly.centroid().map(|p| (p.x(), p.y()))
    }
}

/// Merge disconnected rings into closed polygons
pub fn merge_rings_to_polygons(rings: Vec<Vec<Coord<f64>>>) -> Vec<Polygon<f64>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Coord, LineString};

    #[test]
    fn test_merge_simple_ring() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };
        // p1 again to close
        let ring = vec![p1, p2, p3, p4, p1];

        let polygons = merge_rings_to_polygons(vec![ring]);
        assert_eq!(polygons.len(), 1);
    }

    #[test]
    fn test_merge_split_ring() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };

        // Segment 1: p1 -> p2 -> p3
        let s1 = vec![p1, p2, p3];
        // Segment 2: p3 -> p4 -> p1
        let s2 = vec![p3, p4, p1];

        // Should merge
        let polygons = merge_rings_to_polygons(vec![s1, s2]);
        assert_eq!(polygons.len(), 1);
    }

    #[test]
    fn test_merge_disordered_split_ring() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };

        // Segment 1: p1 -> p2 -> p3
        let s1 = vec![p1, p2, p3];
        // Segment 2: p3 -> p4 -> p1
        let s2 = vec![p3, p4, p1];

        // Pass in s2 then s1
        let polygons = merge_rings_to_polygons(vec![s2, s1]);
        assert_eq!(polygons.len(), 1);
    }

    #[test]
    fn test_merge_gap_fails() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };
        // p5 disconnect
        let p5 = Coord { x: 2.0, y: 2.0 };

        // Segment 1: p1 -> p2
        let s1 = vec![p1, p2];
        // Segment 2: p3 -> p4
        let s2 = vec![p3, p4];

        let polygons = merge_rings_to_polygons(vec![s1, s2]);
        assert_eq!(polygons.len(), 0);
    }
}
