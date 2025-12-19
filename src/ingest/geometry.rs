use anyhow::Result;
use geo::{Coord, LineString, Polygon};
use hashbrown::{HashMap, HashSet};
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, WayId};
use sled::Db;
use std::io::{Read, Seek};
use tempfile::Builder;
use tracing::info;

/// Manages geometry resolution for Ways and Relations
pub struct GeometryResolver {
    node_db: Db,
    // For ways, we store their nodes. For relations, we might need more complex handling.
    // For now, let's focus on Ways (Buildings, Parks) which are the most common skipped items.
    // Relations (Multipolygons) are harder but we can try.

    // We store way->nodes in memory. If this is too big, could use sled too, but usually it fits.
    way_nodes: HashMap<WayId, Vec<NodeId>>,
}

impl GeometryResolver {
    /// Build the resolver by scanning the file
    pub fn build<R: Read + Seek, F>(reader: &mut OsmPbfReader<R>, filter: F) -> Result<Self>
    where
        F: Fn(&osmpbfreader::Tags) -> bool,
    {
        info!("Building geometry index...");

        // Sets of things we need
        let mut needed_ways = HashSet::new();
        let mut needed_nodes = HashSet::new();

        // Pass 1: Scan for relevant Ways and their dependencies
        info!("Pass 1/3: Identifying relevant ways...");
        reader.rewind()?;

        let mut way_nodes_map = HashMap::new();

        for obj in reader.iter() {
            let obj = obj?;
            match obj {
                OsmObj::Way(way) => {
                    if filter(&way.tags) {
                        needed_ways.insert(way.id);
                        way_nodes_map.insert(way.id, way.nodes.clone());
                        for node in &way.nodes {
                            needed_nodes.insert(*node);
                        }
                    }
                }
                // TODO: Relations
                _ => {}
            }
        }

        info!(
            "Found {} relevant ways, referencing {} nodes",
            needed_ways.len(),
            needed_nodes.len()
        );

        // Pass 2: Store node coordinates
        info!("Pass 2/3: Storing node coordinates...");
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
        })
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
