use anyhow::Result;
use geo::{Contains, Coord, LineString, MultiPolygon, Point, Polygon};
use hashbrown::{HashMap, HashSet};
use memmap2::Mmap;
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, RelationId, WayId};
use std::io::{BufWriter, Read, Seek, Write};
use tempfile::tempfile;
use tracing::info;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct NodeData {
    id: i64,
    lon: f64,
    lat: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingRole {
    Outer,
    Inner,
}

#[derive(Debug, Clone, Copy)]
struct RelationWayMember {
    way_id: WayId,
    role: RingRole,
}

/// Manages geometry resolution for Ways and Relations
pub struct GeometryResolver {
    nodes_mmap: Mmap,
    num_nodes: usize,
    way_nodes: HashMap<WayId, Vec<NodeId>>,
    relation_members: HashMap<RelationId, Vec<RelationWayMember>>,
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
                            let role = if member.role == "inner" {
                                RingRole::Inner
                            } else if member.role == "outer" || member.role == "" {
                                // Empty roles are commonly used as outer members on
                                // boundary relations. Unknown roles such as subarea,
                                // admin_centre, and label are not geometry members.
                                RingRole::Outer
                            } else {
                                continue;
                            };

                            ways.push(RelationWayMember { way_id, role });
                            needed_ways.insert(way_id);
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

        let mut file = tempfile()?;
        let mut writer = BufWriter::new(&mut file);
        let mut stored_count = 0;

        let mut sorted = true;
        let mut last_id = i64::MIN;

        for obj in reader.iter() {
            let obj = obj?;
            if let OsmObj::Node(node) = obj {
                if needed_nodes.contains(&node.id) {
                    let id = node.id.0;
                    if id < last_id {
                        sorted = false;
                    }
                    last_id = id;

                    let data = NodeData {
                        id,
                        lon: node.lon(),
                        lat: node.lat(),
                    };

                    // Safety: NodeData is Repr(C) and contains only plain data types (i64, f64)
                    // We write the raw bytes directly to the file
                    let bytes = unsafe {
                        std::slice::from_raw_parts(
                            &data as *const NodeData as *const u8,
                            std::mem::size_of::<NodeData>(),
                        )
                    };
                    writer.write_all(bytes)?;
                    stored_count += 1;
                }
            }
        }

        writer.flush()?;
        drop(writer); // Drop writer to release borrow on file

        // Memory map the file
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        // Ensure we have complete records
        let struct_size = std::mem::size_of::<NodeData>();
        assert_eq!(
            mmap.len() % struct_size,
            0,
            "File size must be multiple of struct size"
        );

        if !sorted && stored_count > 0 {
            info!("Node data not sorted, sorting in-place...");
            let slice: &mut [NodeData] = unsafe {
                std::slice::from_raw_parts_mut(
                    mmap.as_mut_ptr() as *mut NodeData,
                    mmap.len() / struct_size,
                )
            };
            slice.sort_unstable_by_key(|n| n.id);
        }

        let mmap = mmap.make_read_only()?;

        info!(
            "Stored {} node coordinates using {} bytes",
            stored_count,
            mmap.len()
        );

        Ok(Self {
            nodes_mmap: mmap,
            num_nodes: stored_count,
            way_nodes: way_nodes_map,
            relation_members: relation_members_map,
        })
    }

    /// Helper to get node coordinates
    pub fn get_node_coords(&self, node_id: NodeId) -> Option<Coord<f64>> {
        let slice: &[NodeData] = unsafe {
            std::slice::from_raw_parts(self.nodes_mmap.as_ptr() as *const NodeData, self.num_nodes)
        };

        if let Ok(idx) = slice.binary_search_by_key(&node_id.0, |n| n.id) {
            let node = &slice[idx];
            Some(Coord {
                x: node.lon,
                y: node.lat,
            })
        } else {
            None
        }
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

        let mut outer_segments: Vec<Vec<NodeId>> = Vec::new();
        let mut inner_segments: Vec<Vec<NodeId>> = Vec::new();

        for member in member_ways {
            // A multipolygon relation is only valid if all geometry members and
            // all of their referenced nodes are available. Do not silently build
            // a partial country boundary from a clipped or stale PBF.
            let nodes = self.way_nodes.get(&member.way_id)?;
            if nodes.len() < 2 {
                return None;
            }

            match member.role {
                RingRole::Outer => outer_segments.push(nodes.clone()),
                RingRole::Inner => inner_segments.push(nodes.clone()),
            }
        }

        if outer_segments.is_empty() {
            return None;
        }

        // OSM multipolygon rings are defined by shared node IDs. Assemble them
        // topologically first, then resolve coordinates. Crucially, an open chain
        // is invalid and must never be closed by inventing a straight segment.
        let outer_node_rings = merge_way_segments_to_rings(outer_segments)?;
        let inner_node_rings = merge_way_segments_to_rings(inner_segments)?;

        let outer_rings: Vec<LineString<f64>> = outer_node_rings
            .into_iter()
            .map(|ring| self.node_ring_to_linestring(ring))
            .collect::<Option<Vec<_>>>()?;
        let inner_rings: Vec<LineString<f64>> = inner_node_rings
            .into_iter()
            .map(|ring| self.node_ring_to_linestring(ring))
            .collect::<Option<Vec<_>>>()?;

        let polygons = polygons_from_rings(outer_rings, inner_rings)?;
        Some(MultiPolygon::new(polygons))
    }

    fn node_ring_to_linestring(&self, ring: Vec<NodeId>) -> Option<LineString<f64>> {
        if ring.len() < 4 || ring.first() != ring.last() {
            return None;
        }

        let coords = ring
            .into_iter()
            .map(|node_id| self.get_node_coords(node_id))
            .collect::<Option<Vec<_>>>()?;

        Some(LineString::new(coords))
    }

    /// Resolve geometry for a Way
    pub fn resolve_way(&self, way_id: WayId) -> Option<Polygon<f64>> {
        let nodes = self.way_nodes.get(&way_id)?;

        // OSM closed ways are closed by repeating the same node ID, not merely
        // by having two distinct nodes at identical coordinates.
        if nodes.len() < 4 || nodes.first() != nodes.last() {
            return None;
        }

        let coords: Vec<Coord<f64>> = nodes
            .iter()
            .map(|nid| self.get_node_coords(*nid))
            .collect::<Option<Vec<_>>>()?;

        Some(Polygon::new(LineString::new(coords), vec![]))
    }

    /// Get centroid for a Way
    pub fn resolve_centroid(&self, way_id: WayId) -> Option<(f64, f64)> {
        use geo::Centroid;
        let poly = self.resolve_way(way_id)?;
        poly.centroid().map(|p| (p.x(), p.y()))
    }
}

/// Assemble OSM way members into naturally closed rings using node IDs.
///
/// Returns `None` when a chain cannot be closed or when more than one way can
/// continue from the same endpoint. Rejecting ambiguous/incomplete topology is
/// safer than manufacturing a polygon that can cover the wrong country.
fn merge_way_segments_to_rings(mut segments: Vec<Vec<NodeId>>) -> Option<Vec<Vec<NodeId>>> {
    let mut rings = Vec::new();

    // Closed ways are already complete rings. Pull them out first so they cannot
    // accidentally be joined to an open chain that happens to share a node.
    let mut i = 0;
    while i < segments.len() {
        if segments[i].len() < 2 {
            return None;
        }
        if segments[i].first() == segments[i].last() {
            let ring = segments.remove(i);
            if ring.len() < 4 {
                return None;
            }
            rings.push(ring);
        } else {
            i += 1;
        }
    }

    while !segments.is_empty() {
        let mut current = segments.remove(0);

        loop {
            if current.first() == current.last() {
                if current.len() < 4 {
                    return None;
                }
                rings.push(current);
                break;
            }

            let start = *current.first()?;
            let end = *current.last()?;

            // Prefer extending the end of the chain. A single matching way at the
            // start is also valid; multiple matches at the same endpoint are
            // ambiguous and should not be guessed.
            let mut append_match: Option<(usize, bool)> = None;
            for (idx, segment) in segments.iter().enumerate() {
                let seg_start = *segment.first()?;
                let seg_end = *segment.last()?;
                let reverse = if end == seg_start {
                    Some(false)
                } else if end == seg_end {
                    Some(true)
                } else {
                    None
                };

                if let Some(reverse) = reverse {
                    if append_match.is_some() {
                        return None;
                    }
                    append_match = Some((idx, reverse));
                }
            }

            if let Some((idx, reverse)) = append_match {
                let mut segment = segments.remove(idx);
                if reverse {
                    segment.reverse();
                }
                segment.remove(0); // duplicate shared endpoint
                current.extend(segment);
                continue;
            }

            let mut prepend_match: Option<(usize, bool)> = None;
            for (idx, segment) in segments.iter().enumerate() {
                let seg_start = *segment.first()?;
                let seg_end = *segment.last()?;
                let reverse = if start == seg_end {
                    Some(false)
                } else if start == seg_start {
                    Some(true)
                } else {
                    None
                };

                if let Some(reverse) = reverse {
                    if prepend_match.is_some() {
                        return None;
                    }
                    prepend_match = Some((idx, reverse));
                }
            }

            if let Some((idx, reverse)) = prepend_match {
                let mut segment = segments.remove(idx);
                if reverse {
                    segment.reverse();
                }
                segment.pop(); // duplicate shared endpoint
                segment.extend(current);
                current = segment;
                continue;
            }

            // No member continues this chain: it is an open ring. OSM does not
            // permit us to invent an edge from the end back to the beginning.
            return None;
        }
    }

    Some(rings)
}

/// Build polygons from assembled outer and inner OSM rings.
fn polygons_from_rings(
    outer_rings: Vec<LineString<f64>>,
    inner_rings: Vec<LineString<f64>>,
) -> Option<Vec<Polygon<f64>>> {
    if outer_rings.is_empty() {
        return None;
    }

    let mut polygons: Vec<Polygon<f64>> = outer_rings
        .into_iter()
        .map(|outer| Polygon::new(outer, vec![]))
        .collect();

    for inner in inner_rings {
        let sample = inner.0.first()?;
        let point = Point::new(sample.x, sample.y);

        let containing: Vec<usize> = polygons
            .iter()
            .enumerate()
            .filter(|(_, polygon)| polygon.contains(&point))
            .map(|(idx, _)| idx)
            .collect();

        // A valid inner ring belongs to exactly one outer ring.
        if containing.len() != 1 {
            return None;
        }

        let idx = containing[0];
        let exterior = polygons[idx].exterior().clone();
        let mut interiors = polygons[idx].interiors().to_vec();
        interiors.push(inner);
        polygons[idx] = Polygon::new(exterior, interiors);
    }

    Some(polygons)
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

        // Only accept a ring that actually closes. Never synthesize the
        // final edge: doing so can turn a clipped country boundary into a huge
        // false polygon that contains neighbouring cities.
        if current.first() == current.last() && current.len() >= 4 {
            let line_string = LineString::new(current);
            result.push(Polygon::new(line_string, vec![]));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Contains, Coord, LineString, Point};

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
    fn test_open_chain_is_not_force_closed() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };

        // This used to become p1 -> p2 -> p3 -> p4 -> p1 by inventing
        // the final edge. An open OSM ring must be rejected instead.
        let polygons = merge_rings_to_polygons(vec![vec![p1, p2, p3, p4]]);
        assert!(polygons.is_empty());
    }

    #[test]
    fn test_node_id_ring_assembly_succeeds() {
        let a = NodeId(1);
        let b = NodeId(2);
        let c = NodeId(3);
        let d = NodeId(4);

        let rings = merge_way_segments_to_rings(vec![vec![a, b, c], vec![c, d, a]]).unwrap();
        assert_eq!(rings, vec![vec![a, b, c, d, a]]);
    }

    #[test]
    fn test_node_id_ring_assembly_rejects_gap() {
        let a = NodeId(1);
        let b = NodeId(2);
        let c = NodeId(3);
        let d = NodeId(4);

        assert!(merge_way_segments_to_rings(vec![vec![a, b, c], vec![d, a]]).is_none());
    }

    #[test]
    fn test_inner_ring_becomes_hole() {
        let outer = LineString::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
            Coord { x: 0.0, y: 10.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        let inner = LineString::new(vec![
            Coord { x: 4.0, y: 4.0 },
            Coord { x: 6.0, y: 4.0 },
            Coord { x: 6.0, y: 6.0 },
            Coord { x: 4.0, y: 6.0 },
            Coord { x: 4.0, y: 4.0 },
        ]);

        let polygons = polygons_from_rings(vec![outer], vec![inner]).unwrap();
        assert_eq!(polygons.len(), 1);
        assert!(polygons[0].contains(&Point::new(2.0, 2.0)));
        assert!(!polygons[0].contains(&Point::new(5.0, 5.0)));
    }

    #[test]
    fn test_merge_gap_fails() {
        let p1 = Coord { x: 0.0, y: 0.0 };
        let p2 = Coord { x: 1.0, y: 0.0 };
        let p3 = Coord { x: 1.0, y: 1.0 };
        let p4 = Coord { x: 0.0, y: 1.0 };
        // p5 disconnect
        let _p5 = Coord { x: 2.0, y: 2.0 };

        // Segment 1: p1 -> p2
        let s1 = vec![p1, p2];
        // Segment 2: p3 -> p4
        let s2 = vec![p3, p4];

        let polygons = merge_rings_to_polygons(vec![s1, s2]);
        assert_eq!(polygons.len(), 0);
    }
}
