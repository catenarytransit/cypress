//! Nearest-first lookup over occupied grid cells. Only the cell directory is
//! resident: coordinates and postings remain in the generation's mmap files.
use cypress::models::memdb::CypressMemDb;
use rstar::{PointDistance, RTree, RTreeObject, AABB};
use std::collections::BinaryHeap;

const EARTH_KM: f64 = 6371.0;

pub fn unit_point(lat: f64, lon: f64) -> [f64; 3] {
    let (lat, lon) = (lat.to_radians(), lon.to_radians());
    [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
}

pub struct Cell {
    pub id: u32,
    bounds: AABB<[f64; 3]>,
}
impl Cell {
    pub fn new(id: u32) -> Self {
        let row = id as usize / CypressMemDb::GRID_COLS;
        let col = id as usize % CypressMemDb::GRID_COLS;
        let size = CypressMemDb::GRID_CELL_SIZE as f64;
        // Include rounding in f32 coord_to_cell, then evaluate extrema of
        // sin/cos at the interval endpoints and cardinal angles. Cartesian
        // bounds are tight even for cells crossing the date line or poles.
        let lat_lo = (row as f64 * size - 90.0 - 0.0001).max(-90.0);
        let lat_hi = ((row + 1) as f64 * size - 90.0 + 0.0001).min(90.0);
        let lon_lo = col as f64 * size - 180.0 - 0.0001;
        let lon_hi = (col + 1) as f64 * size - 180.0 + 0.0001;
        let lats = [lat_lo, lat_hi, 0.0f64.clamp(lat_lo, lat_hi)];
        let lons = [
            lon_lo,
            lon_hi,
            (-180.0f64).clamp(lon_lo, lon_hi),
            (-90.0f64).clamp(lon_lo, lon_hi),
            0.0f64.clamp(lon_lo, lon_hi),
            90.0f64.clamp(lon_lo, lon_hi),
            180.0f64.clamp(lon_lo, lon_hi),
        ];
        let mut lower = [f64::INFINITY; 3];
        let mut upper = [f64::NEG_INFINITY; 3];
        for lat in lats {
            for &lon in &lons {
                let point = unit_point(lat, lon);
                for axis in 0..3 {
                    lower[axis] = lower[axis].min(point[axis] - 1e-15);
                    upper[axis] = upper[axis].max(point[axis] + 1e-15);
                }
            }
        }
        Self {
            id,
            bounds: AABB::from_corners(lower, upper),
        }
    }
}
impl RTreeObject for Cell {
    type Envelope = AABB<[f64; 3]>;
    fn envelope(&self) -> Self::Envelope {
        self.bounds
    }
}
impl PointDistance for Cell {
    fn distance_2(&self, point: &[f64; 3]) -> f64 {
        self.bounds.distance_2(point)
    }
}

// BinaryHeap keeps the farthest retained point at the root. Distances are
// finite because invalid coordinates are rejected before insertion.
#[derive(Debug, PartialEq)]
struct Hit {
    distance: f64,
    id: u32,
}
impl Eq for Hit {}
impl PartialOrd for Hit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Hit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.id.cmp(&other.id))
    }
}

pub fn nearest(
    tree: &RTree<Cell>,
    lat: f64,
    lon: f64,
    limit: usize,
    radius_km: f64,
    mut visit: impl FnMut(u32, &mut dyn FnMut(u32, f64, f64)),
) -> Vec<(u32, f64)> {
    if limit == 0 || !lat.is_finite() || !lon.is_finite() {
        return Vec::new();
    }
    let query = unit_point(lat, lon);
    let radius_squared = (2.0 * (radius_km / (2.0 * EARTH_KM)).sin()).powi(2);
    let mut hits = BinaryHeap::<Hit>::with_capacity(limit);
    let mut bound = radius_squared;
    for (cell, distance) in tree.nearest_neighbor_iter_with_distance_2(&query) {
        if distance > bound {
            break;
        }
        visit(cell.id, &mut |id, lat, lon| {
            if !lat.is_finite() || !lon.is_finite() {
                return;
            }
            let point = unit_point(lat, lon);
            let distance = (0..3).map(|i| (query[i] - point[i]).powi(2)).sum::<f64>();
            if distance <= radius_squared {
                let hit = Hit { distance, id };
                if hits.len() < limit {
                    hits.push(hit);
                } else if hits.peek().is_some_and(|worst| hit < *worst) {
                    hits.pop();
                    hits.push(hit);
                }
            }
        });
        if hits.len() == limit {
            bound = hits.peek().unwrap().distance.min(radius_squared);
        }
    }
    hits.into_sorted_vec()
        .into_iter()
        .map(|hit| {
            (
                hit.id,
                2.0 * EARTH_KM * (hit.distance.sqrt() / 2.0).min(1.0).asin(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn check(points: &[(f32, f32)], queries: &[(f64, f64)]) {
        let mut cells = BTreeMap::<u32, Vec<u32>>::new();
        for (i, &(lat, lon)) in points.iter().enumerate() {
            cells
                .entry(CypressMemDb::coord_to_cell(lat, lon))
                .or_default()
                .push(i as u32);
        }
        let tree = RTree::bulk_load(cells.keys().map(|&id| Cell::new(id)).collect());
        // Every indexed coordinate must be inside its conservative cell bound,
        // including f32 rounding and cells at the poles/date line.
        for (&cell, ids) in &cells {
            let envelope = Cell::new(cell).bounds;
            for &id in ids {
                let (lat, lon) = points[id as usize];
                assert_eq!(
                    envelope.distance_2(&unit_point(lat as f64, lon as f64)),
                    0.0
                );
            }
        }
        for &(lat, lon) in queries {
            for limit in [0, 1, 10, 40, points.len() + 1] {
                let actual = nearest(&tree, lat, lon, limit, 1.0, |cell, emit| {
                    for &id in &cells[&cell] {
                        let (lat, lon) = points[id as usize];
                        emit(id, lat as f64, lon as f64);
                    }
                });
                let mut expected: Vec<_> = points
                    .iter()
                    .enumerate()
                    .filter_map(|(id, &(p_lat, p_lon))| {
                        let distance = super::super::search::haversine_distance_km(
                            (lat, lon),
                            (p_lat as f64, p_lon as f64),
                        );
                        (distance <= 1.0).then_some((id as u32, distance))
                    })
                    .collect();
                expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                expected.truncate(limit);
                assert_eq!(actual.len(), expected.len());
                // At the poles, floating point evaluation can order effectively
                // equidistant coordinates differently in chord and haversine
                // formulas. Compare distances, allowing only sub-millimetre ties.
                for (a, b) in actual.iter().zip(&expected) {
                    assert!((a.1 - b.1).abs() < 1e-8);
                    if a.0 != b.0 {
                        assert!((a.1 - b.1).abs() < 1e-10, "different non-tied neighbors");
                    }
                }
            }
        }
    }

    #[test]
    fn matches_brute_force_at_date_line_poles_and_cell_edges() {
        let mut points = vec![
            (0.0, 180.0),
            (0.0, -180.0),
            (90.0, 0.0),
            (-90.0, 0.0),
            (0.0, 0.0),
        ];
        let mut seed = 5u64;
        for (lat, lon) in [
            (47.37, 8.54),
            (80.0, 5.0),
            (0.0, 179.999),
            (89.999, 10.0),
            (-89.999, -10.0),
        ] {
            for _ in 0..300 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let dy = (seed >> 32) as f64 / u32::MAX as f64 * 0.04 - 0.02;
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let dx = (seed >> 32) as f64 / u32::MAX as f64 * 0.2 - 0.1;
                points.push((
                    (lat + dy).clamp(-90.0, 90.0) as f32,
                    ((lon + dx + 180.0).rem_euclid(360.0) - 180.0) as f32,
                ));
            }
        }
        check(
            &points,
            &[
                (47.37, 8.54),
                (80.0, 5.0),
                (0.0, 180.0),
                (90.0, 10.0),
                (-90.0, -10.0),
                (0.0, 0.0),
            ],
        );
        check(&[], &[(0.0, 0.0)]);
    }

    #[test]
    fn stops_before_visiting_distant_cells() {
        let tree = RTree::bulk_load(vec![
            Cell::new(CypressMemDb::coord_to_cell(0.0, 0.0)),
            Cell::new(CypressMemDb::coord_to_cell(50.0, 50.0)),
        ]);
        let mut visited = 0;
        let hits = nearest(&tree, 0.0, 0.0, 1, 1.0, |_, emit| {
            visited += 1;
            emit(0, 0.0, 0.0);
        });
        assert_eq!(visited, 1);
        assert_eq!(hits, vec![(0, 0.0)]);
    }
}

#[cfg(test)]
mod benchmark {
    use super::*;
    use std::{collections::BTreeMap, time::Instant};

    /// Synthetic warm spatial-only comparison; excludes Scylla and cold mmap I/O.
    #[test]
    #[ignore]
    fn dense_city_lookup() {
        let mut cells = BTreeMap::<u32, Vec<(u32, f32, f32)>>::new();
        for id in 0..500_000u32 {
            let lat = 47.32 + (id / 1000) as f32 * 0.0002;
            let lon = 8.49 + (id % 1000) as f32 * 0.0001;
            cells
                .entry(CypressMemDb::coord_to_cell(lat, lon))
                .or_default()
                .push((id, lat, lon));
        }
        let tree = RTree::bulk_load(cells.keys().map(|&id| Cell::new(id)).collect());
        let (lat, lon) = (47.3703, 8.5403);
        let mut new_count = 0;
        let started = Instant::now();
        let mut actual = Vec::new();
        for _ in 0..100 {
            actual = nearest(&tree, lat, lon, 10, 1.0, |cell, emit| {
                for &(id, lat, lon) in &cells[&cell] {
                    new_count += 1;
                    emit(id, lat as f64, lon as f64);
                }
            });
            std::hint::black_box(&actual);
        }
        let new_elapsed = started.elapsed();
        let mut old_count = 0;
        let started = Instant::now();
        let mut expected = Vec::new();
        for _ in 0..100 {
            expected.clear();
            let delta = (1.0 / 111.0) as f32;
            let start = CypressMemDb::coord_to_cell(lat as f32 - delta, lon as f32 - delta);
            let end = CypressMemDb::coord_to_cell(lat as f32 + delta, lon as f32 + delta);
            for row in
                start as usize / CypressMemDb::GRID_COLS..=end as usize / CypressMemDb::GRID_COLS
            {
                for col in start as usize % CypressMemDb::GRID_COLS
                    ..=end as usize % CypressMemDb::GRID_COLS
                {
                    if let Some(points) = cells.get(&((row * CypressMemDb::GRID_COLS + col) as u32))
                    {
                        for &(id, p_lat, p_lon) in points {
                            old_count += 1;
                            let distance = super::super::search::haversine_distance_km(
                                (lat, lon),
                                (p_lat as f64, p_lon as f64),
                            );
                            if distance <= 1.0 {
                                expected.push((id, distance));
                            }
                        }
                    }
                }
            }
            expected.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            expected.truncate(10);
            std::hint::black_box(&expected);
        }
        let old_elapsed = started.elapsed();
        assert_eq!(
            actual.iter().map(|v| v.0).collect::<Vec<_>>(),
            expected.iter().map(|v| v.0).collect::<Vec<_>>()
        );
        eprintln!("100 queries: old={old_elapsed:?} ({old_count} points), R-tree={new_elapsed:?} ({new_count} points)");
    }
}
