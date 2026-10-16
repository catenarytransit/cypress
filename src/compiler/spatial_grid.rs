//! Bounded-memory spatial-grid construction.
//!
//! The scan produces (cell_id, place_id) pairs on disk. This module external-
//! sorts fixed-size chunks, performs an N-way merge, and streams final CSR
//! arrays to temporary files before concatenating them to cypress_grid.bin.
//! No allocation scales with the total number of places.

use anyhow::{anyhow, ensure, Context, Result};
use cypress::models::spatial_grid::{
    SpatialGridView, SPATIAL_GRID_HEADER_BYTES, SPATIAL_GRID_MAGIC, SPATIAL_GRID_VERSION,
};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const PAIR_BYTES: u64 = 8;
const MAX_MERGE_FAN_IN: usize = 32;

#[derive(Debug)]
pub struct SpatialBuildStats {
    pub place_count: u64,
    pub active_cells: u64,
    pub initial_runs: usize,
    pub grid_bytes: u64,
}

pub fn write_pair<W: Write>(out: &mut W, cell: u32, place: u32) -> Result<()> {
    out.write_all(&cell.to_le_bytes())?;
    out.write_all(&place.to_le_bytes())?;
    Ok(())
}

fn read_pair<R: Read>(reader: &mut R) -> Result<Option<(u32, u32)>> {
    let mut raw = [0u8; 8];
    // Only reads complete records. All spill and sorted files have their
    // lengths validated as multiples of 8 before reaching this helper.
    let mut read_bytes = 0usize;
    while read_bytes < raw.len() {
        let n = reader.read(&mut raw[read_bytes..])?;
        if n == 0 {
            if read_bytes == 0 {
                return Ok(None);
            }
            return Err(anyhow!("truncated spatial pair"));
        }
        read_bytes += n;
    }
    Ok(Some((
        u32::from_le_bytes(raw[0..4].try_into()?),
        u32::from_le_bytes(raw[4..8].try_into()?),
    )))
}

fn verify_pair_file(path: &Path) -> Result<()> {
    ensure!(
        fs::metadata(path)?.len() % PAIR_BYTES == 0,
        "spatial pair file is not a multiple of eight bytes: {}",
        path.display()
    );
    Ok(())
}

fn sort_one_run(records: &mut Vec<(u32, u32)>, dir: &Path, serial: usize) -> Result<PathBuf> {
    records.sort_unstable();
    let path = dir.join(format!("sorted_{serial:09}.bin"));
    let mut writer = BufWriter::new(File::create(&path)?);
    for &(cell, place) in records.iter() {
        write_pair(&mut writer, cell, place)?;
    }
    writer.flush()?;
    Ok(path)
}

/// Merge sorted runs without materializing the union of their records.
fn merge_runs(paths: &[PathBuf], mut write: impl FnMut(u32, u32) -> Result<()>) -> Result<()> {
    let mut readers = paths
        .iter()
        .map(|p| File::open(p).map(BufReader::new))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(u32, u32, usize)>>::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some((cell, place)) = read_pair(reader)? {
            heap.push(Reverse((cell, place, idx)));
        }
    }

    let mut previous: Option<(u32, u32)> = None;
    while let Some(Reverse((cell, place, source))) = heap.pop() {
        if let Some(prev) = previous {
            ensure!(
                (cell, place) > prev,
                "spatial pairs are duplicated or out of order"
            );
        }
        write(cell, place)?;
        previous = Some((cell, place));
        if let Some((next_cell, next_place)) = read_pair(&mut readers[source])? {
            heap.push(Reverse((next_cell, next_place, source)));
        }
    }
    Ok(())
}

/// Multi-pass fan-in bounds both heap memory and open file descriptors.
fn coalesce_runs(mut runs: Vec<PathBuf>, dir: &Path) -> Result<Vec<PathBuf>> {
    let mut pass = 0usize;
    while runs.len() > MAX_MERGE_FAN_IN {
        let mut new_runs = Vec::new();
        for (n, group) in runs.chunks(MAX_MERGE_FAN_IN).enumerate() {
            let path = dir.join(format!("merge_{pass:04}_{n:08}.bin"));
            let mut writer = BufWriter::new(File::create(&path)?);
            merge_runs(group, |cell, place| write_pair(&mut writer, cell, place))?;
            writer.flush()?;
            new_runs.push(path);
        }
        for old in runs {
            fs::remove_file(old)?;
        }
        runs = new_runs;
        pass += 1;
    }
    Ok(runs)
}

/// Place IDs should form a permutation of [0, expected_places). We validate
/// bounds and total count here; full uniqueness follows from the scan's
/// exactly-once enumeration, not merely from sorted (cell, place) pairs.
fn build_with_pairs(
    pairs_path: &Path,
    workspace: &Path,
    output_path: &Path,
    expected_places: u32,
    chunk_capacity: usize,
) -> Result<SpatialBuildStats> {
    ensure!(chunk_capacity > 0, "spatial sort buffer must be positive");
    ensure!(
        output_path != pairs_path,
        "spatial input cannot equal output"
    );
    verify_pair_file(pairs_path)?;
    ensure!(
        fs::metadata(pairs_path)?.len() == expected_places as u64 * PAIR_BYTES,
        "spatial pair count differs from compiled place count"
    );
    fs::create_dir_all(workspace)?;
    let mut reader = BufReader::new(File::open(pairs_path)?);
    let mut records = Vec::<(u32, u32)>::with_capacity(chunk_capacity);
    let mut runs = Vec::new();
    let mut eof = false;
    while !eof {
        records.clear();
        for _ in 0..chunk_capacity {
            match read_pair(&mut reader)? {
                Some(pair) => records.push(pair),
                None => {
                    eof = true;
                    break;
                }
            }
        }
        if !records.is_empty() {
            runs.push(sort_one_run(&mut records, workspace, runs.len())?);
        }
    }
    drop(records);
    drop(reader);
    let initial_runs = runs.len();
    let runs = coalesce_runs(runs, workspace)?;

    let cells_path = workspace.join("active_cells.u32");
    let offsets_path = workspace.join("cell_offsets.u32");
    let places_path = workspace.join("cell_places.u32");
    let mut cells = BufWriter::new(File::create(&cells_path)?);
    let mut offsets = BufWriter::new(File::create(&offsets_path)?);
    let mut places = BufWriter::new(File::create(&places_path)?);
    let mut last_cell: Option<u32> = None;
    let mut place_count = 0u32;
    let mut cell_count = 0u64;
    merge_runs(&runs, |cell, place| {
        ensure!(place < expected_places, "out-of-range spatial place ID");
        if last_cell != Some(cell) {
            ensure!(
                last_cell.map_or(true, |previous| cell > previous),
                "spatial cells are not increasing"
            );
            cells.write_all(&cell.to_le_bytes())?;
            offsets.write_all(&place_count.to_le_bytes())?;
            cell_count += 1;
            last_cell = Some(cell);
        }
        places.write_all(&place.to_le_bytes())?;
        place_count = place_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("spatial place count overflow"))?;
        Ok(())
    })?;
    offsets.write_all(&place_count.to_le_bytes())?;
    cells.flush()?;
    offsets.flush()?;
    places.flush()?;
    drop(cells);
    drop(offsets);
    drop(places);
    ensure!(
        place_count == expected_places,
        "incomplete spatial grid output"
    );

    // Stream the three finalized columns to a single mmap-compatible file.
    let pending = workspace.join("cypress_grid.pending");
    let mut out = BufWriter::new(File::create(&pending)?);
    out.write_all(SPATIAL_GRID_MAGIC)?;
    out.write_all(&SPATIAL_GRID_VERSION.to_le_bytes())?;
    out.write_all(&0u32.to_le_bytes())?;
    out.write_all(&(place_count as u64).to_le_bytes())?;
    out.write_all(&cell_count.to_le_bytes())?;
    for path in [&cells_path, &offsets_path, &places_path] {
        std::io::copy(&mut BufReader::new(File::open(path)?), &mut out)?;
    }
    out.flush()?;
    drop(out);
    let bytes = fs::metadata(&pending)?.len();
    ensure!(
        bytes == SPATIAL_GRID_HEADER_BYTES as u64 + (2 * cell_count + 1 + place_count as u64) * 4,
        "invalid spatial grid output length"
    );
    let file = File::open(&pending)?;
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    let view = SpatialGridView::validate(&mmap)?;
    ensure!(
        view.place_count == expected_places as usize,
        "invalid spatial place count"
    );
    drop(mmap);
    drop(file);
    fs::rename(&pending, output_path).with_context(|| {
        format!(
            "Failed to publish staged spatial grid {}",
            output_path.display()
        )
    })?;
    for path in runs {
        fs::remove_file(path)?;
    }
    fs::remove_file(cells_path)?;
    fs::remove_file(offsets_path)?;
    fs::remove_file(places_path)?;
    Ok(SpatialBuildStats {
        place_count: place_count as u64,
        active_cells: cell_count,
        initial_runs,
        grid_bytes: bytes,
    })
}

pub fn build_spatial_grid(
    pairs_path: &Path,
    workspace: &Path,
    output_path: &Path,
    expected_places: u32,
    sort_buffer_mib: usize,
) -> Result<SpatialBuildStats> {
    ensure!(
        sort_buffer_mib > 0,
        "--grid-sort-buffer-mib must be at least one"
    );
    let cap = sort_buffer_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("sort buffer size overflow"))?
        / 8;
    build_with_pairs(pairs_path, workspace, output_path, expected_places, cap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cypress::models::spatial_grid::SpatialGridView;

    #[test]
    fn external_sort_preserves_old_order() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.bin");
        let mut writer = BufWriter::new(File::create(&input).unwrap());
        for (cell, id) in [(12, 0), (2, 1), (9, 2), (12, 3), (2, 4), (9, 5)] {
            write_pair(&mut writer, cell, id).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let output = temp.path().join("grid.bin");
        let stats = build_with_pairs(&input, &temp.path().join("scratch"), &output, 6, 2).unwrap();
        assert_eq!(stats.initial_runs, 3);
        let bytes = fs::read(&output).unwrap();
        let view = SpatialGridView::validate(&bytes).unwrap();
        assert_eq!(view.cell_places(2).unwrap().collect::<Vec<_>>(), vec![1, 4]);
        assert_eq!(view.cell_places(9).unwrap().collect::<Vec<_>>(), vec![2, 5]);
        assert_eq!(
            view.cell_places(12).unwrap().collect::<Vec<_>>(),
            vec![0, 3]
        );
    }

    #[test]
    fn merge_is_bounded_for_many_runs() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.bin");
        let mut writer = BufWriter::new(File::create(&input).unwrap());
        for id in 0..96u32 {
            write_pair(&mut writer, 7 + (id % 3), id).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let output = temp.path().join("grid.bin");
        let stats = build_with_pairs(&input, &temp.path().join("scratch"), &output, 96, 2).unwrap();
        assert!(stats.initial_runs > MAX_MERGE_FAN_IN);
        let bytes = fs::read(&output).unwrap();
        let view = SpatialGridView::validate(&bytes).unwrap();
        assert_eq!(view.cell_places(7).unwrap().len(), 32);
        assert_eq!(view.cell_places(8).unwrap().len(), 32);
        assert_eq!(view.cell_places(9).unwrap().len(), 32);
    }

    #[test]
    fn empty_grid_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.bin");
        File::create(&input).unwrap();
        let output = temp.path().join("grid.bin");
        build_with_pairs(&input, &temp.path().join("scratch"), &output, 0, 2).unwrap();
        assert_eq!(
            SpatialGridView::validate(&fs::read(output).unwrap())
                .unwrap()
                .place_count,
            0
        );
    }
}
