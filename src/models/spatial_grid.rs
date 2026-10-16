//! Read-only mmap view of a disk-backed CSR spatial grid.
use anyhow::{anyhow, ensure, Result};

pub const SPATIAL_GRID_FILE: &str = "cypress_grid.bin";
pub const SPATIAL_GRID_MAGIC: &[u8; 8] = b"CYSGRID1";
pub const SPATIAL_GRID_VERSION: u32 = 1;
pub const SPATIAL_GRID_HEADER_BYTES: usize = 32;

fn at(bytes: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap())
}

/// One file: header, sorted active cells, CSR offsets and ordered place IDs.
pub struct SpatialGridView<'a> {
    cells: &'a [u8],
    offsets: &'a [u8],
    places: &'a [u8],
    pub cell_count: usize,
    pub place_count: usize,
}

impl<'a> SpatialGridView<'a> {
    /// Constant-time, borrow-only view of a file validated during load.
    pub fn from_validated(data: &'a [u8]) -> Result<Self> {
        ensure!(
            data.len() >= SPATIAL_GRID_HEADER_BYTES,
            "grid header truncated"
        );
        ensure!(
            &data[..8] == SPATIAL_GRID_MAGIC,
            "spatial grid magic mismatch"
        );
        ensure!(
            u32::from_le_bytes(data[8..12].try_into()?) == SPATIAL_GRID_VERSION,
            "unsupported spatial grid version"
        );
        ensure!(data[12..16] == [0; 4], "unknown grid flags");
        let place_count = usize::try_from(u64::from_le_bytes(data[16..24].try_into()?))?;
        let cell_count = usize::try_from(u64::from_le_bytes(data[24..32].try_into()?))?;
        ensure!(
            place_count <= u32::MAX as usize,
            "spatial place count exceeds u32"
        );
        let cell_bytes = cell_count
            .checked_mul(4)
            .ok_or_else(|| anyhow!("cell length overflow"))?;
        let offset_bytes = cell_count
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| anyhow!("offset length overflow"))?;
        let place_bytes = place_count
            .checked_mul(4)
            .ok_or_else(|| anyhow!("place length overflow"))?;
        let cell_start = SPATIAL_GRID_HEADER_BYTES;
        let off_start = cell_start
            .checked_add(cell_bytes)
            .ok_or_else(|| anyhow!("grid overflow"))?;
        let place_start = off_start
            .checked_add(offset_bytes)
            .ok_or_else(|| anyhow!("grid overflow"))?;
        let expected = place_start
            .checked_add(place_bytes)
            .ok_or_else(|| anyhow!("grid overflow"))?;
        ensure!(
            expected == data.len(),
            "spatial grid section lengths mismatch"
        );
        Ok(Self {
            cells: &data[cell_start..off_start],
            offsets: &data[off_start..place_start],
            places: &data[place_start..],
            cell_count,
            place_count,
        })
    }

    /// Check directory ordering and bounds once at generation load.
    pub fn validate(data: &'a [u8]) -> Result<Self> {
        let grid = Self::from_validated(data)?;
        ensure!(at(grid.offsets, 0) == 0, "first cell offset must be zero");
        ensure!(
            at(grid.offsets, grid.cell_count) as usize == grid.place_count,
            "last cell offset must equal place count"
        );
        let mut previous_cell = None;
        let mut previous_offset = 0;
        for i in 0..grid.cell_count {
            let cell = at(grid.cells, i);
            if let Some(prev) = previous_cell {
                ensure!(cell > prev, "spatial cells must be strictly increasing");
            }
            let next = at(grid.offsets, i + 1);
            ensure!(next > previous_offset, "empty or invalid spatial cell");
            ensure!(
                next as usize <= grid.place_count,
                "spatial offset out of range"
            );
            previous_cell = Some(cell);
            previous_offset = next;
        }
        ensure!(
            grid.cell_count > 0 || grid.place_count == 0,
            "nonempty spatial index without cells"
        );
        Ok(grid)
    }

    pub fn cell_places(&self, wanted: u32) -> Option<SpatialPlaceIter<'a>> {
        let mut low = 0;
        let mut high = self.cell_count;
        while low < high {
            let mid = low + (high - low) / 2;
            if at(self.cells, mid) < wanted {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if low >= self.cell_count || at(self.cells, low) != wanted {
            return None;
        }
        let start = at(self.offsets, low) as usize * 4;
        let end = at(self.offsets, low + 1) as usize * 4;
        Some(SpatialPlaceIter {
            bytes: &self.places[start..end],
        })
    }
}

pub struct SpatialPlaceIter<'a> {
    bytes: &'a [u8],
}

impl Iterator for SpatialPlaceIter<'_> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        let head = self.bytes.get(..4)?;
        let id = u32::from_le_bytes(head.try_into().ok()?);
        self.bytes = &self.bytes[4..];
        Some(id)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.bytes.len() / 4;
        (n, Some(n))
    }
}
impl ExactSizeIterator for SpatialPlaceIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(cells: &[u32], offsets: &[u32], places: &[u32]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(SPATIAL_GRID_MAGIC);
        b.extend_from_slice(&SPATIAL_GRID_VERSION.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&(places.len() as u64).to_le_bytes());
        b.extend_from_slice(&(cells.len() as u64).to_le_bytes());
        for v in cells {
            b.extend_from_slice(&v.to_le_bytes());
        }
        for v in offsets {
            b.extend_from_slice(&v.to_le_bytes());
        }
        for v in places {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b
    }
    #[test]
    fn lookup_and_empty_grid() {
        let data = fixture(&[3, 90, 128], &[0, 2, 3, 5], &[1, 4, 8, 9, 12]);
        let view = SpatialGridView::validate(&data).unwrap();
        assert_eq!(view.cell_places(3).unwrap().collect::<Vec<_>>(), vec![1, 4]);
        assert_eq!(view.cell_places(90).unwrap().collect::<Vec<_>>(), vec![8]);
        assert!(view.cell_places(20).is_none());
        let empty = fixture(&[], &[0], &[]);
        assert!(SpatialGridView::validate(&empty)
            .unwrap()
            .cell_places(4)
            .is_none());
    }
    #[test]
    fn rejects_bad_directory() {
        assert!(SpatialGridView::validate(&fixture(&[1, 2], &[0, 3, 3], &[0, 1, 2])).is_err());
        assert!(SpatialGridView::validate(&fixture(&[2, 1], &[0, 1, 2], &[0, 1])).is_err());
    }
}
