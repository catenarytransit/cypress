//! Versioned, mmap-readable dense columns. All sections are little-endian and
//! 8-byte aligned; the compiler writes them sequentially without heap copies.
use anyhow::{anyhow, ensure, Result};
use std::convert::TryInto;

pub const DENSE_FILE: &str = "cypress_dense.bin";
pub const DENSE_MAGIC: &[u8; 8] = b"CYDENS01";
pub const DENSE_VERSION: u32 = 1;
pub const SECTION_COUNT: usize = 15;
pub const HEADER_SIZE: usize = 24 + SECTION_COUNT * 16;

/// The order is part of the disk format, not an enum derived from Rust layout.
pub const SECTION_NAMES: [&str; SECTION_COUNT] = [
    "place_latitudes", "place_longitudes", "place_importances",
    "place_source_id_offsets", "place_source_id_bytes",
    "place_layer_ranks", "place_populations",
    "house_street_groups", "house_number_offsets", "house_number_bytes",
    "house_postcode_offsets", "house_postcode_bytes", "house_place_ids",
    "place_address_house_indices", "place_area_sets",
];

pub const ELEMENT_BYTES: [usize; SECTION_COUNT] = [
    4, 4, 4, 4, 1, 1, 4, 4, 8, 1, 8, 1, 4, 4, 4,
];

pub struct DenseColumnsView<'a> {
    pub place_latitudes: &'a [f32],
    pub place_longitudes: &'a [f32],
    pub place_importances: &'a [f32],
    pub place_source_id_offsets: &'a [u32],
    pub place_source_id_bytes: &'a [u8],
    pub place_layer_ranks: &'a [u8],
    pub place_populations: &'a [u32],
    pub house_street_groups: &'a [u32],
    pub house_number_offsets: &'a [u64],
    pub house_number_bytes: &'a [u8],
    pub house_postcode_offsets: &'a [u64],
    pub house_postcode_bytes: &'a [u8],
    pub house_place_ids: &'a [u32],
    pub place_address_house_indices: &'a [u32],
    pub place_area_sets: &'a [u32],
    pub dictionary_fingerprint: u64,
}

impl<'a> DenseColumnsView<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self> {
        ensure!(cfg!(target_endian = "little"), "dense sidecar requires little-endian");
        ensure!(data.len() >= HEADER_SIZE, "truncated dense column header");
        ensure!(&data[0..8] == DENSE_MAGIC, "bad dense sidecar magic");
        ensure!(u32::from_le_bytes(data[8..12].try_into()?) == DENSE_VERSION,
                "unsupported dense sidecar version");
        ensure!(u32::from_le_bytes(data[12..16].try_into()?) == SECTION_COUNT as u32,
                "incorrect dense sidecar section count");
        let fingerprint = u64::from_le_bytes(data[16..24].try_into()?);
        let mut ranges = [(0usize, 0usize); SECTION_COUNT];
        let mut cursor = HEADER_SIZE;
        for (i, range) in ranges.iter_mut().enumerate() {
            let pos = 24 + i * 16;
            let offset = usize::try_from(u64::from_le_bytes(data[pos..pos + 8].try_into()?))?;
            let count = usize::try_from(u64::from_le_bytes(data[pos + 8..pos + 16].try_into()?))?;
            let bytes = count.checked_mul(ELEMENT_BYTES[i]).ok_or_else(|| anyhow!("dense column size overflow"))?;
            let end = offset.checked_add(bytes).ok_or_else(|| anyhow!("dense column range overflow"))?;
            ensure!(offset == cursor, "dense sections not contiguous/aligned");
            ensure!(end <= data.len(), "truncated dense column");
            *range = (offset, end);
            cursor = end.checked_add(7).ok_or_else(|| anyhow!("dense alignment overflow"))? & !7;
            ensure!(cursor >= end, "dense alignment overflow");
        }
        ensure!(cursor == data.len(), "dense sidecar has trailing or missing data");

        fn cast<'b, T: bytemuck::Pod>(data: &'b [u8], pair: (usize, usize)) -> Result<&'b [T]> {
            bytemuck::try_cast_slice(&data[pair.0..pair.1])
                .map_err(|e| anyhow!("unaligned dense column: {e:?}"))
        }
        let view = Self {
            place_latitudes: cast(data, ranges[0])?,
            place_longitudes: cast(data, ranges[1])?,
            place_importances: cast(data, ranges[2])?,
            place_source_id_offsets: cast(data, ranges[3])?,
            place_source_id_bytes: &data[ranges[4].0..ranges[4].1],
            place_layer_ranks: &data[ranges[5].0..ranges[5].1],
            place_populations: cast(data, ranges[6])?,
            house_street_groups: cast(data, ranges[7])?,
            house_number_offsets: cast(data, ranges[8])?,
            house_number_bytes: &data[ranges[9].0..ranges[9].1],
            house_postcode_offsets: cast(data, ranges[10])?,
            house_postcode_bytes: &data[ranges[11].0..ranges[11].1],
            house_place_ids: cast(data, ranges[12])?,
            place_address_house_indices: cast(data, ranges[13])?,
            place_area_sets: cast(data, ranges[14])?,
            dictionary_fingerprint: fingerprint,
        };
        view.validate()?;
        Ok(view)
    }

    fn validate(&self) -> Result<()> {
        let n = self.place_latitudes.len();
        let h = self.house_place_ids.len();
        for count in [
            self.place_longitudes.len(), self.place_importances.len(),
            self.place_layer_ranks.len(), self.place_populations.len(),
            self.place_address_house_indices.len(), self.place_area_sets.len(),
        ] { ensure!(count == n, "inconsistent dense place count"); }
        ensure!(self.place_source_id_offsets.len() == n + 1, "invalid source offsets");
        ensure!(self.house_street_groups.len() == h, "invalid house street count");
        ensure!(self.house_number_offsets.len() == h + 1, "invalid number offsets");
        ensure!(self.house_postcode_offsets.len() == h + 1, "invalid postcode offsets");
        ensure!(self.place_source_id_offsets.first() == Some(&0), "invalid source start");
        ensure!(self.house_number_offsets.first() == Some(&0), "invalid number start");
        ensure!(self.house_postcode_offsets.first() == Some(&0), "invalid postcode start");
        ensure!(self.place_source_id_offsets.last().map(|&x| u64::from(x)) == Some(self.place_source_id_bytes.len() as u64),
                "invalid source byte length");
        ensure!(self.house_number_offsets.last().copied() == Some(self.house_number_bytes.len() as u64),
                "invalid number byte length");
        ensure!(self.house_postcode_offsets.last().copied() == Some(self.house_postcode_bytes.len() as u64),
                "invalid postcode byte length");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_truncated_file() {
        assert!(DenseColumnsView::parse(&[]).is_err());
        assert!(DenseColumnsView::parse(&vec![0; HEADER_SIZE]).is_err());
    }
}
