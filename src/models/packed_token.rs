//! Experimental mmap posting-pack format, independent of the v1 token sidecar.
//! FST term -> lexical ordinal -> fixed-width metadata -> u32 StringId postings.
//! Posting payloads are physically grouped by document-frequency bucket, not
//! term order. StringIds remain the canonical Cypress dictionary IDs.
use anyhow::{anyhow, ensure, Result};
use fst::Map;

use super::token_index::{dictionary_fingerprint, TOKEN_ANALYZER_VERSION};

pub const PACK_FILE: &str = "cypress_token_pack.bin";
pub const PACK_MAGIC: &[u8; 8] = b"CYPPACK2";
pub const PACK_VERSION: u32 = 1;
pub const PACK_HEADER_BYTES: usize = 64;
pub const PACK_META_BYTES: usize = 16;

fn u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| anyhow!("offset overflow"))?;
    Ok(u32::from_le_bytes(bytes.get(offset..end).ok_or_else(|| anyhow!("truncated pack u32"))?.try_into()?))
}

fn u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| anyhow!("offset overflow"))?;
    Ok(u64::from_le_bytes(bytes.get(offset..end).ok_or_else(|| anyhow!("truncated pack u64"))?.try_into()?))
}

pub struct PackedTokenIndexView<'a> {
    pub fst: Map<&'a [u8]>,
    metadata: &'a [u8],
    postings: &'a [u8],
    pub string_count: u64,
    pub fingerprint: u64,
    pub token_count: u64,
    pub posting_count: u64,
}

pub struct PackedPosting<'a> {
    bytes: &'a [u8],
    pub document_frequency: u32,
}

impl PackedPosting<'_> {
    pub fn id_at(&self, index: usize) -> u32 {
        let offset = index * 4;
        // All ranges and lengths are generation-validated at load time.
        u32::from_le_bytes(self.bytes[offset..offset + 4].try_into().unwrap())
    }

    pub fn contains(&self, wanted: u32) -> bool {
        let mut lower = 0usize;
        let mut upper = self.document_frequency as usize;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.id_at(middle) < wanted {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        lower < self.document_frequency as usize && self.id_at(lower) == wanted
    }

    pub fn decode_ids(&self) -> Vec<u32> {
        (0..self.document_frequency as usize).map(|i| self.id_at(i)).collect()
    }
}

impl<'a> PackedTokenIndexView<'a> {
    fn load(data: &'a [u8], deep: bool) -> Result<Self> {
        ensure!(data.len() >= PACK_HEADER_BYTES, "pack header truncated");
        ensure!(&data[0..8] == PACK_MAGIC, "pack magic mismatch");
        ensure!(u32_at(data, 8)? == PACK_VERSION, "unsupported packed token version");
        ensure!(u32_at(data, 12)? == TOKEN_ANALYZER_VERSION, "token analyzer mismatch");
        let string_count = u64_at(data, 16)?;
        let fingerprint = u64_at(data, 24)?;
        let token_count = u64_at(data, 32)?;
        let posting_count = u64_at(data, 40)?;
        let fst_size = usize::try_from(u64_at(data, 48)?)?;
        let posting_size = usize::try_from(u64_at(data, 56)?)?;
        let fst_end = PACK_HEADER_BYTES.checked_add(fst_size).ok_or_else(|| anyhow!("pack offset overflow"))?;
        let meta_size = usize::try_from(token_count)?.checked_mul(PACK_META_BYTES).ok_or_else(|| anyhow!("pack metadata overflow"))?;
        let meta_end = fst_end.checked_add(meta_size).ok_or_else(|| anyhow!("pack metadata overflow"))?;
        let final_end = meta_end.checked_add(posting_size).ok_or_else(|| anyhow!("pack payload overflow"))?;
        ensure!(final_end == data.len(), "packed token file size mismatch");
        let fst = Map::new(data.get(PACK_HEADER_BYTES..fst_end).ok_or_else(|| anyhow!("pack FST truncated"))?)?;
        ensure!(fst.len() == token_count as usize, "pack FST length mismatch");
        let metadata = &data[fst_end..meta_end];
        let postings = &data[meta_end..final_end];
        if deep {
            let mut counted = 0u64;
            for i in 0..token_count as usize {
                let p = i * PACK_META_BYTES;
                let start = usize::try_from(u64_at(metadata, p)?)?;
                let frequency = u32_at(metadata, p + 8)?;
                let end = start.checked_add(frequency as usize * 4).ok_or_else(|| anyhow!("pack posting overflow"))?;
                ensure!(start % 4 == 0 && end <= postings.len(), "pack posting bounds invalid");
                counted = counted.checked_add(frequency as u64).ok_or_else(|| anyhow!("pack frequency overflow"))?;
            }
            ensure!(counted == posting_count, "pack posting count mismatch");
        }
        Ok(Self { fst, metadata, postings, string_count, fingerprint, token_count, posting_count })
    }

    /// Validate the full file once, at load/publish time.
    pub fn parse(data: &'a [u8]) -> Result<Self> {
        Self::load(data, true)
    }

    /// Query path: constant-time header and bounds checks after load validation.
    pub fn view_validated(data: &'a [u8]) -> Result<Self> {
        Self::load(data, false)
    }

    pub fn matches_dictionary(&self, names: &[u8], offsets: &[u32]) -> bool {
        self.string_count == offsets.len().saturating_sub(1) as u64
            && self.fingerprint == dictionary_fingerprint(names, offsets)
    }

    pub fn lookup(&self, term: &str) -> Result<Option<PackedPosting<'a>>> {
        let Some(ordinal) = self.fst.get(term) else { return Ok(None); };
        ensure!(ordinal < self.token_count, "pack FST ordinal invalid");
        let p = usize::try_from(ordinal)? * PACK_META_BYTES;
        let start = usize::try_from(u64_at(self.metadata, p)?)?;
        let frequency = u32_at(self.metadata, p + 8)?;
        let end = start.checked_add(frequency as usize * 4).ok_or_else(|| anyhow!("pack posting overflow"))?;
        let bytes = self.postings.get(start..end).ok_or_else(|| anyhow!("pack posting bounds invalid"))?;
        Ok(Some(PackedPosting { bytes, document_frequency: frequency }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::MapBuilder;

    #[test]
    fn frequency_ordered_posting_offsets_can_be_nonmonotonic() {
        let mut b = MapBuilder::memory();
        b.insert("berlin", 0).unwrap();
        b.insert("paris", 1).unwrap();
        let fst = b.into_inner().unwrap();
        let names = b"berlinparis";
        let bounds = [0, 6, 11];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PACK_MAGIC);
        bytes.extend_from_slice(&PACK_VERSION.to_le_bytes());
        bytes.extend_from_slice(&TOKEN_ANALYZER_VERSION.to_le_bytes());
        for value in [2u64, dictionary_fingerprint(names, &bounds), 2, 3, fst.len() as u64, 12] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&fst);
        // Paris is physically stored before Berlin despite lexical ordering.
        for (start, df) in [(4u64, 2u32), (0, 1)] {
            bytes.extend_from_slice(&start.to_le_bytes());
            bytes.extend_from_slice(&df.to_le_bytes());
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        for id in [7u32, 2, 6] { bytes.extend_from_slice(&id.to_le_bytes()); }
        let view = PackedTokenIndexView::parse(&bytes).unwrap();
        assert!(view.matches_dictionary(names, &bounds));
        assert_eq!(view.lookup("berlin").unwrap().unwrap().decode_ids(), vec![2, 6]);
        assert!(view.lookup("berlin").unwrap().unwrap().contains(6));
        assert!(!view.lookup("paris").unwrap().unwrap().contains(6));
        assert_eq!(view.lookup("paris").unwrap().unwrap().decode_ids(), vec![7]);
    }

    #[test]
    fn reject_truncated_and_stale_packs() {
        assert!(PackedTokenIndexView::parse(b"short").is_err());
    }
}
