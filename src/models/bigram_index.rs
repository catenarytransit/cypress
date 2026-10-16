//! Disk-backed flattened bigram posting storage.
//!
//! The compiler writes only posting StringIds in this sidecar; the 65,537
//! entry offset directory remains in the existing CypressMemDb structure.
//! This preserves the legacy lookup and scoring semantics without holding
//! every posting in compiler heap memory.
use anyhow::{ensure, Result};
use xxhash_rust::xxh64::xxh64;

pub const BIGRAM_INDEX_FILE: &str = "cypress_bigrams.bin";
pub const BIGRAM_INDEX_MAGIC: &[u8; 8] = b"CYBIGRM1";
pub const BIGRAM_INDEX_VERSION: u32 = 1;
pub const BIGRAM_HEADER_SIZE: usize = 40;

/// Tie the postings to their exact source name bytes and per-string gram
/// counts. The index loader also verifies the StringId count and all offsets.
/// Names remain lowercased using the existing Cypress compiler analyzer.
pub fn name_fingerprint(name_bytes: &[u8], gram_counts: &[u8]) -> u64 {
    xxh64(name_bytes, xxh64(gram_counts, 0))
}

pub struct BigramIndexView<'a> {
    postings: &'a [u32],
    pub string_count: u64,
    pub fingerprint: u64,
}

impl<'a> BigramIndexView<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self> {
        ensure!(data.len() >= BIGRAM_HEADER_SIZE, "truncated bigram sidecar");
        ensure!(&data[..8] == BIGRAM_INDEX_MAGIC, "bigram sidecar magic mismatch");
        ensure!(
            u32::from_le_bytes(data[8..12].try_into()?) == BIGRAM_INDEX_VERSION,
            "unsupported bigram sidecar version"
        );
        ensure!(data[12..16] == [0; 4], "unknown bigram sidecar flags");
        let string_count = u64::from_le_bytes(data[16..24].try_into()?);
        let posting_count = u64::from_le_bytes(data[24..32].try_into()?);
        let fingerprint = u64::from_le_bytes(data[32..40].try_into()?);
        let bytes_len = usize::try_from(posting_count)?.checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("bigram sidecar size overflow"))?;
        ensure!(
            data.len() == BIGRAM_HEADER_SIZE.checked_add(bytes_len)
                .ok_or_else(|| anyhow::anyhow!("bigram sidecar length overflow"))?,
            "bigram sidecar posting length mismatch"
        );
        ensure!(cfg!(target_endian = "little"), "bigram sidecar requires little-endian host");
        let postings = bytemuck::try_cast_slice::<u8, u32>(&data[BIGRAM_HEADER_SIZE..])
            .map_err(|e| anyhow::anyhow!("unaligned bigram mmap data: {e:?}"))?;
        Ok(Self { postings, string_count, fingerprint })
    }

    pub fn len(&self) -> usize { self.postings.len() }

    pub fn postings(&self, start: usize, end: usize) -> Option<&'a [u32]> {
        self.postings.get(start..end)
    }

    pub fn matches_names(&self, bytes: &[u8], grams: &[u8]) -> bool {
        self.string_count == grams.len() as u64
            && self.fingerprint == name_fingerprint(bytes, grams)
    }

    /// Validate the legacy offset array against the sidecar. This is O(65536)
    /// only at startup/hot reload, not for each user request.
    pub fn validate_offsets(&self, offsets: &[u32]) -> Result<()> {
        ensure!(offsets.len() == 65537, "bigram offset directory wrong length");
        ensure!(offsets.first() == Some(&0), "bigram offsets must start at zero");
        let mut last = 0;
        for &end in offsets {
            ensure!(end >= last && end as usize <= self.len(), "invalid bigram offsets");
            last = end;
        }
        ensure!(last as usize == self.len(), "bigram sidecar count disagrees with offsets");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(ids: &[u32], names: &[u8], counts: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(BIGRAM_INDEX_MAGIC);
        buf.extend_from_slice(&BIGRAM_INDEX_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&(counts.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(ids.len() as u64).to_le_bytes());
        buf.extend_from_slice(&name_fingerprint(names, counts).to_le_bytes());
        for &x in ids { buf.extend_from_slice(&x.to_le_bytes()); }
        buf
    }

    #[test]
    fn postings_round_trip() {
        let data = fixture(&[3, 9, 12], b"abc", &[2, 2]);
        let view = BigramIndexView::parse(&data).unwrap();
        assert_eq!(view.postings(0, 3), Some(&[3, 9, 12][..]));
        assert!(view.matches_names(b"abc", &[2, 2]));
        assert!(!view.matches_names(b"acb", &[2, 2]));
        let mut off = vec![0u32; 65537];
        off[0] = 0;
        off[1] = 3;
        off[2..].fill(3);
        view.validate_offsets(&off).unwrap();
        off[2] = 2;
        assert!(view.validate_offsets(&off).is_err());
    }

    #[test]
    fn rejects_corrupted_header() {
        let mut data = fixture(&[0, 1], b"hi", &[1]);
        data.pop();
        assert!(BigramIndexView::parse(&data).is_err());
    }
}
