//! Experimental generation-bound, mmap-native hybrid inverted index.
//!
//! One Unicode FST maps tokens to postings. Each token is split on the upper
//! 16 bits of its canonical StringIds, like Roaring containers. Sparse slices
//! store sorted u16 lows; dense slices store a fixed 8192-byte bitmap.
//! Query membership touches one container and never decodes a full postings
//! list or allocates skip tables. Cypress multilingual names stay unchanged.
use anyhow::{anyhow, ensure, Result};
use fst::Map;
use super::token_index::{dictionary_fingerprint, TOKEN_ANALYZER_VERSION};

pub const FILE: &str = "cypress_roaring_tokens.bin";
pub const MAGIC: &[u8; 8] = b"CYPROAR1";
pub const VERSION: u32 = 1;
pub const HEADER_BYTES: usize = 64;
pub const META_BYTES: usize = 16;
pub const CONTAINER_BYTES: usize = 12;
pub const BITMAP_BYTES: usize = 8192;
pub const ARRAY_MAX: usize = 4096;

fn le16(src: &[u8], pos: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(src.get(pos..pos + 2).ok_or_else(|| anyhow!("truncated u16"))?.try_into()?))
}
fn le32(src: &[u8], pos: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(src.get(pos..pos + 4).ok_or_else(|| anyhow!("truncated u32"))?.try_into()?))
}
fn le64(src: &[u8], pos: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(src.get(pos..pos + 8).ok_or_else(|| anyhow!("truncated u64"))?.try_into()?))
}

#[derive(Clone, Copy)]
struct Container<'a> {
    high: u16,
    count: u32,
    bitmap: bool,
    body: &'a [u8],
}

pub struct RoaringTokenIndexView<'a> {
    pub fst: Map<&'a [u8]>,
    metadata: &'a [u8],
    postings: &'a [u8],
    pub token_count: u64,
    pub posting_count: u64,
    pub string_count: u64,
    pub fingerprint: u64,
}

pub struct RoaringPosting<'a> {
    bytes: &'a [u8],
    pub document_frequency: u32,
}

impl RoaringPosting<'_> {
    fn container_count(&self) -> usize {
        u32::from_le_bytes(self.bytes[0..4].try_into().unwrap()) as usize
    }
    fn container(&self, index: usize) -> Container<'_> {
        let pos = 4 + index * CONTAINER_BYTES;
        let high = u16::from_le_bytes(self.bytes[pos..pos + 2].try_into().unwrap());
        let bitmap = self.bytes[pos + 2] == 1;
        let count = u32::from_le_bytes(self.bytes[pos + 4..pos + 8].try_into().unwrap());
        let start = u32::from_le_bytes(self.bytes[pos + 8..pos + 12].try_into().unwrap()) as usize;
        let size = if bitmap { BITMAP_BYTES } else { count as usize * 2 };
        Container { high, count, bitmap, body: &self.bytes[start..start + size] }
    }
    /// O(log #containers + log 4096) for sparse entries; O(log #containers)
    /// plus one bitmap bit-test for dense ones. No allocation or varint decoding.
    pub fn contains(&self, id: u32) -> bool {
        let high = (id >> 16) as u16;
        let low = id as u16;
        let mut left = 0;
        let mut right = self.container_count();
        while left < right {
            let middle = left + (right - left) / 2;
            if self.container(middle).high < high { left = middle + 1; }
            else { right = middle; }
        }
        if left >= self.container_count() { return false; }
        let c = self.container(left);
        if c.high != high { return false; }
        if c.bitmap {
            let b = c.body[(low as usize) >> 3];
            return b & (1 << (low & 7)) != 0;
        }
        let mut lo = 0usize;
        let mut hi = c.count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let v = u16::from_le_bytes(c.body[mid*2..mid*2+2].try_into().unwrap());
            if v < low { lo = mid + 1; } else { hi = mid; }
        }
        lo < c.count as usize
            && u16::from_le_bytes(c.body[lo*2..lo*2+2].try_into().unwrap()) == low
    }
    /// Decode only the selected rarest anchor list, never the high-DF probes.
    pub fn anchor_ids(&self) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.document_frequency as usize);
        for n in 0..self.container_count() {
            let c = self.container(n);
            let high = (c.high as u32) << 16;
            if c.bitmap {
                for (i, byte) in c.body.iter().copied().enumerate() {
                    let mut bits = byte;
                    while bits != 0 {
                        let bit = bits.trailing_zeros();
                        out.push(high | (i as u32 * 8 + bit));
                        bits &= bits - 1;
                    }
                }
            } else {
                for v in c.body.chunks_exact(2) {
                    out.push(high | u16::from_le_bytes([v[0],v[1]]) as u32);
                }
            }
        }
        out
    }
    fn validate(&self) -> Result<()> {
        let n = self.container_count();
        let header_end = 4usize.checked_add(n.checked_mul(CONTAINER_BYTES).ok_or_else(|| anyhow!("container count overflow"))?)
            .ok_or_else(|| anyhow!("container headers overflow"))?;
        ensure!(header_end <= self.bytes.len(), "truncated container headers");
        let mut count = 0u64;
        let mut last_high: Option<u16> = None;
        for i in 0..n {
            let pos = 4 + i * CONTAINER_BYTES;
            let high = le16(self.bytes, pos)?;
            let kind = self.bytes[pos + 2];
            ensure!(kind <= 1 && self.bytes[pos + 3] == 0, "invalid container type");
            let size = le32(self.bytes, pos + 4)? as usize;
            ensure!((1..=65536).contains(&size), "invalid container cardinality");
            if let Some(prev) = last_high { ensure!(high > prev, "containers not sorted"); }
            last_high = Some(high);
            let start = le32(self.bytes, pos + 8)? as usize;
            let body_len = if kind == 1 { BITMAP_BYTES } else {
                ensure!(size <= ARRAY_MAX, "sparse array too large");
                size * 2
            };
            ensure!(start >= header_end && start.checked_add(body_len).map_or(false, |end| end <= self.bytes.len()),
                    "container data out of bounds");
            let body = &self.bytes[start..start+body_len];
            if kind == 1 {
                ensure!(size > ARRAY_MAX, "bitmap should be dense");
                ensure!(body.iter().map(|b| b.count_ones() as usize).sum::<usize>() == size, "bitmap cardinality mismatch");
            } else {
                let mut prior = None;
                for pair in body.chunks_exact(2) {
                    let value = u16::from_le_bytes([pair[0], pair[1]]);
                    if let Some(p) = prior { ensure!(value > p, "array not sorted"); }
                    prior = Some(value);
                }
            }
            count += size as u64;
        }
        ensure!(count == self.document_frequency as u64, "document frequency mismatch");
        Ok(())
    }
}

impl<'a> RoaringTokenIndexView<'a> {
    fn load(bytes: &'a [u8], deep: bool) -> Result<Self> {
        ensure!(bytes.len() >= HEADER_BYTES && &bytes[..8] == MAGIC, "roaring index header invalid");
        ensure!(le32(bytes, 8)? == VERSION, "roaring index version mismatch");
        ensure!(le32(bytes, 12)? == TOKEN_ANALYZER_VERSION, "analyzer version mismatch");
        let string_count = le64(bytes, 16)?;
        let fingerprint = le64(bytes, 24)?;
        let token_count = le64(bytes, 32)?;
        let posting_count = le64(bytes, 40)?;
        let fst_size = usize::try_from(le64(bytes, 48)?)?;
        let posting_bytes = usize::try_from(le64(bytes, 56)?)?;
        let fst_end = HEADER_BYTES.checked_add(fst_size).ok_or_else(|| anyhow!("FST overflow"))?;
        let meta_end = fst_end.checked_add(usize::try_from(token_count)?.checked_mul(META_BYTES).ok_or_else(|| anyhow!("meta overflow"))?).ok_or_else(|| anyhow!("meta overflow"))?;
        let end = meta_end.checked_add(posting_bytes).ok_or_else(|| anyhow!("payload overflow"))?;
        ensure!(end == bytes.len(), "roaring index file length mismatch");
        let fst = Map::new(&bytes[HEADER_BYTES..fst_end])?;
        ensure!(fst.len() == usize::try_from(token_count)?, "roaring FST size mismatch");
        let view = Self {
            fst, metadata: &bytes[fst_end..meta_end], postings: &bytes[meta_end..end],
            token_count, posting_count, string_count, fingerprint,
        };
        if deep {
            let mut count = 0u64;
            for n in 0..usize::try_from(token_count)? {
                let p = view.posting_at(n)?;
                p.validate()?;
                count = count.checked_add(p.document_frequency as u64).ok_or_else(|| anyhow!("posting overflow"))?;
            }
            ensure!(count == posting_count, "total posting count mismatch");
        }
        Ok(view)
    }
    pub fn parse(bytes: &'a [u8]) -> Result<Self> { Self::load(bytes, true) }
    pub fn view_validated(bytes: &'a [u8]) -> Result<Self> { Self::load(bytes, false) }
    pub fn matches_dictionary(&self, names: &[u8], offsets: &[u32]) -> bool {
        self.string_count == offsets.len().saturating_sub(1) as u64
            && self.fingerprint == dictionary_fingerprint(names, offsets)
    }
    fn posting_at(&self, ordinal: usize) -> Result<RoaringPosting<'a>> {
        let pos = ordinal.checked_mul(META_BYTES).ok_or_else(|| anyhow!("metadata overflow"))?;
        let offset = usize::try_from(le64(self.metadata, pos)?)?;
        let size = le32(self.metadata, pos + 8)? as usize;
        let df = le32(self.metadata, pos + 12)?;
        let end = offset.checked_add(size).ok_or_else(|| anyhow!("posting overflow"))?;
        let data = self.postings.get(offset..end).ok_or_else(|| anyhow!("posting bounds invalid"))?;
        ensure!(data.len() >= 4, "posting headers truncated");
        // Cheap check for query safety; full verification happens at mmap load.
        let n = le32(data, 0)? as usize;
        ensure!(4usize.checked_add(n.saturating_mul(CONTAINER_BYTES)).map_or(false, |e| e<=data.len()), "invalid container header length");
        Ok(RoaringPosting { bytes: data, document_frequency: df })
    }
    pub fn lookup(&self, token: &str) -> Result<Option<RoaringPosting<'a>>> {
        let Some(ord) = self.fst.get(token) else { return Ok(None) };
        ensure!(ord < self.token_count, "invalid token ordinal");
        Ok(Some(self.posting_at(usize::try_from(ord)?)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_index_rejected() {
        assert!(RoaringTokenIndexView::parse(b"short").is_err());
    }
    #[test]
    fn dictionary_fingerprint_is_generation_bound() {
        assert_ne!(dictionary_fingerprint(b"bern", &[0,4]), dictionary_fingerprint(b"berne", &[0,5]));
    }
}
