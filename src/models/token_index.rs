//! Sidecar token index format (version 1).
//!
//! This file does not change CypressMemDb or existing query behavior.
//! The FST maps a Unicode token to an ordinal, which selects delta-varint
//! StringId postings. Skip blocks contain (last_id, relative_byte_offset)
//! to support seek-based intersections in a subsequent query PR.

use anyhow::{anyhow, bail, ensure, Result};
use fst::Map;
use xxhash_rust::xxh64::xxh64;

pub const TOKEN_INDEX_FILE: &str = "cypress_tokens.bin";
pub const TOKEN_INDEX_MAGIC: &[u8; 8] = b"CYPTOK01";
pub const TOKEN_INDEX_VERSION: u32 = 1;
pub const TOKEN_ANALYZER_VERSION: u32 = 1;
pub const TOKEN_SKIP_BLOCK: usize = 128;
pub const TOKEN_INDEX_HEADER_BYTES: usize = 72;

/// Fingerprint the exact byte arena and its boundaries, not just the name text.
/// A sidecar from a different compiler generation must never be joined by ID.
pub fn dictionary_fingerprint(name_bytes: &[u8], name_offsets: &[u32]) -> u64 {
    xxh64(
        bytemuck::cast_slice(name_offsets),
        xxh64(name_bytes, 0),
    )
}

/// Token boundaries for the *existing* lowercased string dictionary.
/// This is deliberately language-neutral and non-destructive: all token
/// postings refer to original StringIds. PR 7 will add script-specific
/// segmentation; unsegmented CJK names remain searchable as whole tokens.
/// Combining marks stay attached rather than splitting decomposed names.
pub fn tokenize_name(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !c.is_alphanumeric() && !is_combining_mark(c))
        .filter(|token| !token.is_empty())
}

fn is_combining_mark(c: char) -> bool {
    matches!(
        c as u32,
        0x0300..=0x036F
            | 0x0483..=0x0489
            | 0x0591..=0x05BD
            | 0x05BF
            | 0x05C1..=0x05C2
            | 0x05C4..=0x05C5
            | 0x0610..=0x061A
            | 0x064B..=0x065F
            | 0x0670
            | 0x06D6..=0x06ED
            | 0x0900..=0x0903
            | 0x093A..=0x094F
            | 0x0981..=0x0983
            | 0x0E31
            | 0x0E34..=0x0E3A
            | 0x0E47..=0x0E4E
            | 0x1AB0..=0x1AFF
            | 0x1DC0..=0x1DFF
            | 0x20D0..=0x20FF
            | 0xFE20..=0xFE2F
    )
}

fn le_u64(bytes: &[u8], pos: usize) -> Result<u64> {
    let raw = bytes.get(pos..pos.checked_add(8).ok_or_else(|| anyhow!("offset overflow"))?)
        .ok_or_else(|| anyhow!("truncated token index u64"))?;
    Ok(u64::from_le_bytes(raw.try_into()?))
}

fn le_u32(bytes: &[u8], pos: usize) -> Result<u32> {
    let raw = bytes.get(pos..pos.checked_add(4).ok_or_else(|| anyhow!("offset overflow"))?)
        .ok_or_else(|| anyhow!("truncated token index u32"))?;
    Ok(u32::from_le_bytes(raw.try_into()?))
}

fn take<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.checked_add(len).ok_or_else(|| anyhow!("token index length overflow"))?;
    let slice = data.get(*cursor..end).ok_or_else(|| anyhow!("truncated token index section"))?;
    *cursor = end;
    Ok(slice)
}

/// Borrowed view suitable for use directly on a read-only mmap.
/// Header: magic[8], format u32, analyzer u32, seven little-endian u64:
/// string_count, dictionary_fingerprint, token_count, posting_count,
/// fst_bytes, posting_bytes, skip_count.
/// Sections: FST, (T+1) posting offsets, T docfreqs,
/// (T+1) skip offsets, skip_count * (last_id:u32, relative_offset:u64),
/// and the delta-varint posting byte arena.
pub struct TokenIndexView<'a> {
    pub fst: Map<&'a [u8]>,
    posting_offsets: &'a [u8],
    frequencies: &'a [u8],
    skip_offsets: &'a [u8],
    skips: &'a [u8],
    postings: &'a [u8],
    pub token_count: u64,
    pub posting_count: u64,
    pub string_count: u64,
    pub fingerprint: u64,
}

pub struct TokenPostingList<'a> {
    pub document_frequency: u32,
    /// Absolute offset in the posting arena for skip seek.
    pub posting_start: u64,
    pub bytes: &'a [u8],
    pub skip_bytes: &'a [u8],
}

impl<'a> TokenIndexView<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self> {
        ensure!(data.len() >= TOKEN_INDEX_HEADER_BYTES, "token index header is truncated");
        ensure!(&data[..8] == TOKEN_INDEX_MAGIC, "token index magic mismatch");
        ensure!(le_u32(data, 8)? == TOKEN_INDEX_VERSION, "unsupported token index version");
        ensure!(le_u32(data, 12)? == TOKEN_ANALYZER_VERSION, "unsupported analyzer version");
        let string_count = le_u64(data, 16)?;
        let fingerprint = le_u64(data, 24)?;
        let token_count = le_u64(data, 32)?;
        let posting_count = le_u64(data, 40)?;
        let fst_bytes = le_u64(data, 48)?;
        let posting_bytes = le_u64(data, 56)?;
        let skip_count = le_u64(data, 64)?;

        let n = usize::try_from(token_count)?;
        let off_bytes = n.checked_add(1).and_then(|x| x.checked_mul(8))
            .ok_or_else(|| anyhow!("token count overflow"))?;
        let df_bytes = n.checked_mul(4).ok_or_else(|| anyhow!("docfreq overflow"))?;
        let skips_bytes = usize::try_from(skip_count)?.checked_mul(12)
            .ok_or_else(|| anyhow!("skip count overflow"))?;

        let mut cursor = TOKEN_INDEX_HEADER_BYTES;
        let fst = Map::new(take(data, &mut cursor, usize::try_from(fst_bytes)?)?)?;
        let posting_offsets = take(data, &mut cursor, off_bytes)?;
        let frequencies = take(data, &mut cursor, df_bytes)?;
        let skip_offsets = take(data, &mut cursor, off_bytes)?;
        let skips = take(data, &mut cursor, skips_bytes)?;
        let postings = take(data, &mut cursor, usize::try_from(posting_bytes)?)?;
        ensure!(cursor == data.len(), "unexpected token index trailing bytes");
        ensure!(fst.len() == n, "token FST count mismatch");
        ensure!(le_u64(posting_offsets, 0)? == 0, "posting offsets must start at zero");
        ensure!(le_u64(skip_offsets, 0)? == 0, "skip offsets must start at zero");

        let mut counted = 0u64;
        let mut expected_skip_count = 0u64;
        for i in 0..n {
            let a = le_u64(posting_offsets, i * 8)?;
            let b = le_u64(posting_offsets, (i + 1) * 8)?;
            ensure!(a <= b && b <= posting_bytes, "invalid posting offsets");
            let sa = le_u64(skip_offsets, i * 8)?;
            let sb = le_u64(skip_offsets, (i + 1) * 8)?;
            ensure!(sa <= sb && sb <= skip_count, "invalid skip offsets");
            let df = le_u32(frequencies, i * 4)? as u64;
            counted = counted.checked_add(df).ok_or_else(|| anyhow!("posting count overflow"))?;
            expected_skip_count += df.div_ceil(TOKEN_SKIP_BLOCK as u64);
            ensure!(sb - sa == df.div_ceil(TOKEN_SKIP_BLOCK as u64), "incorrect skip count for term");
            ensure!(df == 0 || a < b, "nonempty posting has no bytes");
        }
        ensure!(le_u64(posting_offsets, n * 8)? == posting_bytes, "posting length mismatch");
        ensure!(le_u64(skip_offsets, n * 8)? == skip_count, "skip length mismatch");
        ensure!(counted == posting_count, "posting count mismatch");
        ensure!(expected_skip_count == skip_count, "total skip count mismatch");

        Ok(Self {
            fst, posting_offsets, frequencies, skip_offsets, skips, postings,
            token_count, posting_count, string_count, fingerprint,
        })
    }

    /// Cheap borrow-only view of a sidecar that was fully validated at load.
    /// Call parse and matches_dictionary before this method; keep mmap immutable.
    /// Avoids O(token_count) validation on every search request.
    pub fn view_validated(data: &'a [u8]) -> Result<Self> {
        ensure!(data.len() >= TOKEN_INDEX_HEADER_BYTES, "token index header truncated");
        ensure!(&data[..8] == TOKEN_INDEX_MAGIC, "token index magic mismatch");
        ensure!(le_u32(data, 8)? == TOKEN_INDEX_VERSION, "token index version mismatch");
        ensure!(le_u32(data, 12)? == TOKEN_ANALYZER_VERSION, "analyzer version mismatch");
        let string_count = le_u64(data, 16)?;
        let fingerprint = le_u64(data, 24)?;
        let token_count = le_u64(data, 32)?;
        let posting_count = le_u64(data, 40)?;
        let fst_bytes = usize::try_from(le_u64(data, 48)?)?;
        let posting_bytes = usize::try_from(le_u64(data, 56)?)?;
        let skip_count = usize::try_from(le_u64(data, 64)?)?;
        let n = usize::try_from(token_count)?;
        let offset_len = n.checked_add(1)
            .and_then(|v| v.checked_mul(8))
            .ok_or_else(|| anyhow!("offset length overflow"))?;
        let freq_len = n.checked_mul(4).ok_or_else(|| anyhow!("freq overflow"))?;
        let skip_len = skip_count.checked_mul(12)
            .ok_or_else(|| anyhow!("skip length overflow"))?;
        let mut cursor = TOKEN_INDEX_HEADER_BYTES;
        let fst = Map::new(take(data, &mut cursor, fst_bytes)?)?;
        let posting_offsets = take(data, &mut cursor, offset_len)?;
        let frequencies = take(data, &mut cursor, freq_len)?;
        let skip_offsets = take(data, &mut cursor, offset_len)?;
        let skips = take(data, &mut cursor, skip_len)?;
        let postings = take(data, &mut cursor, posting_bytes)?;
        ensure!(cursor == data.len(), "token index trailing bytes");
        Ok(Self {
            fst, posting_offsets, frequencies, skip_offsets, skips, postings,
            token_count, posting_count, string_count, fingerprint,
        })
    }

    pub fn matches_dictionary(&self, bytes: &[u8], offsets: &[u32]) -> bool {
        self.string_count == offsets.len().saturating_sub(1) as u64
            && self.fingerprint == dictionary_fingerprint(bytes, offsets)
    }

    pub fn lookup(&self, token: &str) -> Result<Option<TokenPostingList<'a>>> {
        let Some(ordinal) = self.fst.get(token) else {
            return Ok(None);
        };
        let i = usize::try_from(ordinal)?;
        ensure!(ordinal < self.token_count, "invalid FST ordinal");
        let start = usize::try_from(le_u64(self.posting_offsets, i * 8)?)?;
        let end = usize::try_from(le_u64(self.posting_offsets, (i + 1) * 8)?)?;
        let skip_start = usize::try_from(le_u64(self.skip_offsets, i * 8)?)?;
        let skip_end = usize::try_from(le_u64(self.skip_offsets, (i + 1) * 8)?)?;
        Ok(Some(TokenPostingList {
            document_frequency: le_u32(self.frequencies, i * 4)?,
            posting_start: start as u64,
            bytes: &self.postings[start..end],
            skip_bytes: &self.skips[skip_start * 12..skip_end * 12],
        }))
    }
}

impl TokenPostingList<'_> {
    /// Test/debug decoding; PR 4 will introduce allocation-free seek cursors.
    pub fn decode_ids(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::with_capacity(self.document_frequency as usize);
        let mut pos = 0;
        let mut last = 0u32;
        for i in 0..self.document_frequency {
            let mut delta = 0u32;
            let mut terminated = false;
            for shift in (0..=28).step_by(7) {
                let byte = *self.bytes.get(pos)
                    .ok_or_else(|| anyhow!("truncated varint"))?;
                pos += 1;
                if shift == 28 && byte & 0xF0 != 0 {
                    bail!("varint overflows u32");
                }
                delta |= ((byte & 0x7F) as u32) << shift;
                if byte & 0x80 == 0 {
                    terminated = true;
                    break;
                }
            }
            ensure!(terminated, "unterminated varint");
            ensure!(i == 0 || delta > 0, "posting IDs are not strictly increasing");
            last = last.checked_add(delta).ok_or_else(|| anyhow!("posting ID overflow"))?;
            ids.push(last);
        }
        ensure!(pos == self.bytes.len(), "unexpected trailing posting bytes");
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenization_keeps_unicode_and_combining_marks() {
        assert_eq!(
            tokenize_name("München, München / 東京駅").collect::<Vec<_>>(),
            vec!["München", "München", "東京駅"]
        );
        assert_eq!(
            tokenize_name("cafe\u{0301} São-Paulo القَاهِرة").collect::<Vec<_>>(),
            vec!["cafe\u{0301}", "São", "Paulo", "القَاهِرة"]
        );
    }

    #[test]
    fn fingerprint_changes_with_boundaries() {
        assert_ne!(
            dictionary_fingerprint(b"ab", &[0, 1, 2]),
            dictionary_fingerprint(b"ab", &[0, 2])
        );
    }

    #[test]
    fn reject_truncated_header() {
        assert!(TokenIndexView::parse(b"CYPTOK01").is_err());
    }
}
