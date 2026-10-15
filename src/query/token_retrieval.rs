//! Selective token retrieval. This is an approximate candidate generator:
//! AND intersections only capture name/street tokens co-occurring in one
//! string. Administrative and alternative-name cross-field matches may be
//! absent. Use shadow mode to measure recall before enabling token mode.
use anyhow::{anyhow, ensure, Result};
use cypress::models::token_index::{tokenize_name, TokenIndexView, TokenPostingList};

pub const MAX_ANCHOR_DF: u32 = 50_000;
pub const MAX_CANDIDATES: usize = 50_000;

#[derive(Default, Debug)]
pub struct RetrievalStats {
    pub posting_entries_examined: u64,
    pub block_seeks: u64,
    pub tokens: usize,
    pub anchor_df: u32,
    pub candidates: usize,
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    for shift in (0..=28).step_by(7) {
        let byte = *bytes.get(*cursor).ok_or_else(|| anyhow!("truncated posting"))?;
        *cursor += 1;
        ensure!(shift != 28 || byte & 0xf0 == 0, "invalid varint");
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(anyhow!("unterminated varint"))
}

#[derive(Clone, Copy)]
struct Skip {
    last_id: u32,
    byte_end: usize,
}

fn skips(posting: &TokenPostingList<'_>) -> Result<Vec<Skip>> {
    let mut out = Vec::with_capacity(posting.skip_bytes.len() / 12);
    ensure!(posting.skip_bytes.len() % 12 == 0, "invalid skip table");
    for entry in posting.skip_bytes.chunks_exact(12) {
        let last_id = u32::from_le_bytes(entry[0..4].try_into()?);
        let absolute_end = u64::from_le_bytes(entry[4..12].try_into()?);
        let byte_end = usize::try_from(absolute_end)?
            .checked_sub(usize::try_from(posting.posting_start)?)
            .ok_or_else(|| anyhow!("invalid skip pointer"))?;
        ensure!(byte_end <= posting.bytes.len(), "skip pointer beyond posting");
        out.push(Skip { last_id, byte_end });
    }
    Ok(out)
}

/// Sequential postings scan for one chosen low-frequency anchor.
fn read_anchor(posting: &TokenPostingList<'_>, stats: &mut RetrievalStats) -> Result<Vec<u32>> {
    ensure!(posting.document_frequency <= MAX_ANCHOR_DF, "anchor over limit");
    let mut result = Vec::with_capacity(posting.document_frequency as usize);
    let mut pos = 0usize;
    let mut prev = 0u32;
    for i in 0..posting.document_frequency {
        let delta = decode_varint(posting.bytes, &mut pos)?;
        ensure!(i == 0 || delta > 0, "unordered posting");
        prev = prev.checked_add(delta).ok_or_else(|| anyhow!("ID overflow"))?;
        result.push(prev);
        stats.posting_entries_examined += 1;
    }
    ensure!(pos == posting.bytes.len(), "trailing posting data");
    Ok(result)
}

/// Test one document against one high-DF posting without scanning all its
/// entries. Seek using each block's final ID; decode only selected block.
/// Blocks delta-code relative to the preceding block's last ID.
fn contains(
    posting: &TokenPostingList<'_>,
    table: &[Skip],
    target: u32,
    stats: &mut RetrievalStats,
) -> Result<bool> {
    let block = table.partition_point(|entry| entry.last_id < target);
    if block == table.len() { return Ok(false); }
    stats.block_seeks += 1;
    let start = if block == 0 { 0 } else { table[block - 1].byte_end };
    let end = table[block].byte_end;
    let mut prev = if block == 0 { 0 } else { table[block - 1].last_id };
    let mut pos = start;
    while pos < end {
        let delta = decode_varint(posting.bytes, &mut pos)?;
        prev = prev.checked_add(delta).ok_or_else(|| anyhow!("posting overflow"))?;
        stats.posting_entries_examined += 1;
        if prev == target { return Ok(true); }
        if prev > target { return Ok(false); }
    }
    Ok(false)
}

/// Returns None when no *complete*, selective conjunctive token plan exists.
/// A caller must then use legacy retrieval. Never truncate an overbroad
/// posting or treat unknown terms as if they matched.
pub fn retrieve(
    index: &TokenIndexView<'_>,
    normalized_query: &str,
) -> Result<Option<(Vec<u32>, RetrievalStats)>> {
    let mut tokens: Vec<&str> = tokenize_name(normalized_query).collect();
    tokens.sort_unstable();
    tokens.dedup();
    if tokens.is_empty() || tokens.len() > 8 { return Ok(None); }
    let mut postings = Vec::with_capacity(tokens.len());
    for token in &tokens {
        let Some(posting) = index.lookup(token)? else { return Ok(None); };
        if posting.document_frequency == 0 { return Ok(None); }
        postings.push(posting);
    }
    let best = postings.iter().enumerate()
        .min_by_key(|(_, p)| p.document_frequency).map(|(i, _)| i).unwrap();
    if postings[best].document_frequency > MAX_ANCHOR_DF { return Ok(None); }
    let mut stats = RetrievalStats {
        tokens: tokens.len(),
        anchor_df: postings[best].document_frequency,
        ..Default::default()
    };
    let anchor = read_anchor(&postings[best], &mut stats)?;
    let skip_tables = postings.iter().map(skips).collect::<Result<Vec<_>>>()?;
    let mut matched = Vec::new();
    for id in anchor {
        let mut ok = true;
        for (i, posting) in postings.iter().enumerate() {
            if i == best { continue; }
            if !contains(posting, &skip_tables[i], id, &mut stats)? {
                ok = false;
                break;
            }
        }
        if ok {
            matched.push(id);
            if matched.len() >= MAX_CANDIDATES { return Ok(None); }
        }
    }
    stats.candidates = matched.len();
    Ok(Some((matched, stats)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delta_varint_seek_intersection_counts_examined_entries() {
        // Blocks: [1, 4, 9], [100, 200]. Deltas encoded continuously.
        let bytes = [1u8, 3, 5, 91, 100];
        let mut skip_bytes = Vec::new();
        for (last_id, end) in [(9u32, 3u64), (200, 5)] {
            skip_bytes.extend_from_slice(&last_id.to_le_bytes());
            skip_bytes.extend_from_slice(&end.to_le_bytes());
        }
        let posting = TokenPostingList {
            document_frequency: 5, posting_start: 0,
            bytes: &bytes, skip_bytes: &skip_bytes
        };
        let table = skips(&posting).unwrap();
        let mut stats = RetrievalStats::default();
        assert_eq!(read_anchor(&posting, &mut stats).unwrap(), vec![1, 4, 9, 100, 200]);
        assert!(contains(&posting, &table, 100, &mut stats).unwrap());
        assert!(!contains(&posting, &table, 10, &mut stats).unwrap());
        assert!(!contains(&posting, &table, 201, &mut stats).unwrap());
        assert_eq!(stats.block_seeks, 2);
    }

    #[test]
    fn skip_partition_picks_first_possible_block() {
        let table = [Skip {last_id: 10, byte_end: 5}, Skip {last_id: 35, byte_end: 12}];
        assert_eq!(table.partition_point(|s| s.last_id < 11), 1);
        assert_eq!(table.partition_point(|s| s.last_id < 36), 2);
    }
}
