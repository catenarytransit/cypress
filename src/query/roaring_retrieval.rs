//! Adaptive posting intersection for the mmap Roaring-style index.
//!
//! Sort by document frequency, enumerate only the rarest term, and test
//! ascending-frequency probe terms. Sparse containers binary-search u16 lows;
//! dense ones answer using a single bit test. No query-time skip allocations,
//! no prefix expansion and no arbitrary truncation of broad candidate sets.
use anyhow::Result;
use cypress::models::roaring_token::{RoaringPosting, RoaringTokenIndexView};
use cypress::models::token_index::tokenize_name;
use super::token_retrieval::RetrievalStats;

const MAX_ANCHOR_DF: u32 = 50_000;
const MAX_FAST_PATH_IDS: usize = 6_000;

pub fn retrieve(
    index: &RoaringTokenIndexView<'_>,
    query: &str,
) -> Result<Option<(Vec<u32>, RetrievalStats)>> {
    let mut terms: Vec<_> = tokenize_name(query).collect();
    terms.sort_unstable();
    terms.dedup();
    if terms.is_empty() || terms.len() > 8 { return Ok(None); }
    let mut lists: Vec<RoaringPosting<'_>> = Vec::with_capacity(terms.len());
    for term in terms {
        let Some(p) = index.lookup(term)? else { return Ok(None) };
        if p.document_frequency == 0 { return Ok(None); }
        lists.push(p);
    }
    lists.sort_unstable_by_key(|p| p.document_frequency);
    let anchor_df = lists[0].document_frequency;
    if anchor_df > MAX_ANCHOR_DF { return Ok(None); }
    let mut stats = RetrievalStats {
        tokens: lists.len(), anchor_df, ..Default::default()
    };
    let mut matched = Vec::new();
    for id in lists[0].anchor_ids() {
        stats.posting_entries_examined += 1;
        if lists[1..].iter().all(|p| {
            stats.block_seeks += 1;
            p.contains(id)
        }) {
            matched.push(id);
            if matched.len() > MAX_FAST_PATH_IDS {
                // Fall back to full legacy search instead of dropping results.
                return Ok(None);
            }
        }
    }
    stats.candidates = matched.len();
    if matched.is_empty() { return Ok(None); }
    Ok(Some((matched, stats)))
}
