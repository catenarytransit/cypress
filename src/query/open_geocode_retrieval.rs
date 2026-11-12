//! Alternate packed-memory retrieval (no prefix completion expansion).
//!
//! The compiler stores fixed-width u32 postings in document-frequency buckets,
//! while the lexical FST only stores term -> metadata ordinal. Lookup is
//! allocation-free; intersection binary-searches mmap slices directly. Cypress
//! still owns multilingual normalization, administration joins and all ranking.
use anyhow::Result;
use cypress::models::packed_token::{PackedPosting, PackedTokenIndexView};
use cypress::models::token_index::tokenize_name;
use super::token_retrieval::RetrievalStats;

const MAX_ANCHOR_DF: u32 = 50_000;
const MAX_FAST_PATH_IDS: usize = 6_000;

/// Return None for broad or absent plans: the caller must fall back to the
/// original bigram/fuzzy retrieval, without choosing an arbitrary subset.
pub fn retrieve(
    index: &PackedTokenIndexView<'_>,
    query: &str,
) -> Result<Option<(Vec<u32>, RetrievalStats)>> {
    let mut words: Vec<_> = tokenize_name(query).collect();
    words.sort_unstable();
    words.dedup();
    if words.is_empty() || words.len() > 8 {
        return Ok(None);
    }
    let mut lists: Vec<PackedPosting<'_>> = Vec::with_capacity(words.len());
    for word in words {
        let Some(posting) = index.lookup(word)? else {
            return Ok(None);
        };
        if posting.document_frequency == 0 {
            return Ok(None);
        }
        lists.push(posting);
    }
    let anchor = lists.iter().enumerate()
        .min_by_key(|(_, posting)| posting.document_frequency)
        .map(|(index, _)| index).unwrap();
    let anchor_df = lists[anchor].document_frequency;
    if anchor_df > MAX_ANCHOR_DF {
        return Ok(None);
    }
    let mut stats = RetrievalStats {
        tokens: lists.len(),
        anchor_df,
        ..Default::default()
    };
    let mut matched = Vec::new();
    for i in 0..anchor_df as usize {
        let id = lists[anchor].id_at(i);
        stats.posting_entries_examined += 1;
        let mut present = true;
        for (position, posting) in lists.iter().enumerate() {
            if position != anchor {
                // Indexed u32 binary search touches O(log df) cache lines
                // without the old per-query skip-table allocation/varints.
                if !posting.contains(id) {
                    present = false;
                    break;
                }
            }
        }
        if present {
            matched.push(id);
            if matched.len() > MAX_FAST_PATH_IDS {
                return Ok(None);
            }
        }
    }
    stats.candidates = matched.len();
    if matched.is_empty() { return Ok(None); }
    Ok(Some((matched, stats)))
}
