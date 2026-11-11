//! Experimental open-geocode-inspired FST/packed-postings retrieval.
//!
//! Uses Cypress's own generation-validated, mmap-backed multilingual StringIds.
//! Search only selects candidates: Cypress's established admin/place/ranking
//! pipeline evaluates all returned IDs. Never silently truncate broad prefixes.
use anyhow::Result;
use cypress::models::token_index::{tokenize_name, TokenIndexView};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Map, Streamer};

use super::token_retrieval::{self, RetrievalStats};

const MAX_PREFIX_EXPANSIONS: usize = 12;
const MAX_FAST_PATH_IDS: usize = 6_000;

/// Enumerate *all* completions only if the FST subtree is small enough.
/// Its order is lexical rather than relevance-ranked; first-N is unsafe.
fn bounded_completions(map: &Map<&[u8]>, prefix: &str) -> Vec<String> {
    if prefix.chars().count() < 3 {
        return Vec::new();
    }
    let automaton = Str::new(prefix).starts_with();
    let mut stream = map.search(automaton).into_stream();
    let mut result = Vec::new();
    while let Some((key, _)) = stream.next() {
        if result.len() >= MAX_PREFIX_EXPANSIONS {
            return Vec::new();
        }
        if let Ok(word) = std::str::from_utf8(key) {
            result.push(word.to_string());
        }
    }
    result
}

/// Prefer selective FST term conjunctions over bigram postings. Autocomplete
/// can replace a last typed prefix with *every* bounded vocabulary completion.
/// If the plan is broad/absent, the caller must use Cypress legacy retrieval.
/// This preserves alternatives for German/French/Italian/Romansh/other names.
pub fn retrieve(
    index: &TokenIndexView<'_>,
    query: &str,
) -> Result<Option<(Vec<u32>, RetrievalStats)>> {
    let words: Vec<_> = tokenize_name(query).collect();
    if words.is_empty() || words.len() > 8 {
        return Ok(None);
    }

    let mut combined = Vec::<u32>::new();
    let mut combined_stats = RetrievalStats::default();
    let mut has_plan = false;

    if let Some((ids, stats)) = token_retrieval::retrieve(index, query)? {
        if ids.len() <= MAX_FAST_PATH_IDS {
            combined = ids;
            combined_stats = stats;
            has_plan = true;
        }
    }

    // The final token may be an unfinished autocomplete prefix. An exact
    // term hit does not preclude other names beginning with that text.
    let prefix = words[words.len() - 1];
    let completions = bounded_completions(&index.fst, prefix);
    let stem = &words[..words.len() - 1];
    for completion in completions {
        if completion == prefix {
            continue;
        }
        let completed = if stem.is_empty() {
            completion
        } else {
            format!("{} {}", stem.join(" "), completion)
        };
        let Some((ids, stats)) = token_retrieval::retrieve(index, &completed)? else {
            // An unselective expansion cannot safely replace legacy retrieval.
            return Ok(None);
        };
        combined_stats.posting_entries_examined += stats.posting_entries_examined;
        combined_stats.block_seeks += stats.block_seeks;
        combined_stats.anchor_df = combined_stats.anchor_df.max(stats.anchor_df);
        combined_stats.tokens = combined_stats.tokens.max(stats.tokens);
        combined.extend(ids);
        has_plan = true;
        if combined.len() > MAX_FAST_PATH_IDS {
            return Ok(None);
        }
    }

    if !has_plan {
        return Ok(None);
    }
    combined.sort_unstable();
    combined.dedup();
    combined_stats.candidates = combined.len();
    if combined.is_empty() || combined.len() > MAX_FAST_PATH_IDS {
        return Ok(None);
    }
    Ok(Some((combined, combined_stats)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::MapBuilder;

    #[test]
    fn bounded_prefix_never_truncates_lexical_results() {
        let mut builder = MapBuilder::memory();
        for (i, word) in ["bern", "berne", "berner", "bernhard"].iter().enumerate() {
            builder.insert(word.as_bytes(), i as u64).unwrap();
        }
        let bytes = builder.into_inner().unwrap();
        let map = Map::new(bytes.as_slice()).unwrap();
        assert_eq!(bounded_completions(&map, "ber"),
                   vec!["bern", "berne", "berner", "bernhard"]);
        assert!(bounded_completions(&map, "be").is_empty());
        assert!(bounded_completions(&map, "other").is_empty());
    }

    #[test]
    fn broad_prefix_does_not_pick_arbitrary_first_twelve() {
        let mut builder = MapBuilder::memory();
        for i in 0..(MAX_PREFIX_EXPANSIONS + 1) {
            builder.insert(format!("bahnhof{i:02}"), i as u64).unwrap();
        }
        let bytes = builder.into_inner().unwrap();
        let map = Map::new(bytes.as_slice()).unwrap();
        assert!(bounded_completions(&map, "bahn").is_empty());
    }

    #[test]
    fn unicode_prefix_not_latin_only() {
        let mut builder = MapBuilder::memory();
        builder.insert("zürich".as_bytes(), 0).unwrap();
        let bytes = builder.into_inner().unwrap();
        let map = Map::new(bytes.as_slice()).unwrap();
        assert_eq!(bounded_completions(&map, "zür"), vec!["zürich"]);
    }
}
