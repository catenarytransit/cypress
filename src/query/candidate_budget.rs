//! Opt-in candidate-budget experiment for worldwide autocomplete.
//!
//! The baseline always receives every ID. Shadow mode calculates the same
//! shortlist without affecting results, so rollout decisions can be based on
//! real top-K comparisons instead of an arbitrary candidate limit.
//!
//! Exact full-span and exact sub-phrase matches always outrank weak prefixes.
//! Lower-priority candidates are selected by the strongest existing place
//! importance. No field order or numeric-token assumptions enter this policy.

use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShortlistMode {
    Off,
    Shadow,
    On,
}

impl ShortlistMode {
    pub fn from_env(value: &str) -> Self {
        match value {
            "shadow" => Self::Shadow,
            "on" => Self::On,
            _ => Self::Off,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Quality {
    /// Full query equals the stored string.
    pub full_exact: bool,
    /// Stored string equals one of the query's exact contiguous phrases.
    pub phrase_exact: bool,
    /// A multiword phrase is a prefix of the stored name.
    pub multiword_prefix: bool,
    /// Whole query is a prefix of the stored name.
    pub full_prefix: bool,
    /// Existing place importance, plus category bonus if available.
    pub importance: f32,
    /// Shorter names rank ahead when otherwise tied.
    pub bytes: usize,
}
impl Quality {
    pub fn compare(&self, other: &Self) -> Ordering {
        self.full_exact
            .cmp(&other.full_exact)
            .then(self.phrase_exact.cmp(&other.phrase_exact))
            .then(self.full_prefix.cmp(&other.full_prefix))
            .then(self.multiword_prefix.cmp(&other.multiword_prefix))
            .then_with(|| self.importance.total_cmp(&other.importance))
            .then_with(|| other.bytes.cmp(&self.bytes))
    }
}
#[derive(Debug)]
pub struct Selection {
    /// Sorted StringIds, suitable for binary_search in hot query loops.
    pub ids: Vec<u32>,
    pub exact_retained: usize,
    pub candidates_before: usize,
}
pub fn select(
    candidates: &[u32],
    budget: usize,
    mut quality: impl FnMut(u32) -> Quality,
) -> Selection {
    let mut ranked: Vec<(u32, Quality)> = candidates
        .iter()
        .copied()
        .map(|id| (id, quality(id)))
        .collect();
    ranked.sort_unstable_by(|(a, qa), (b, qb)| qb.compare(qa).then(a.cmp(b)));
    // Preserve every exact name/phrase regardless of the ordinary budget;
    // this is essential for ambiguous languages and duplicate locality names.
    let exact_retained = ranked
        .iter()
        .take_while(|(_, q)| q.phrase_exact || q.full_exact)
        .count();
    let retain = budget.max(exact_retained).min(ranked.len());
    let mut ids: Vec<u32> = ranked.iter().take(retain).map(|&(id, _)| id).collect();
    ids.sort_unstable();
    ids.dedup();
    Selection {
        ids,
        exact_retained,
        candidates_before: candidates.len(),
    }
}
/// Read the best place quality from already validated dense/legacy columns.
/// A missing legacy importance column must not crash an experimental query.
/// At most 32 mapped places are inspected, as in the original heuristic.
pub fn best_mapped_importance(
    place_ids: &[u32],
    place_importances: &[f32],
    place_count: usize,
    mut prior: impl FnMut(usize) -> f32,
) -> f32 {
    place_ids
        .iter()
        .take(32)
        .filter_map(|&id| {
            let idx = id as usize;
            if idx >= place_count {
                return None;
            }
            let importance = place_importances.get(idx).copied().unwrap_or(0.0);
            Some(prior(idx) + importance)
        })
        .fold(0.0_f32, f32::max)
}

/// Name-link-only shadow diagnostic (not exact result recall). Each StringId's
/// place postings are sorted by PlaceId during compilation. Binary search
/// avoids scanning large posting lists for each shortlisted StringId.
pub fn shadow_name_coverage(
    shortlisted_strings: &[u32],
    string_place_offsets: &[u32],
    string_place_ids: &[u32],
    result_place_ids: impl IntoIterator<Item = u32>,
) -> usize {
    result_place_ids
        .into_iter()
        .filter(|&place_id| {
            shortlisted_strings.iter().any(|&sid| {
                let pos = sid as usize;
                let (Some(&start), Some(&end)) = (
                    string_place_offsets.get(pos),
                    string_place_offsets.get(pos + 1),
                ) else {
                    return false;
                };
                let (start, end) = (start as usize, end as usize);
                string_place_ids
                    .get(start..end)
                    .is_some_and(|ids| ids.binary_search(&place_id).is_ok())
            })
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn q(phrase: bool, priority: f32) -> Quality {
        Quality {
            full_exact: false,
            phrase_exact: phrase,
            multiword_prefix: false,
            full_prefix: false,
            importance: priority,
            bytes: 10,
        }
    }
    #[test]
    fn exact_hits_cannot_be_evicted_by_common_prefixes() {
        let items = [0, 1, 2, 3, 4];
        let s = select(&items, 2, |id| match id {
            3 | 4 => q(true, 0.),
            _ => q(false, 1000.),
        });
        assert_eq!(s.ids, vec![3, 4]);
        assert_eq!(s.exact_retained, 2);
    }
    #[test]
    fn exact_matches_can_exceed_budget() {
        let s = select(&[5, 3, 1], 1, |_| q(true, 0.));
        assert_eq!(s.ids, vec![1, 3, 5]);
    }
    #[test]
    fn cheap_importance_sort_is_stable() {
        let s = select(&[11, 12, 13], 1, |id| q(false, id as f32));
        assert_eq!(s.ids, vec![13]);
    }
    #[test]
    fn dense_importances_are_read_without_legacy_vec() {
        // Exactly the dense layout case: archived importance Vec is empty, but
        // QueryDbView supplies the populated dense slice.
        let dense = [0.5, 2.5, 1.0];
        let actual = best_mapped_importance(&[0, 1, 2], &dense, 3, |idx| idx as f32);
        assert_eq!(actual, 3.5);
    }

    #[test]
    fn old_or_missing_importance_column_never_panics() {
        let legacy = [0.5, 1.5, 2.5];
        assert_eq!(best_mapped_importance(&[0, 2], &legacy, 3, |_| 5.0), 7.5);
        // Defensive fallback covers malformed/missing vectors, even though
        // valid compiled generations have a full importance column.
        assert_eq!(
            best_mapped_importance(&[0, 110_538_418], &[], 2, |_| 5.0),
            5.0
        );
        assert_eq!(best_mapped_importance(&[1234], &[], 2, |_| 0.0), 0.0);
    }

    #[test]
    fn shadow_coverage_uses_sorted_postings_and_bounds_checks() {
        // String 0 -> places 3, 7; string 1 -> place 8.
        let offsets = [0, 2, 3];
        let postings = [3, 7, 8];
        assert_eq!(
            shadow_name_coverage(&[0, 1], &offsets, &postings, [3, 8, 99]),
            2
        );
        assert_eq!(
            shadow_name_coverage(&[0, 999], &offsets, &postings, [7, 8]),
            1
        );
        assert_eq!(shadow_name_coverage(&[1], &offsets, &postings, [3, 8]), 1);
        assert_eq!(shadow_name_coverage(&[], &offsets, &postings, [3, 8]), 0);
    }

    #[test]
    fn mode_default_is_safe() {
        assert_eq!(ShortlistMode::from_env(""), ShortlistMode::Off);
        assert_eq!(ShortlistMode::from_env("shadow"), ShortlistMode::Shadow);
    }
}
