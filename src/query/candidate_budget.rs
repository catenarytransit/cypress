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
    fn mode_default_is_safe() {
        assert_eq!(ShortlistMode::from_env(""), ShortlistMode::Off);
        assert_eq!(ShortlistMode::from_env("shadow"), ShortlistMode::Shadow);
    }
}
