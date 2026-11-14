//! Exact fuzzy bigram counting in cache-sized StringId blocks.
//!
//! Token AND and fuzzy T-overlap are different operations. This implements
//! blocked ScanCount (Kaser/Lemire, arXiv:1402.4466 §5.1), using the sorted
//! production bigram postings directly. No new index or ID permutation.
//!
//! An 8 KiB byte counter block replaces corpus-sized count/epoch arrays.
//! Replay the local posting slices to emit final saturating counts in legacy
//! discovery order. Only threshold survivors are materialized, with no
//! all-touched cache snapshot, origin table, or global survivor sort.
const BLOCK_BITS: u32 = 13;
const BLOCK_SIZE: usize = 1 << BLOCK_BITS;
const BLOCK_MASK: u32 = BLOCK_SIZE as u32 - 1;

/// Inputs must be individually sorted and unique (compiler invariant), in
/// query-bigram order. Output preserves legacy first-touch order, including
/// ties later resolved by the existing candidate cap. Counts saturate at 255.
/// Threshold is a minimum, not a cap on the counts used for cosine scoring.
pub fn count_threshold(postings: &[&[u32]], threshold: u16) -> Vec<(u32, u8)> {
    if threshold == 0 || threshold > 255 || threshold as usize > postings.len() {
        return Vec::new();
    }
    let mut remaining = postings.to_vec();
    let mut counts = vec![0u8; BLOCK_SIZE];
    let mut blocks = vec![&[][..]; postings.len()];
    let mut matches = vec![Vec::new(); postings.len()];
    while let Some(base) = remaining.iter().filter_map(|p| p.first()).min().copied() {
        let high = base >> BLOCK_BITS;
        counts.fill(0);
        for (posting, block) in remaining.iter_mut().zip(&mut blocks) {
            // The smallest next ID selects this block. All remaining prefixes
            // are >= high, making equality a monotone prefix predicate.
            let end = posting.partition_point(|id| id >> BLOCK_BITS == high);
            let (head, rest) = posting.split_at(end);
            *posting = rest;
            *block = head;
            for &id in head {
                let low = (id & BLOCK_MASK) as usize;
                counts[low] = counts[low].saturating_add(1);
            }
        }
        // Replay cache-local slices in legacy first-touch order. Zeroing an
        // emitted count prevents duplicates, with no per-entry epochs/origins
        // or global survivor sort. Reading the final count keeps cosine exact.
        for (block, output) in blocks.iter().zip(&mut matches) {
            for &id in *block {
                let count = &mut counts[(id & BLOCK_MASK) as usize];
                if *count as u16 >= threshold {
                    output.push((id, *count));
                    *count = 0;
                }
            }
        }
    }
    matches.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn oracle(postings: &[&[u32]], threshold: u16) -> Vec<(u32, u8)> {
        let mut counts = BTreeMap::<u32, u8>::new();
        let mut order = Vec::new();
        for p in postings {
            for &id in p.iter() {
                let count = counts.entry(id).or_insert_with(|| {
                    order.push(id);
                    0
                });
                *count = count.saturating_add(1);
            }
        }
        order
            .into_iter()
            .filter_map(|id| {
                let n = counts[&id];
                (n as u16 >= threshold).then_some((id, n))
            })
            .collect()
    }

    #[test]
    fn boundaries_empty_lists_saturation_and_discovery_order() {
        let a = [0, 8191, 8192, 65535, u32::MAX];
        let b = [1, 8192, 65535, u32::MAX];
        let c = [1, 8191, 65535];
        let lists: Vec<&[u32]> = vec![&a, &[], &b, &c];
        for t in 1..=5 {
            assert_eq!(count_threshold(&lists, t), oracle(&lists, t));
        }
        assert!(count_threshold(&[], 2).is_empty());
        let repeated = vec![&a[..]; 300];
        for t in [1, 2, 254, 255, 256] {
            assert_eq!(count_threshold(&repeated, t), oracle(&repeated, t));
        }
    }

    #[test]
    fn randomized_threshold_counts_match_legacy_including_order() {
        let mut seed = 19u32;
        for trial in 0..40 {
            let mut lists = vec![Vec::new(); 3 + trial % 17];
            for id in 0..40000u32 {
                for list in &mut lists {
                    seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                    if seed >> 28 == 0 {
                        list.push(id);
                    }
                }
            }
            let lists: Vec<_> = lists.iter().map(Vec::as_slice).collect();
            for t in [1, 2, 3, 5, 8] {
                assert_eq!(count_threshold(&lists, t), oracle(&lists, t));
            }
        }
    }
}

#[cfg(test)]
mod timing {
    use super::*;
    use std::{hint::black_box, time::Instant};

    // Mirror production epoch ScanCount + snapshot + minimum-count filter,
    // with arrays reused across iterations. No binary-search straw baseline.
    fn legacy(
        lists: &[&[u32]],
        threshold: u16,
        counts: &mut [u8],
        epochs: &mut [u32],
        epoch: u32,
    ) -> Vec<(u32, u8)> {
        let mut touched = Vec::new();
        for p in lists {
            for &id in *p {
                let i = id as usize;
                if epochs[i] != epoch {
                    epochs[i] = epoch;
                    counts[i] = 1;
                    touched.push(id);
                } else {
                    counts[i] = counts[i].saturating_add(1);
                }
            }
        }
        let snapshot_ids = touched.clone();
        let snapshot_counts: Vec<_> = touched.iter().map(|&id| counts[id as usize]).collect();
        black_box((&snapshot_ids, &snapshot_counts));
        touched
            .into_iter()
            .filter_map(|id| {
                let count = counts[id as usize];
                (count as u16 >= threshold).then_some((id, count))
            })
            .collect()
    }

    #[test]
    #[ignore = "manual blocked ScanCount timing; no HTTP speedup claim"]
    fn benchmark_blocked_vs_epoch_counts() {
        for (label, n, list_count, mask, threshold) in [
            ("broad-fuzzy", 8_000_000u32, 16, 15, 4),
            ("dense-short", 2_000_000, 6, 3, 3),
            ("sparse", 8_000_000, 12, 255, 3),
        ] {
            let mut lists = vec![Vec::new(); list_count];
            for id in 0..n {
                for (i, list) in lists.iter_mut().enumerate() {
                    // Deterministic sparse distribution, independent per gram.
                    let mut h = id.wrapping_add((i as u32).wrapping_mul(0x9e3779b9));
                    h = (h ^ (h >> 16)).wrapping_mul(0x85ebca6b);
                    h = (h ^ (h >> 13)).wrapping_mul(0xc2b2ae35);
                    h ^= h >> 16;
                    if h & mask == 0 {
                        list.push(id);
                    }
                }
            }
            let lists: Vec<_> = lists.iter().map(Vec::as_slice).collect();
            let mut counts = vec![0; n as usize];
            let mut epochs = vec![0; n as usize];
            let expected = legacy(&lists, threshold, &mut counts, &mut epochs, 1);
            assert_eq!(count_threshold(&lists, threshold), expected);
            let mut old = Vec::new();
            let mut new = Vec::new();
            // Alternate order to reduce warm-cache/order bias.
            for run in 0..8 {
                let mut measure_old = || {
                    let start = Instant::now();
                    black_box(legacy(
                        black_box(&lists),
                        threshold,
                        &mut counts,
                        &mut epochs,
                        run + 2,
                    ));
                    old.push(start.elapsed().as_secs_f64());
                };
                let mut measure_new = || {
                    let start = Instant::now();
                    black_box(count_threshold(black_box(&lists), threshold));
                    new.push(start.elapsed().as_secs_f64());
                };
                if run % 2 == 0 {
                    measure_old();
                    measure_new();
                } else {
                    measure_new();
                    measure_old();
                }
            }
            old.sort_by(f64::total_cmp);
            new.sort_by(f64::total_cmp);
            eprintln!("{label}: entries={}, survivors={}, epoch_ms={:.3}, blocked_ms={:.3}, speedup={:.2}x",
                lists.iter().map(|p| p.len()).sum::<usize>(), expected.len(),
                old[4] * 1000.0, new[4] * 1000.0, old[4] / new[4]);
        }
    }
}
