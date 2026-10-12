/// ADR-style token match scoring — ported from adr/score.h.
/// Lower scores are better; `NO_MATCH` (f32::MAX) means rejected.
use super::sift4::{sift4, SiftOffset};
use std::sync::OnceLock;

pub const NO_MATCH: f32 = f32::MAX;

/// Experimental lexical rejection; off by default. Read once per process.
/// The query binary logs the selected mode; the compiled index is unchanged.
static LEXICAL_LOWER_BOUND: OnceLock<bool> = OnceLock::new();

fn parse_lexical_lower_bound(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes")
}

pub fn lexical_lower_bound_enabled() -> bool {
    *LEXICAL_LOWER_BOUND.get_or_init(|| {
        std::env::var("CYPRESS_LEXICAL_LOWER_BOUND")
            .map(|value| parse_lexical_lower_bound(&value))
            .unwrap_or(false)
    })
}

const TOKEN_DELIMITERS: &[u8] = b" -,;/().";

fn is_delimiter(c: u8) -> bool {
    TOKEN_DELIMITERS.contains(&c)
}

fn tokenize<'a>(s: &'a [u8], out: &mut Vec<&'a [u8]>) {
    out.clear();
    let mut start: Option<&[u8]> = None;
    for (i, &c) in s.iter().enumerate() {
        if is_delimiter(c) {
            if let Some(s) = start.take() {
                out.push(&s[..i - (s.as_ptr() as usize - s.as_ptr() as usize)]);
            }
        } else if start.is_none() {
            start = Some(&s[i..]);
        }
    }
    if let Some(s) = start {
        out.push(s);
    }
}

fn tokenize_bytes<'a>(s: &'a [u8], out: &mut Vec<&'a [u8]>) {
    out.clear();
    let mut i = 0;
    while i < s.len() {
        if is_delimiter(s[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < s.len() && !is_delimiter(s[i]) {
            i += 1;
        }
        out.push(&s[start..i]);
    }
}

#[inline]
pub fn get_exact_match_score(query_len: usize) -> f32 {
    -2.0 - query_len as f32 * 0.75
}

/// Score a prefix match whose prefix relationship is already known by the
/// caller (for example, by an FST traversal). This is exactly the score that
/// `get_token_match_score` produces without invoking SIFT4.
pub fn get_known_prefix_match_score(dataset_token: &[u8], query: &[u8]) -> f32 {
    if dataset_token.is_empty() || query.is_empty() || !dataset_token.starts_with(query) {
        return NO_MATCH;
    }
    if dataset_token == query {
        return get_exact_match_score(query.len());
    }
    let cut = &dataset_token[..query.len()];
    finish_token_match_score(dataset_token, query, cut, 0)
}

fn finish_token_match_score(dataset_token: &[u8], query: &[u8], cut: &[u8], dist: usize) -> f32 {
    let cut_len = cut.len();
    if cut_len == 0 || dist >= cut_len {
        return NO_MATCH;
    }

    let overhang_penalty =
        ((dataset_token.len().saturating_sub(query.len())) as f32 / 4.0).min(4.0);
    let relative_coverage = 6.0 * (dist as f32 / cut_len as f32);

    let mut common_prefix_bonus = 0.0_f32;
    let end = cut_len.min(query.len());
    for i in 0..end {
        if cut[i] != query[i] {
            break;
        }
        common_prefix_bonus -= 0.25;
    }

    let first_letter_penalty = if cut[0] != query[0] { 2.0 } else { -0.5 };
    let second_letter_penalty = if cut.len() > 1 && query.len() > 1 {
        if cut[1] != query[1] {
            1.0
        } else {
            -0.25
        }
    } else {
        -0.25
    };

    let score = dist as f32
        + first_letter_penalty
        + second_letter_penalty
        + overhang_penalty
        + relative_coverage
        + common_prefix_bonus;

    let max = (cut_len as f32 / 2.0).ceil();
    if score > max {
        NO_MATCH
    } else {
        score
    }
}

/// A zero-edit-distance optimistic lower bound on `finish_token_match_score`.
/// All distance-dependent terms are nonnegative. The remaining terms are
/// exact multiples of 0.25; calculate them in integer quarter-score units.
/// Require a margin of >0.25 score points to conservatively accommodate
/// floating-point addition order in the original scorer. For very long
/// strings, retain the original path rather than depending on that margin.
/// This function must be called only for nonempty tokens / slices.
fn definitely_rejected_before_sift4(dataset_token: &[u8], query: &[u8], cut: &[u8]) -> bool {
    let n = cut.len();
    if n == 0 || n > 256 {
        return false;
    }
    let common_prefix = cut.iter().zip(query.iter()).take_while(|(a, b)| a == b).count();
    let overhang_quarters = dataset_token.len().saturating_sub(query.len()).min(16) as i32;
    let first_quarters = if cut[0] != query[0] { 8 } else { -2 };
    let second_quarters = if n > 1 && query.len() > 1 {
        if cut[1] != query[1] { 4 } else { -1 }
    } else {
        -1
    };
    let optimistic_quarters = first_quarters + second_quarters
        + overhang_quarters - common_prefix as i32;
    let allowed_quarters = 4 * n.div_ceil(2) as i32;
    optimistic_quarters > allowed_quarters + 1
}

fn get_token_match_score_with_mode(
    dataset_token: &[u8],
    query: &[u8],
    offset_arr: &mut Vec<SiftOffset>,
    use_lower_bound: bool,
) -> f32 {
    if dataset_token.is_empty() || query.is_empty() {
        return NO_MATCH;
    }

    if dataset_token == query {
        return get_exact_match_score(query.len());
    }

    let cut_len = dataset_token.len().min(query.len());
    let cut = &dataset_token[..cut_len];

    // Autocomplete fast path. When the stored alias starts with the query,
    // `cut` is byte-for-byte equal to `query`, so SIFT4's distance is provably
    // zero. Feed that exact distance into the existing score formula.
    if dataset_token.starts_with(query) {
        return get_known_prefix_match_score(dataset_token, query);
    }

    if use_lower_bound && definitely_rejected_before_sift4(dataset_token, query, cut) {
        return NO_MATCH;
    }
    let max_dist = cut_len / 2 + 2;
    let dist = sift4(cut, query, 3, max_dist, offset_arr);
    finish_token_match_score(dataset_token, query, cut, dist)
}

fn get_token_match_score(
    dataset_token: &[u8],
    query: &[u8],
    offset_arr: &mut Vec<SiftOffset>,
) -> f32 {
    get_token_match_score_with_mode(dataset_token, query, offset_arr, false)
}

/// Token boundaries prepared once per candidate string and then reused for
/// every query phrase. This preserves get_match_score's scoring semantics while
/// removing repeated delimiter scans and token-vector allocations.
pub struct PreparedMatchCandidate<'a> {
    dataset_name: &'a [u8],
    token_ranges: [(usize, usize); 8],
    token_count: usize,
    use_lower_bound: bool,
}

impl<'a> PreparedMatchCandidate<'a> {
    pub fn new(dataset_name: &'a [u8]) -> Self {
        Self::new_with_mode(dataset_name, lexical_lower_bound_enabled())
    }

    fn new_with_mode(dataset_name: &'a [u8], use_lower_bound: bool) -> Self {
        let mut token_ranges = [(0usize, 0usize); 8];
        let mut token_count = 0usize;
        let mut i = 0usize;

        while i < dataset_name.len() && token_count < token_ranges.len() {
            while i < dataset_name.len() && is_delimiter(dataset_name[i]) {
                i += 1;
            }
            if i >= dataset_name.len() {
                break;
            }

            let start = i;
            while i < dataset_name.len() && !is_delimiter(dataset_name[i]) {
                i += 1;
            }
            token_ranges[token_count] = (start, i);
            token_count += 1;
        }

        Self {
            dataset_name,
            token_ranges,
            token_count,
            use_lower_bound,
        }
    }

    #[inline]
    fn token(&self, idx: usize) -> &'a [u8] {
        let (start, end) = self.token_ranges[idx];
        &self.dataset_name[start..end]
    }

    #[inline]
    fn token_score(&self, dataset_token: &[u8], query: &[u8], offset_arr: &mut Vec<SiftOffset>) -> f32 {
        get_token_match_score_with_mode(dataset_token, query, offset_arr, self.use_lower_bound)
    }

    /// Scores this prepared dataset name against one query phrase.
    pub fn score(&self, query: &[u8], offset_arr: &mut Vec<SiftOffset>) -> f32 {
        if self.dataset_name.is_empty() || query.is_empty() {
            return NO_MATCH;
        }

        let fallback = self.token_score(self.dataset_name, query, offset_arr);

        if self.token_count <= 1 {
            return fallback;
        }

        let mut best_score = NO_MATCH;
        let mut best_token_bits: u8 = 0;

        // Try all contiguous sub-phrases up to length 4. The same stack buffer
        // behavior as get_match_score is retained; only tokenization is reused.
        for from in 0..self.token_count {
            for len in 1..=4.min(self.token_count - from) {
                let to = from + len;
                let token_bits: u8 = ((1u16 << to) - (1u16 << from)) as u8;

                let mut buf_storage = [0u8; 256];
                let score = if len == 1 {
                    self.token_score(self.token(from), query, offset_arr)
                } else {
                    let total_len: usize =
                        (from..to).map(|idx| self.token(idx).len()).sum::<usize>() + len - 1;

                    if total_len <= buf_storage.len() {
                        let mut buf_len = 0;
                        for idx in from..to {
                            if idx != from {
                                buf_storage[buf_len] = b' ';
                                buf_len += 1;
                            }
                            let token = self.token(idx);
                            buf_storage[buf_len..buf_len + token.len()].copy_from_slice(token);
                            buf_len += token.len();
                        }
                        self.token_score(&buf_storage[..buf_len], query, offset_arr)
                    } else {
                        let mut phrase = Vec::with_capacity(total_len);
                        for idx in from..to {
                            if idx != from {
                                phrase.push(b' ');
                            }
                            phrase.extend_from_slice(self.token(idx));
                        }
                        self.token_score(&phrase, query, offset_arr)
                    }
                };

                if score < best_score {
                    best_score = score;
                    best_token_bits = token_bits;
                }
            }
        }

        if best_score == NO_MATCH {
            return NO_MATCH;
        }

        let mut sum = best_score;
        let mut n_not_matched = 0u32;
        for s_idx in 0..self.token_count {
            if (best_token_bits & (1 << s_idx)) == 0 {
                n_not_matched += 1;
                let penalty = (self.token(s_idx).len() as f32 / 4.0).clamp(0.75, 3.0);
                sum += penalty;
            }
        }

        if n_not_matched as usize == self.token_count {
            return NO_MATCH;
        }

        let max = (self.dataset_name.len().min(query.len()) as f32 / 2.0).ceil();
        let score = fallback.min(sum);
        if score >= max {
            NO_MATCH
        } else {
            score
        }
    }
}

/// Scores a dataset name against a single query input.
/// Kept as a compatibility wrapper for existing callers and tests.
pub fn get_match_score(dataset_name: &[u8], query: &[u8], offset_arr: &mut Vec<SiftOffset>) -> f32 {
    PreparedMatchCandidate::new(dataset_name).score(query, offset_arr)
}

/// ADR-style category bonus for place types based on layer rank.
pub fn get_layer_score_bonus(layer_rank: u8) -> f32 {
    match layer_rank {
        9 => 3.0, // Country
        8 => 2.5, // MacroRegion
        7 => 2.5, // Region
        6 => 2.0, // MacroCounty
        5 => 2.0, // County
        4 => 2.0, // LocalAdmin
        3 => 3.0, // Locality — cities are high-value like ADR's kCity
        2 => 1.0, // Borough
        1 => 0.5, // Neighbourhood
        _ => 0.0, // Street/Address/Venue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_bound_is_opt_in() {
        for value in ["", "0", "off", "false", "garbage"] {
            assert!(!parse_lexical_lower_bound(value));
        }
        for value in ["1", "yes", "on", "TRUE", " On "] {
            assert!(parse_lexical_lower_bound(value));
        }
    }

    #[test]
    fn optimistic_rejection_keeps_token_scores_bitwise_identical() {
        let mut words = vec![Vec::new()];
        for len in 1..=5 {
            for number in 0..3usize.pow(len) {
                let mut number = number;
                let mut word = vec![0u8; len as usize];
                for character in &mut word {
                    *character = b'a' + (number % 3) as u8;
                    number /= 3;
                }
                words.push(word);
            }
        }
        let mut offsets = Vec::new();
        for data in &words {
            for query in &words {
                let old = get_token_match_score_with_mode(data, query, &mut offsets, false);
                let new = get_token_match_score_with_mode(data, query, &mut offsets, true);
                assert_eq!(old.to_bits(), new.to_bits(), "{data:?} vs {query:?}");
            }
        }
    }

    #[test]
    fn optimized_prepared_scorer_matches_scalar_on_address_and_poi_phrases() {
        let names: &[&[u8]] = &[
            b"4885 paseo de vega", b"santa monica pier", b"london eye",
            b"uc irvine", b"university of california irvine",
            b"pariser platz 1", b"avenue anatole france", b"brandenburger tor",
            b"tokyo tower", b"hauptbahnhof", b"1604 primrose ave",
            b"5 avenue des champs elysees", b"sydney opera house",
            b"new york city", b"",
        ];
        let phrases: &[&[u8]] = &[
            b"irvine", b"uc irvine", b"paseo de vega", b"4885",
            b"santa monica pier california", b"pier", b"london eye",
            b"paris", b"new york", b"brandenburger tor",
            b"hbf", b"u", b"zz", b"a b", b"",
        ];
        let mut offsets = Vec::new();
        for &data in names {
            let old = PreparedMatchCandidate::new_with_mode(data, false);
            let new = PreparedMatchCandidate::new_with_mode(data, true);
            for &query in phrases {
                assert_eq!(
                    old.score(query, &mut offsets).to_bits(),
                    new.score(query, &mut offsets).to_bits(),
                    "name={data:?}, query={query:?}",
                );
            }
        }
    }

    #[test]
    fn exact_match_best_score() {
        let mut offset_arr = Vec::new();
        let score = get_match_score(b"paris", b"paris", &mut offset_arr);
        assert!(score < -4.0);
        assert_ne!(score, NO_MATCH);
    }

    #[test]
    fn prefix_match_good_score() {
        let mut offset_arr = Vec::new();
        let score = get_match_score(b"paris", b"par", &mut offset_arr);
        assert_ne!(score, NO_MATCH);
        assert!(score < 3.0);
    }

    #[test]
    fn no_match_unrelated() {
        let mut offset_arr = Vec::new();
        let score = get_match_score(b"london", b"xyz", &mut offset_arr);
        assert_eq!(score, NO_MATCH);
    }

    #[test]
    fn multi_token_match() {
        let mut offset_arr = Vec::new();
        let score = get_match_score(b"new york city", b"new york", &mut offset_arr);
        assert_ne!(score, NO_MATCH);
        assert!(score < 2.0);
    }

    #[test]
    fn typo_tolerance() {
        let mut offset_arr = Vec::new();
        let score = get_match_score(b"paris", b"pars", &mut offset_arr);
        assert_ne!(score, NO_MATCH);
    }

    #[test]
    fn admin_place_bonus() {
        assert!(get_layer_score_bonus(3) > get_layer_score_bonus(0));
        assert!(get_layer_score_bonus(9) > get_layer_score_bonus(1));
    }

    #[test]
    fn multi_token_full_match() {
        let mut offset_arr = Vec::new();
        let tokens: Vec<&[u8]> = vec![b"new", b"york"];
        let (score, matched) =
            get_multi_token_match_score(b"new york city", &tokens, &mut offset_arr);
        assert_ne!(score, NO_MATCH);
        assert_eq!(matched, 0b11);
    }

    #[test]
    fn multi_token_partial_match() {
        let mut offset_arr = Vec::new();
        let tokens: Vec<&[u8]> = vec![b"roma", b"italia"];
        let (score, matched) = get_multi_token_match_score(b"roma", &tokens, &mut offset_arr);
        assert_ne!(score, NO_MATCH);
        assert_eq!(matched & 1, 1);
        assert_eq!(matched & 2, 0);
    }
}

/// Multi-token scoring: greedily matches each query token against dataset
/// name tokens. Returns (combined_score, matched_token_bitmask).
/// Unmatched query tokens are NOT penalized here — the caller applies
/// area matching and penalties for unmatched tokens.
pub fn get_multi_token_match_score(
    dataset_name: &[u8],
    query_tokens: &[&[u8]],
    offset_arr: &mut Vec<SiftOffset>,
) -> (f32, u8) {
    if dataset_name.is_empty() || query_tokens.is_empty() {
        return (NO_MATCH, 0);
    }

    if query_tokens.len() == 1 {
        let score = get_match_score(dataset_name, query_tokens[0], offset_arr);
        let mask = if score != NO_MATCH { 1u8 } else { 0u8 };
        return (score, mask);
    }

    let max_tokens = query_tokens.len().min(8);
    let mut s_tokens: Vec<&[u8]> = Vec::new();
    tokenize_bytes(dataset_name, &mut s_tokens);
    if s_tokens.is_empty() {
        return (NO_MATCH, 0);
    }
    let max_s = s_tokens.len().min(8);
    let s_tokens = &s_tokens[..max_s];

    // Score matrix: query_token[i] vs dataset_token[j]
    let mut scores = [[NO_MATCH; 8]; 8];
    for (qi, qt) in query_tokens[..max_tokens].iter().enumerate() {
        for (si, st) in s_tokens.iter().enumerate() {
            scores[qi][si] = get_token_match_score(st, qt, offset_arr);
        }
    }

    // Greedy assignment: pick best (query_token, dataset_token) pairs
    let mut matched_q = 0u8;
    let mut matched_s = 0u8;
    let mut total_score = 0.0f32;
    let mut n_matched = 0u32;

    for _ in 0..max_tokens.min(max_s) {
        let mut best = NO_MATCH;
        let mut best_qi = 0usize;
        let mut best_si = 0usize;
        for qi in 0..max_tokens {
            if matched_q & (1 << qi) != 0 {
                continue;
            }
            for si in 0..max_s {
                if matched_s & (1 << si) != 0 {
                    continue;
                }
                if scores[qi][si] < best {
                    best = scores[qi][si];
                    best_qi = qi;
                    best_si = si;
                }
            }
        }
        if best == NO_MATCH {
            break;
        }
        matched_q |= 1 << best_qi;
        matched_s |= 1 << best_si;
        total_score += best;
        n_matched += 1;
    }

    if n_matched == 0 {
        return (NO_MATCH, 0);
    }

    // Penalty for unmatched dataset tokens
    for si in 0..max_s {
        if matched_s & (1 << si) == 0 {
            let penalty = (s_tokens[si].len() as f32 / 4.0).clamp(0.75, 3.0);
            total_score += penalty;
        }
    }

    (total_score, matched_q)
}

/// Score a single query token against a raw area name byte slice.
pub fn get_area_match_score(
    area_name: &[u8],
    query_token: &[u8],
    offset_arr: &mut Vec<SiftOffset>,
) -> f32 {
    get_match_score(area_name, query_token, offset_arr)
}
