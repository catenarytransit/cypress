/// ADR-style token match scoring — ported from adr/score.h.
/// Lower scores are better; `NO_MATCH` (f32::MAX) means rejected.
use super::sift4::sift4;

pub const NO_MATCH: f32 = f32::MAX;

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

fn get_token_match_score(dataset_token: &[u8], query: &[u8]) -> f32 {
    if dataset_token == query {
        return -2.0 - query.len() as f32 * 0.75;
    }

    let cut_len = dataset_token.len().min(query.len());
    let cut = &dataset_token[..cut_len];
    let max_dist = cut_len / 2 + 2;
    let dist = sift4(cut, query, 3, max_dist);

    if dist >= cut_len {
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

/// Scores a dataset name against a single query input.
/// The dataset name is split into tokens and all contiguous sub-phrases
/// (up to 4 tokens) are checked. Returns the best score, or `NO_MATCH`.
pub fn get_match_score(dataset_name: &[u8], query: &[u8]) -> f32 {
    if dataset_name.is_empty() || query.is_empty() {
        return NO_MATCH;
    }

    let mut s_tokens = Vec::new();
    tokenize_bytes(dataset_name, &mut s_tokens);

    let fallback = get_token_match_score(dataset_name, query);

    if s_tokens.len() <= 1 {
        return fallback;
    }

    let max_tokens = s_tokens.len().min(8);
    let s_tokens = &s_tokens[..max_tokens];

    let mut best_score = NO_MATCH;
    let mut best_token_bits: u8 = 0;

    // Try all contiguous sub-phrases up to length 4
    for from in 0..s_tokens.len() {
        for len in 1..=4.min(s_tokens.len() - from) {
            let to = from + len;

            let token_bits: u8 = ((1u16 << to) - (1u16 << from)) as u8;

            // Build phrase by concatenating tokens with spaces
            let phrase: Vec<u8> = if len == 1 {
                s_tokens[from].to_vec()
            } else {
                let total_len: usize =
                    s_tokens[from..to].iter().map(|t| t.len()).sum::<usize>() + len - 1;
                let mut buf = Vec::with_capacity(total_len);
                for (i, t) in s_tokens[from..to].iter().enumerate() {
                    if i > 0 {
                        buf.push(b' ');
                    }
                    buf.extend_from_slice(t);
                }
                buf
            };

            let score = get_token_match_score(&phrase, query);
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
    for (s_idx, token) in s_tokens.iter().enumerate() {
        if (best_token_bits & (1 << s_idx)) == 0 {
            n_not_matched += 1;
            let penalty = (token.len() as f32 / 4.0).clamp(0.75, 3.0);
            sum += penalty;
        }
    }

    if n_not_matched as usize == s_tokens.len() {
        return NO_MATCH;
    }

    let max = (dataset_name.len().min(query.len()) as f32 / 2.0).ceil();
    let score = fallback.min(sum);
    if score >= max {
        NO_MATCH
    } else {
        score
    }
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
    fn exact_match_best_score() {
        let score = get_match_score(b"paris", b"paris");
        assert!(score < -4.0);
        assert_ne!(score, NO_MATCH);
    }

    #[test]
    fn prefix_match_good_score() {
        let score = get_match_score(b"paris", b"par");
        assert_ne!(score, NO_MATCH);
        assert!(score < 3.0);
    }

    #[test]
    fn no_match_unrelated() {
        let score = get_match_score(b"london", b"xyz");
        assert_eq!(score, NO_MATCH);
    }

    #[test]
    fn multi_token_match() {
        let score = get_match_score(b"new york city", b"new york");
        assert_ne!(score, NO_MATCH);
        assert!(score < 2.0);
    }

    #[test]
    fn typo_tolerance() {
        let score = get_match_score(b"paris", b"pars");
        assert_ne!(score, NO_MATCH);
    }

    #[test]
    fn admin_place_bonus() {
        assert!(get_layer_score_bonus(3) > get_layer_score_bonus(0));
        assert!(get_layer_score_bonus(9) > get_layer_score_bonus(1));
    }
}
