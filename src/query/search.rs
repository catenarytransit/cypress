use anyhow::Result;
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Map, Streamer};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use tracing::{debug, warn};

use cypress::models::memdb::CypressMemDb;
use cypress::models::normalized::NormalizedPlace;
use cypress::models::place::Layer;
use cypress::models::sift4::Sift4CountGuard;
use cypress::models::AdminEntry;
use cypress::scylla::ScyllaClient;

use super::cpu::SearchCpu;
use super::field_aware::{self, JoinIndex};
use super::memdb::{
    AreaPhraseScoreCache, CosSimMatch, Memdb, QueryDbView, GUESS_CONTEXT, MAX_QUERY_PHRASES,
};
use super::token_retrieval;

#[derive(Clone)]
pub struct SearchParams {
    pub text: String,
    pub lang: Option<String>,
    pub bbox: Option<[f64; 4]>,
    pub focus_lat: Option<f64>,
    pub focus_lon: Option<f64>,
    pub focus_weight: Option<f64>,
    pub layers: Option<Vec<String>>,
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(rename = "type")]
    pub result_type: String,
    pub geometry: Geometry,
    pub properties: Properties,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultV2 {
    #[serde(rename = "type")]
    pub result_type: String,
    pub geometry: Geometry,
    pub properties: PropertiesV2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Geometry {
    #[serde(rename = "type")]
    pub geo_type: String,
    pub coordinates: [f64; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Properties {
    pub id: String,
    pub layer: String,
    pub name: String,
    pub names: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub housenumber: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    pub confidence: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PropertiesV2 {
    pub id: String,
    pub layer: String,
    pub name: String,
    pub names: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub housenumber: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    pub confidence: f64,
}

pub struct TimedSearchResults {
    pub results: Vec<SearchResult>,
    pub took_ms: u128,
}

pub struct TimedSearchResultsV2 {
    pub results: Vec<SearchResultV2>,
    pub took_ms: u128,
}

const COS_SIM_CUTOFF: f32 = 0.17;
const MAX_STRING_MATCHES: usize = 6000;
const MAX_SCORED_MATCHES: usize = 10000;
const FOCUS_RESCORE_MULTIPLIER: usize = 5;
const FOCUS_RESCORE_FLOOR: usize = 50;
const SLOW_PHASE_WARN_MS: u128 = 250;
const SLOW_SEARCH_WARN_MS: u128 = 1000;

// All experiments are opt-in and read once at process startup. Run two query
// processes on identical compiled indexes for a controlled A/B comparison.
fn experiment_enabled(name: &'static str) -> bool {
    match std::env::var(name).as_deref() {
        Ok("1" | "true" | "on" | "yes") => true,
        Ok("0" | "false" | "off" | "no") | Err(_) => false,
        Ok(value) => {
            warn!(target: "cypress::query::timing", name, value,
                "invalid experimental boolean flag; using false");
            false
        }
    }
}

static PREPARED_AREA_SCORING: OnceLock<bool> = OnceLock::new();
static CACHE_STREET_COMPONENTS: OnceLock<bool> = OnceLock::new();

fn prepared_area_scoring() -> bool {
    *PREPARED_AREA_SCORING.get_or_init(|| experiment_enabled("CYPRESS_PREPARED_AREA_SCORING"))
}

fn cache_street_components() -> bool {
    *CACHE_STREET_COMPONENTS.get_or_init(|| experiment_enabled("CYPRESS_CACHE_STREET_COMPONENTS"))
}

/// Runtime A/B selector. Defaults to ADR; set CYPRESS_COSINE_MODE=cypress
/// before starting the query process to restore the original cosine behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CosineMode {
    Adr,
    Cypress,
}

impl CosineMode {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "adr" => Some(Self::Adr),
            "cypress" => Some(Self::Cypress),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Adr => "adr",
            Self::Cypress => "cypress",
        }
    }
}

static COSINE_MODE: std::sync::OnceLock<CosineMode> = std::sync::OnceLock::new();
fn active_cosine_mode() -> CosineMode {
    *COSINE_MODE.get_or_init(|| match std::env::var("CYPRESS_COSINE_MODE") {
        Ok(value) => CosineMode::parse(&value).unwrap_or_else(|| {
            warn!(
                target: "cypress::query::timing",
                value = %value,
                "invalid CYPRESS_COSINE_MODE; using default 'adr' (use 'cypress' to roll back)"
            );
            CosineMode::Adr
        }),
        Err(std::env::VarError::NotPresent) => CosineMode::Adr,
        Err(error) => {
            warn!(
                target: "cypress::query::timing",
                %error,
                "could not read CYPRESS_COSINE_MODE; using default 'adr'"
            );
            CosineMode::Adr
        }
    })
}

/// Prefix candidates are additive, but bounded mode refuses broad FST
/// subtrees rather than arbitrarily taking their lexicographically first IDs.
const EXTRA_PREFIX_STRING_BUDGET: usize = 2048;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrefixMode {
    Bounded,
    Legacy,
}

impl PrefixMode {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "bounded" => Some(Self::Bounded),
            "legacy" => Some(Self::Legacy),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Bounded => "bounded",
            Self::Legacy => "legacy",
        }
    }
}

/// Experimental mode. In token mode, conjunction is NOT equivalent to
/// legacy fuzzy/global multi-field retrieval; explicitly opt in to compare.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenRetrievalMode {
    Legacy,
    Shadow,
    Token,
}

fn token_retrieval_mode() -> TokenRetrievalMode {
    static MODE: OnceLock<TokenRetrievalMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        match std::env::var("CYPRESS_TOKEN_RETRIEVAL_MODE")
            .unwrap_or_else(|_| "legacy".to_string())
            .as_str()
        {
            "shadow" => TokenRetrievalMode::Shadow,
            "token" => TokenRetrievalMode::Token,
            "legacy" => TokenRetrievalMode::Legacy,
            other => {
                warn!(target: "cypress::query::timing", %other,
                      "unknown token retrieval mode; using legacy");
                TokenRetrievalMode::Legacy
            }
        }
    })
}

static PREFIX_MODE: std::sync::OnceLock<PrefixMode> = std::sync::OnceLock::new();

fn active_prefix_mode() -> PrefixMode {
    *PREFIX_MODE.get_or_init(|| match std::env::var("CYPRESS_PREFIX_MODE") {
        Ok(value) => PrefixMode::parse(&value).unwrap_or_else(|| {
            warn!(target: "cypress::query::timing", %value,
                "invalid CYPRESS_PREFIX_MODE; using bounded (legacy rolls back)");
            PrefixMode::Bounded
        }),
        Err(_) => PrefixMode::Bounded,
    })
}

// Lifetime process counters allow aggregate hit rates across thread-local
// query caches. The counters are updated once per query, not in hot loops.
static NGRAM_QUERIES: AtomicU64 = AtomicU64::new(0);
static NGRAM_EXACT_HITS: AtomicU64 = AtomicU64::new(0);
static NGRAM_SUBSET_HITS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct SearchWorkStats {
    enabled: bool,
    first_pass_scorer_calls: u64,
    name_mapping_entries_examined: u64,
    street_mapping_entries_examined: u64,
    first_pass_strings_scored: u64,
    local_street_scorer_calls: u64,
    local_house_scorer_calls: u64,
    local_postcode_scorer_calls: u64,
    dp_candidates: u64,
    dp_fallbacks: u64,
    dp_component_steps: u64,
    dp_reachable_states: u64,
    dp_option_checks: u64,
    dp_legal_transitions: u64,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct ColdPostingCost {
    entries: u64,
    largest_list_len: u64,
    largest_gram: Option<u16>,
}

// A logical full-scan baseline: reads only gram posting offsets, never bodies.
fn cold_posting_cost(offsets: &[u32], grams: &[u16]) -> ColdPostingCost {
    let mut cost = ColdPostingCost::default();
    for &gram in grams {
        let i = gram as usize;
        if let Some((&start, &end)) = offsets.get(i).zip(offsets.get(i + 1)) {
            let entries = end.saturating_sub(start) as u64;
            cost.entries = cost.entries.saturating_add(entries);
            if entries > cost.largest_list_len {
                cost.largest_list_len = entries;
                cost.largest_gram = Some(gram);
            }
        }
    }
    cost
}

fn log_phase_timing(op: &str, phase: &str, phase_started: Instant) {
    let elapsed_ms = phase_started.elapsed().as_millis();
    debug!(target: "cypress::query::timing", op, phase, elapsed_ms, "phase complete");
    if elapsed_ms >= SLOW_PHASE_WARN_MS {
        warn!(target: "cypress::query::timing", op, phase, elapsed_ms, "slow phase");
    }
}

fn log_total_timing(op: &str, total_started: Instant, result_count: usize) {
    let elapsed_ms = total_started.elapsed().as_millis();
    debug!(target: "cypress::query::timing", op, elapsed_ms, result_count, "operation complete");
    if elapsed_ms >= SLOW_SEARCH_WARN_MS {
        warn!(target: "cypress::query::timing", op, elapsed_ms, result_count, "slow operation");
    }
}

/// ADR-aligned little-endian bigram encoding: char[0] in low byte, char[1] in high byte.
fn collect_query_bigrams(query: &str, out: &mut Vec<u16>) {
    out.clear();
    out.extend(
        query
            .as_bytes()
            .windows(2)
            .filter(|w| w[0] != b' ' && w[1] != b' ')
            .map(|w| (w[0] as u16) | ((w[1] as u16) << 8)),
    );
    out.sort_unstable();
    out.dedup();
}

/// ADR's split_ngrams() takes the first 128 non-space bigram occurrences.
/// Its cached postings, however, are keyed by *distinct* bigrams. This helper
/// reproduces both rules without changing Cypress's original fallback mode.
fn collect_adr_query_bigrams(query: &str, out: &mut Vec<u16>) {
    out.clear();
    for pair in query.as_bytes().windows(2) {
        if pair[0] == b' ' || pair[1] == b' ' {
            continue;
        }
        if out.len() == 128 {
            break;
        }
        out.push((pair[0] as u16) | ((pair[1] as u16) << 8));
    }
    out.sort_unstable();
    out.dedup();
}

/// ADR's split_ngrams() counts bigram *occurrences*, even when an
/// occurrence repeats. Its posting-list accumulator, by contrast, visits
/// each distinct bigram only once. Unlike Cypress's unique query gram count,
/// this value is used for both ADR's minimum count and cosine denominator.
fn adr_query_bigram_occurrences(query: &str) -> usize {
    query
        .as_bytes()
        .windows(2)
        .filter(|w| w[0] != b' ' && w[1] != b' ')
        .count()
}

/// ADR's n_bigrams_[string] is min(255, normalized_byte_len - 1), not
/// the count of unique bigrams in that string. ADR scoring uses
/// the existing Cypress-normalized stored string length; differences between
/// Cypress and ADR UTF-8 normalization remain a separate parity question.
fn adr_cosine_score(
    matching_unique_bigrams: u8,
    stored_string_byte_len: usize,
    query_bigram_occurrences: usize,
) -> Option<f32> {
    let string_denominator = stored_string_byte_len
        .saturating_sub(1)
        .min(u8::MAX as usize);
    if string_denominator == 0 || query_bigram_occurrences == 0 {
        return None;
    }
    let shared = matching_unique_bigrams as f32;
    Some(shared * shared / (string_denominator * query_bigram_occurrences) as f32)
}

/// Tokenize a query string into words.
fn tokenize_query(query: &str) -> Vec<&[u8]> {
    query
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .filter(|s| !s.is_empty())
        .map(|s| s.as_bytes())
        .collect()
}

struct QueryPhrase {
    bytes: Vec<u8>,
    mask: u8,
    // Sorted by area ID. The score is the best score among multilingual
    // admin aliases whose key starts with this query phrase.
    admin_prefix_scores: Vec<(u32, f32)>,
}

#[derive(Clone, Copy, Debug)]
struct ComponentMatchOption {
    mask: u8,
    cost: f32,
}

#[derive(Clone, Copy, Debug)]
struct NameEvidence {
    place_id: u32,
    mask: u8,
    cost: f32,
}

#[derive(Clone, Copy, Debug)]
struct StreetEvidence {
    street_group_id: u32,
    mask: u8,
    cost: f32,
}

fn collect_exact_phrase_string_ids(
    prefix_fst: &Map<&[u8]>,
    phrases: &[QueryPhrase],
    out: &mut Vec<u32>,
) {
    for phrase in phrases {
        if let Some(string_id) = prefix_fst.get(phrase.bytes.as_slice()) {
            if string_id <= u32::MAX as u64 {
                out.push(string_id as u32);
            }
        }
    }
}

fn collect_component_match_options(
    prepared: &cypress::models::scoring::PreparedMatchCandidate<'_>,
    phrases: &[QueryPhrase],
    offset_arr: &mut Vec<cypress::models::sift4::SiftOffset>,
    out: &mut Vec<ComponentMatchOption>,
) {
    out.clear();
    for phrase in phrases {
        let score = prepared.score(&phrase.bytes, offset_arr);
        if score != cypress::models::scoring::NO_MATCH {
            out.push(ComponentMatchOption {
                mask: phrase.mask,
                cost: score,
            });
        }
    }

    // For one semantic component, two options consuming the same query tokens
    // are interchangeable; retaining only the cheapest one is lossless.
    out.sort_unstable_by(|a, b| {
        a.mask.cmp(&b.mask).then(
            a.cost
                .partial_cmp(&b.cost)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    out.dedup_by(|a, b| a.mask == b.mask);
}

fn is_house_number_phrase(phrase: &QueryPhrase) -> bool {
    phrase.mask.count_ones() <= 2
        && phrase.bytes.len() <= 24
        && phrase.bytes.iter().any(|b| b.is_ascii_digit())
}

fn query_token_mask(token_count: usize) -> u8 {
    match token_count.min(8) {
        0 => 0,
        8 => u8::MAX,
        n => (1u8 << n) - 1,
    }
}

fn get_house_number<'a>(db: &'a QueryDbView<'_>, house_idx: usize) -> Option<&'a [u8]> {
    let (start, end) = db.house_number_range(house_idx)?;
    if start > end || end > db.house_number_bytes.len() {
        return None;
    }
    Some(&db.house_number_bytes[start..end])
}

fn get_house_postcode<'a>(db: &'a QueryDbView<'_>, house_idx: usize) -> Option<&'a [u8]> {
    let (start, end) = db.house_postcode_range(house_idx)?;
    if start > end || end > db.house_postcode_bytes.len() {
        return None;
    }
    Some(&db.house_postcode_bytes[start..end])
}

fn get_street_name<'a>(db: &'a QueryDbView<'_>, street_group_idx: usize) -> Option<&'a [u8]> {
    let string_id = *db.street_name_string_ids.get(street_group_idx)? as usize;
    let start = *db.string_name_offsets.get(string_id)? as usize;
    let end = *db.string_name_offsets.get(string_id + 1)? as usize;
    if start > end || end > db.string_name_bytes.len() {
        return None;
    }
    Some(&db.string_name_bytes[start..end])
}

/// Exact house lookup inside a geographically scoped street group.
/// Houses are compiler-sorted by normalized house number, so this is
/// O(log H_s + R), where H_s is houses in the street group and R is the number
/// of records sharing that exact house number.
fn find_exact_house_range(
    db: &QueryDbView<'_>,
    street_group_idx: usize,
    target: &[u8],
) -> Option<(usize, usize)> {
    let start = *db.street_house_offsets.get(street_group_idx)? as usize;
    let end = *db.street_house_offsets.get(street_group_idx + 1)? as usize;
    if start > end || end > db.house_place_ids.len() {
        return None;
    }

    let mut lo = start;
    let mut hi = end;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let value = get_house_number(db, mid)?;
        if value < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let first = lo;
    if first >= end || get_house_number(db, first)? != target {
        return None;
    }

    lo = first;
    hi = end;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let value = get_house_number(db, mid)?;
        if value <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Some((first, lo))
}

fn place_prior_bonus(db: &QueryDbView<'_>, place_idx: usize) -> f32 {
    let layer_rank = db.place_layer_ranks[place_idx];
    let population = db.place_populations[place_idx] as f32;
    let layer_bonus = cypress::models::scoring::get_layer_score_bonus(layer_rank);
    let importance_bonus = if layer_rank == 0 {
        (population / 2_000.0).clamp(1.2, 5.0)
    } else {
        (population / 200_000.0).clamp(0.0, 3.0)
    };
    layer_bonus + importance_bonus + 5.0
}

/// One exact subset-DP step for a semantic component.
/// `dp[mask]` is the minimum lexical/structural cost after all previously
/// processed components while consuming exactly `mask` query tokens. Every
/// legal assignment either skips this component or selects one non-overlapping
/// option, so induction over components proves the recurrence retains the
/// globally minimum-cost assignment. For T <= 8 query tokens the state space
/// is at most 2^T = 256; runtime is O(C * 2^T * P) for C components and at
/// most P retained phrase options per component, with O(2^T) scratch memory.
fn advance_component_dp(
    dp: &mut [f32; 256],
    options: &[ComponentMatchOption],
    all_mask: u8,
    work: &mut SearchWorkStats,
) {
    if options.is_empty() {
        return;
    }
    if work.enabled {
        work.dp_component_steps += 1;
    }

    let previous = *dp;
    let mut next = previous; // Skipping a component is always legal.
    for mask in 0..=all_mask as usize {
        let base = previous[mask];
        if !base.is_finite() {
            continue;
        }
        if work.enabled {
            work.dp_reachable_states += 1;
            work.dp_option_checks += options.len() as u64;
        }
        for option in options {
            if (mask as u8 & option.mask) != 0 {
                continue;
            }
            if work.enabled {
                work.dp_legal_transitions += 1;
            }
            let new_mask = mask | option.mask as usize;
            next[new_mask] = next[new_mask].min(base + option.cost);
        }
    }
    *dp = next;
}

fn build_area_component_options(
    db: &QueryDbView<'_>,
    area_set_idx: u32,
    phrases: &[QueryPhrase],
    query_epoch: u32,
    area_phrase_scores: &mut AreaPhraseScoreCache,
    offset_arr: &mut Vec<cypress::models::sift4::SiftOffset>,
) -> Vec<Vec<ComponentMatchOption>> {
    if area_set_idx == u32::MAX {
        return Vec::new();
    }
    let set_idx = area_set_idx as usize;
    if set_idx + 1 >= db.area_set_offsets.len() {
        return Vec::new();
    }

    let start = db.area_set_offsets[set_idx] as usize;
    let end = db.area_set_offsets[set_idx + 1] as usize;
    if start > end || end > db.area_set_data.len() {
        return Vec::new();
    }

    let mut components = Vec::new();
    for pos in start..end {
        let area_idx = db.area_set_data[pos] as usize;
        let Some(scores) = cached_area_phrase_scores(
            db,
            area_idx,
            phrases,
            query_epoch,
            area_phrase_scores,
            offset_arr,
        ) else {
            continue;
        };
        let area_pop = db.area_populations.get(area_idx).copied().unwrap_or(0) as f32;
        let area_bonus = 2.0 + (area_pop / 10_000_000.0) * 2.0;

        let mut options = Vec::new();
        for (phrase_idx, phrase) in phrases.iter().enumerate() {
            let score = scores[phrase_idx];
            if score != cypress::models::scoring::NO_MATCH {
                options.push(ComponentMatchOption {
                    mask: phrase.mask,
                    cost: score - area_bonus,
                });
            }
        }

        options.sort_unstable_by(|a, b| {
            a.mask.cmp(&b.mask).then(
                a.cost
                    .partial_cmp(&b.cost)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
        });
        options.dedup_by(|a, b| a.mask == b.mask);
        if !options.is_empty() {
            components.push(options);
        }
    }
    components
}

fn score_structured_candidate(
    db: &QueryDbView<'_>,
    place_idx: usize,
    phrases: &[QueryPhrase],
    query_tokens: &[&[u8]],
    name_evidence: &[NameEvidence],
    area_components: &[Vec<ComponentMatchOption>],
    scratch: &mut Vec<ComponentMatchOption>,
    offset_arr: &mut Vec<cypress::models::sift4::SiftOffset>,
    street_option_cache: &mut HashMap<u32, Vec<ComponentMatchOption>>,
    work: &mut SearchWorkStats,
) -> Option<f32> {
    let token_count = query_tokens.len().min(8);
    if token_count == 0 {
        return None;
    }
    let all_mask = query_token_mask(token_count);

    let mut dp = [f32::INFINITY; 256];
    dp[0] = 0.0;

    scratch.clear();
    scratch.extend(name_evidence.iter().map(|e| ComponentMatchOption {
        mask: e.mask,
        cost: e.cost,
    }));
    advance_component_dp(&mut dp, scratch, all_mask, work);

    let house_idx = db
        .place_address_house_indices
        .get(place_idx)
        .copied()
        .filter(|&idx| idx != u32::MAX)
        .map(|idx| idx as usize);

    if let Some(house_idx) = house_idx {
        if let Some(street_group_idx) = db
            .house_street_groups
            .get(house_idx)
            .copied()
            .map(|idx| idx as usize)
        {
            if let Some(street_name) = get_street_name(db, street_group_idx) {
                if cache_street_components() {
                    if let Some(&name_id) = db.street_name_string_ids.get(street_group_idx) {
                        if !street_option_cache.contains_key(&name_id) {
                            let prepared =
                                cypress::models::scoring::PreparedMatchCandidate::new(street_name);
                            if work.enabled {
                                work.local_street_scorer_calls += phrases.len() as u64;
                            }
                            collect_component_match_options(
                                &prepared, phrases, offset_arr, scratch,
                            );
                            street_option_cache.insert(name_id, scratch.clone());
                        }
                        if let Some(options) = street_option_cache.get(&name_id) {
                            advance_component_dp(&mut dp, options, all_mask, work);
                        }
                    }
                } else {
                    let prepared =
                        cypress::models::scoring::PreparedMatchCandidate::new(street_name);
                    if work.enabled {
                        work.local_street_scorer_calls += phrases.len() as u64;
                    }
                    collect_component_match_options(&prepared, phrases, offset_arr, scratch);
                    advance_component_dp(&mut dp, scratch, all_mask, work);
                }
            }
        }

        if let Some(house_number) = get_house_number(db, house_idx) {
            let prepared = cypress::models::scoring::PreparedMatchCandidate::new(house_number);
            scratch.clear();
            for phrase in phrases.iter().filter(|p| is_house_number_phrase(p)) {
                if work.enabled {
                    work.local_house_scorer_calls += 1;
                }
                let score = prepared.score(&phrase.bytes, offset_arr);
                if score != cypress::models::scoring::NO_MATCH {
                    scratch.push(ComponentMatchOption {
                        mask: phrase.mask,
                        cost: score - 2.0,
                    });
                }
            }
            advance_component_dp(&mut dp, scratch, all_mask, work);
        }

        if let Some(postcode) = get_house_postcode(db, house_idx).filter(|p| !p.is_empty()) {
            let prepared = cypress::models::scoring::PreparedMatchCandidate::new(postcode);
            if work.enabled {
                work.local_postcode_scorer_calls += phrases.len() as u64;
            }
            collect_component_match_options(&prepared, phrases, offset_arr, scratch);
            advance_component_dp(&mut dp, scratch, all_mask, work);
        }
    }

    for options in area_components {
        advance_component_dp(&mut dp, options, all_mask, work);
    }

    let mut best = f32::INFINITY;
    for mask in 1..=all_mask as usize {
        let mut cost = dp[mask];
        if !cost.is_finite() {
            continue;
        }
        for (token_idx, token) in query_tokens.iter().enumerate().take(8) {
            if (mask as u8 & (1 << token_idx)) == 0 {
                cost += token.len() as f32 * 3.0;
            }
        }
        best = best.min(cost);
    }

    best.is_finite().then_some(best)
}

fn load_fst_map(bytes: &[u8]) -> Option<Map<&[u8]>> {
    if bytes.is_empty() {
        None
    } else {
        Map::new(bytes).ok()
    }
}

fn collect_prefix_string_ids(prefix_fst: &Map<&[u8]>, prefix: &str, out: &mut Vec<u32>) {
    if prefix.chars().count() < 2 {
        return;
    }
    let automaton = Str::new(prefix).starts_with();
    let mut stream = prefix_fst.search(automaton).into_stream();
    while let Some((_key, string_id)) = stream.next() {
        if string_id <= u32::MAX as u64 {
            out.push(string_id as u32);
        }
    }
}

/// Return None if the prefix matches more than `limit` strings. In that case
/// append *nothing*: the first strings returned by an FST are lexicographically
/// ordered, not relevance-ranked. Scanning at most limit+1 entries bounds work.
fn probe_bounded_prefix(
    prefix_fst: &Map<&[u8]>,
    prefix: &str,
    limit: usize,
    out: &mut Vec<u32>,
) -> Option<usize> {
    if prefix.chars().count() < 2 || limit == 0 {
        return Some(0);
    }
    let automaton = Str::new(prefix).starts_with();
    let mut stream = prefix_fst.search(automaton).into_stream();
    let mut matches = Vec::new();
    while let Some((_key, string_id)) = stream.next() {
        if string_id > u32::MAX as u64 {
            continue;
        }
        if matches.len() == limit {
            return None;
        }
        matches.push(string_id as u32);
    }
    let accepted = matches.len();
    out.extend(matches);
    Some(accepted)
}

#[derive(Default)]
struct PrefixPlanStats {
    full_query_raw: usize,
    multiword_prefix_raw: usize,
    token_prefix_raw: Vec<usize>,
    prefixes_probed: usize,
    broad_prefixes_skipped: usize,
    budget_used: usize,
}

/// Longest multiword anchors first; single tokens last. Exact phrase IDs are
/// already present in `out` and do not consume the budget. This does not assume
/// which words are a city or street, so all token roles remain eligible.
fn collect_bounded_prefix_candidates(
    prefix_fst: &Map<&[u8]>,
    query: &str,
    phrases: &[QueryPhrase],
    query_tokens: &[&[u8]],
    out: &mut Vec<u32>,
) -> PrefixPlanStats {
    let mut stats = PrefixPlanStats::default();
    let mut remaining = EXTRA_PREFIX_STRING_BUDGET;

    stats.prefixes_probed += 1;
    match probe_bounded_prefix(prefix_fst, query, remaining, out) {
        Some(count) => {
            stats.full_query_raw = count;
            remaining -= count;
        }
        None => stats.broad_prefixes_skipped += 1,
    }

    let mut multiword: Vec<&QueryPhrase> = phrases
        .iter()
        .filter(|p| p.mask.count_ones() > 1 && p.bytes.as_slice() != query.as_bytes())
        .collect();
    multiword.sort_unstable_by(|a, b| {
        b.mask
            .count_ones()
            .cmp(&a.mask.count_ones())
            .then(b.bytes.len().cmp(&a.bytes.len()))
    });
    for phrase in multiword {
        if remaining == 0 {
            break;
        }
        if let Ok(prefix) = std::str::from_utf8(&phrase.bytes) {
            stats.prefixes_probed += 1;
            match probe_bounded_prefix(prefix_fst, prefix, remaining, out) {
                Some(count) => {
                    stats.multiword_prefix_raw += count;
                    remaining -= count;
                }
                None => stats.broad_prefixes_skipped += 1,
            }
        }
    }

    for token in query_tokens.iter().take(8) {
        let mut count = 0;
        if remaining > 0 {
            if let Ok(prefix) = std::str::from_utf8(token) {
                stats.prefixes_probed += 1;
                match probe_bounded_prefix(prefix_fst, prefix, remaining, out) {
                    Some(n) => {
                        count = n;
                        remaining -= n;
                    }
                    None => stats.broad_prefixes_skipped += 1,
                }
            }
        }
        stats.token_prefix_raw.push(count);
    }
    stats.budget_used = EXTRA_PREFIX_STRING_BUDGET - remaining;
    stats
}

fn resolve_admin_prefix_scores(
    db: &QueryDbView<'_>,
    admin_alias_fst: Option<&Map<&[u8]>>,
    phrase: &[u8],
) -> Vec<(u32, f32)> {
    let Some(admin_alias_fst) = admin_alias_fst else {
        return Vec::new();
    };
    let Ok(prefix) = std::str::from_utf8(phrase) else {
        return Vec::new();
    };
    if prefix.chars().count() < 2 {
        return Vec::new();
    }

    let automaton = Str::new(prefix).starts_with();
    let mut stream = admin_alias_fst.search(automaton).into_stream();
    let mut area_scores = Vec::<(u32, f32)>::new();

    while let Some((alias, group_idx)) = stream.next() {
        let group_idx = group_idx as usize;
        if group_idx + 1 >= db.admin_alias_area_offsets.len() {
            continue;
        }
        let start = db.admin_alias_area_offsets[group_idx] as usize;
        let end = db.admin_alias_area_offsets[group_idx + 1] as usize;
        if start > end || end > db.admin_alias_area_data.len() {
            continue;
        }

        let mut offsets = Vec::new();
        let score = cypress::models::scoring::get_match_score(alias, phrase, &mut offsets);
        if score == cypress::models::scoring::NO_MATCH {
            continue;
        }
        for i in start..end {
            area_scores.push((db.admin_alias_area_data[i], score));
        }
    }

    area_scores.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    let mut best_by_area = Vec::<(u32, f32)>::with_capacity(area_scores.len());
    for (area_id, score) in area_scores {
        if best_by_area.last().map(|last| last.0) == Some(area_id) {
            continue; // sorted by score within each area, so the first is best
        }
        best_by_area.push((area_id, score));
    }
    best_by_area
}

/// Evaluate all phrase variants for an administrative area only once per
/// query, as in ADR's activate_areas(). First-pass matching, address seeding,
/// and exact final reranking must use the identical cached lexical scores.
fn cached_area_phrase_scores<'a>(
    db: &QueryDbView<'_>,
    area_idx: usize,
    phrases: &[QueryPhrase],
    query_epoch: u32,
    cache: &'a mut AreaPhraseScoreCache,
    offset_arr: &mut Vec<cypress::models::sift4::SiftOffset>,
) -> Option<&'a [f32; MAX_QUERY_PHRASES]> {
    debug_assert!(phrases.len() <= MAX_QUERY_PHRASES);
    let name_start = *db.area_name_offsets.get(area_idx)? as usize;
    let name_end = *db.area_name_offsets.get(area_idx.checked_add(1)?)? as usize;
    let area_name = db.area_name_bytes.get(name_start..name_end)?;

    cache.get_or_compute(area_idx, query_epoch, |scores| {
        // Reuse token boundaries for all phrases on a cache miss. This
        // preserves get_match_score's exact existing scoring semantics.
        let prepared = prepared_area_scoring()
            .then(|| cypress::models::scoring::PreparedMatchCandidate::new(area_name));
        for (phrase_idx, phrase) in phrases.iter().enumerate() {
            // This preserves the existing alias-prefix preference exactly.
            scores[phrase_idx] = match phrase
                .admin_prefix_scores
                .binary_search_by_key(&(area_idx as u32), |(id, _)| *id)
            {
                Ok(pos) => phrase.admin_prefix_scores[pos].1,
                Err(_) => match prepared.as_ref() {
                    Some(candidate) => candidate.score(&phrase.bytes, offset_arr),
                    None => cypress::models::scoring::get_area_match_score(
                        area_name,
                        &phrase.bytes,
                        offset_arr,
                    ),
                },
            };
        }
    })
}

/// Try to match unmatched query tokens against area names for a place.
fn score_area_matches(
    db: &QueryDbView<'_>,
    area_set_idx: usize,
    phrases: &[QueryPhrase],
    matched_mask: u8,
    query_epoch: u32,
    area_phrase_scores: &mut AreaPhraseScoreCache,
    offset_arr: &mut Vec<cypress::models::sift4::SiftOffset>,
) -> (f32, u8) {
    if area_set_idx as u32 == u32::MAX {
        return (0.0, matched_mask);
    }

    let as_start = db.area_set_offsets[area_set_idx] as usize;
    let as_end = if area_set_idx + 1 < db.area_set_offsets.len() {
        db.area_set_offsets[area_set_idx + 1] as usize
    } else {
        db.area_set_data.len()
    };

    let mut current_mask = matched_mask;
    let mut area_score = 0.0f32;
    let mut areas_matched = 0u32;

    for ai in as_start..as_end {
        let area_idx = db.area_set_data[ai] as usize;
        let Some(scores) = cached_area_phrase_scores(
            db,
            area_idx,
            phrases,
            query_epoch,
            area_phrase_scores,
            offset_arr,
        ) else {
            continue;
        };
        let mut best_phrase_score = cypress::models::scoring::NO_MATCH;
        let mut best_phrase_mask = 0u8;

        for (phrase_idx, phrase) in phrases.iter().enumerate() {
            if (phrase.mask & current_mask) != 0 {
                continue;
            }

            let score = scores[phrase_idx];
            if score != cypress::models::scoring::NO_MATCH && score < best_phrase_score {
                best_phrase_score = score;
                best_phrase_mask = phrase.mask;
            }
        }

        if best_phrase_score != cypress::models::scoring::NO_MATCH {
            current_mask |= best_phrase_mask;
            area_score += best_phrase_score;

            let area_pop = if area_idx < db.area_populations.len() {
                db.area_populations[area_idx] as f32
            } else {
                0.0
            };
            let area_pop_bonus = (area_pop / 10_000_000.0) * 2.0;
            area_score -= area_pop_bonus;
            areas_matched += 1;
        }
    }

    let areas_bonus = areas_matched as f32 * 2.0;
    area_score -= areas_bonus;

    (area_score, current_mask)
}

pub fn search_place_ids(
    memdb: &Memdb,
    text: &str,
    focus: Option<(f64, f64)>,
    focus_weight: f64,
    bbox: Option<[f64; 4]>,
    max_results: usize,
) -> Vec<(u32, f64)> {
    let total_started = Instant::now();

    if max_results == 0 {
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            "skipping search because max_results is zero"
        );
        return Vec::new();
    }

    let query = text.trim().to_lowercase();
    if query.len() < 2 {
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            query_len = query.len(),
            "skipping search because query is too short"
        );
        return Vec::new();
    }

    debug!(
        target: "cypress::query::timing",
        op = "search_place_ids",
        query_len = query.len(),
        max_results,
        has_focus = focus.is_some(),
        has_bbox = bbox.is_some(),
        "search start"
    );

    let memdb_data = memdb.get_data();
    let db = &memdb_data.get_view();
    let string_count = db.string_bigram_counts.len();
    let place_count = db.place_latitudes.len();
    let safe_focus_weight = focus_weight.max(0.001);

    let prefix_fst = load_fst_map(&db.prefix_fst_bytes[..]);
    let admin_alias_fst = load_fst_map(&db.admin_alias_fst_bytes[..]);

    let query_tokens = tokenize_query(&query);
    let is_multi_token = query_tokens.len() > 1;

    if query_tokens.is_empty() {
        return Vec::new();
    }

    let cosine_mode = active_cosine_mode();
    let prefix_mode = active_prefix_mode();
    debug!(target: "cypress::query::timing",
        prepared_area_scoring = prepared_area_scoring(),
        cache_street_components = cache_street_components(),
        "active ranking experiments");
    debug!(
        target: "cypress::query::timing",
        cosine_mode = cosine_mode.as_str(),
        "active production cosine mode (set CYPRESS_COSINE_MODE=cypress to roll back)"
    );
    debug!(
        target: "cypress::query::timing",
        prefix_mode = prefix_mode.as_str(),
        extra_prefix_budget = EXTRA_PREFIX_STRING_BUDGET,
        "active prefix candidate retrieval mode"
    );
    let detailed_metrics =
        tracing::enabled!(target: "cypress::query::timing", tracing::Level::DEBUG);
    let sift4_guard = Sift4CountGuard::new(detailed_metrics);
    let phrases_started = Instant::now();
    let max_tokens = query_tokens.len().min(8);
    let mut phrases: Vec<QueryPhrase> = Vec::new();
    for i in 0..max_tokens {
        let mut phrase = Vec::new();
        let mut mask = 0u8;
        for j in i..max_tokens {
            if j != i {
                phrase.push(b' ');
            }
            phrase.extend_from_slice(query_tokens[j]);
            mask |= 1 << j;
            let bytes = phrase.clone();
            phrases.push(QueryPhrase {
                admin_prefix_scores: resolve_admin_prefix_scores(
                    db,
                    admin_alias_fst.as_ref(),
                    &bytes,
                ),
                bytes,
                mask,
            });
        }
    }
    let admin_alias_candidates: usize = phrases.iter().map(|p| p.admin_prefix_scores.len()).sum();
    debug!(
        target: "cypress::query::timing",
        op = "search_place_ids",
        query_tokens = max_tokens,
        query_phrases = phrases.len(),
        admin_alias_candidates,
        sift4_calls = sift4_guard.count(),
        "query phrase generation summary"
    );
    log_phase_timing(
        "search_place_ids",
        "generate_query_phrases",
        phrases_started,
    );

    // Prefix candidates are additive. They bypass the cosine candidate cap but
    // still pass through the same SIFT/place/admin/geographic ranker. This keeps
    // typo recovery in the bigram path while guaranteeing that exact prefixes
    // such as "zuri" can reach the final ranker for aliases such as "zurich".
    let prefix_phase_started = Instant::now();
    let mut prefix_string_ids = Vec::<u32>::new();
    let mut exact_phrase_matches = 0usize;
    let mut exact_multiword_phrases = 0usize;
    let mut full_query_prefix_raw = 0usize;
    let mut fallback_token_prefix_raw = Vec::<usize>::new();
    let mut bounded_stats = PrefixPlanStats::default();
    if let Some(prefix_fst) = prefix_fst.as_ref() {
        collect_exact_phrase_string_ids(prefix_fst, &phrases, &mut prefix_string_ids);
        exact_phrase_matches = prefix_string_ids.len();
        exact_multiword_phrases = phrases
            .iter()
            .filter(|phrase| phrase.mask.count_ones() > 1)
            .filter(|phrase| prefix_fst.get(&phrase.bytes).is_some())
            .count();
        match prefix_mode {
            PrefixMode::Legacy => {
                let before_full_prefix = prefix_string_ids.len();
                collect_prefix_string_ids(prefix_fst, &query, &mut prefix_string_ids);
                full_query_prefix_raw = prefix_string_ids.len() - before_full_prefix;
                if prefix_string_ids.len() == before_full_prefix && is_multi_token {
                    for token in query_tokens.iter().take(8) {
                        let before_token = prefix_string_ids.len();
                        if let Ok(token) = std::str::from_utf8(token) {
                            collect_prefix_string_ids(prefix_fst, token, &mut prefix_string_ids);
                        }
                        fallback_token_prefix_raw.push(prefix_string_ids.len() - before_token);
                    }
                }
            }
            PrefixMode::Bounded => {
                bounded_stats = collect_bounded_prefix_candidates(
                    prefix_fst,
                    &query,
                    &phrases,
                    &query_tokens,
                    &mut prefix_string_ids,
                );
                full_query_prefix_raw = bounded_stats.full_query_raw;
                fallback_token_prefix_raw = bounded_stats.token_prefix_raw.clone();
            }
        }
    }
    prefix_string_ids.sort_unstable();
    prefix_string_ids.dedup();
    log_phase_timing(
        "search_place_ids",
        "prefix_fst_lookup",
        prefix_phase_started,
    );
    debug!(
        target: "cypress::query::timing",
        op = "search_place_ids",
        prefix_candidate_count = prefix_string_ids.len(),
        prefix_mode = prefix_mode.as_str(),
        exact_phrase_matches,
        exact_multiword_phrases,
        full_query_prefix_raw,
        multiword_prefix_raw = bounded_stats.multiword_prefix_raw,
        bounded_prefix_probes = bounded_stats.prefixes_probed,
        bounded_broad_skips = bounded_stats.broad_prefixes_skipped,
        bounded_budget_used = bounded_stats.budget_used,
        fallback_token_prefix_raw = ?fallback_token_prefix_raw,
        "prefix FST candidates ready (raw buckets overlap; final count is deduplicated)"
    );

    let join_index = JoinIndex {
        string_place_offsets: &db.string_to_places_offsets[..],
        string_place_ids: db.string_to_places_data,
        place_area_sets: db.place_area_sets,
        string_street_offsets: &db.string_to_street_offsets[..],
        string_street_groups: &db.string_to_street_data[..],
        street_area_sets: &db.street_area_sets[..],
        area_set_offsets: &db.area_set_offsets[..],
        area_set_data: &db.area_set_data[..],
    };
    let mut field_join_only_ids = Vec::<u32>::new();
    let mut field_join_areas = Vec::<u32>::new();

    // PR 4: lookup uses a prevalidated, generation-bound mmap.
    // No O(token_count) validation on the request path.
    let token_mode = token_retrieval_mode();
    let mut token_candidate_ids = None::<Vec<u32>>;
    let mut token_candidate_stats = None::<token_retrieval::RetrievalStats>;
    if token_mode != TokenRetrievalMode::Legacy {
        if let Some(sidecar) = memdb_data.token_mmap.as_ref() {
            match cypress::models::token_index::TokenIndexView::view_validated(sidecar)
                .and_then(|index| token_retrieval::retrieve(&index, &query))
            {
                Ok(Some((ids, stats))) => {
                    if !ids.is_empty() && ids.len() <= MAX_STRING_MATCHES {
                        token_candidate_ids = Some(ids);
                    }
                    token_candidate_stats = Some(stats);
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(target: "cypress::query::timing", %error,
                          "token retrieval rejected; falling back to legacy");
                }
            }
        }

        if token_candidate_ids.is_none() && (2..=8).contains(&query_tokens.len()) {
            if let (Some(sidecar), Some(admin_fst)) =
                (memdb_data.token_mmap.as_ref(), admin_alias_fst.as_ref())
            {
                if let Ok(index) =
                    cypress::models::token_index::TokenIndexView::view_validated(sidecar)
                {
                    let mut joined = Vec::<u32>::new();
                    let mut areas = Vec::<u32>::new();
                    let mut examined = 0u64;
                    let mut links = 0usize;
                    let mut splits_attempted = 0usize;
                    let mut splits_matched = 0usize;
                    let mut rejected = false;
                    for (name, admin) in field_aware::suffix_splits(&query_tokens) {
                        let Some(admin_ids) = field_aware::exact_admin_areas(
                            admin_fst,
                            &db.admin_alias_area_offsets[..],
                            &db.admin_alias_area_data[..],
                            &admin,
                        ) else {
                            continue;
                        };
                        splits_attempted += 1;
                        let Ok(name) = std::str::from_utf8(&name) else {
                            continue;
                        };
                        match token_retrieval::retrieve(&index, name) {
                            Ok(Some((ids, stats))) => {
                                examined += stats.posting_entries_examined;
                                if let Some((filtered, join_stats)) =
                                    field_aware::retain_joined_strings(
                                        &join_index,
                                        &ids,
                                        &admin_ids,
                                    )
                                {
                                    links += join_stats.examined_links;
                                    if !filtered.is_empty() {
                                        splits_matched += 1;
                                        joined.extend(filtered);
                                        areas.extend(admin_ids);
                                    }
                                } else {
                                    rejected = true;
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                warn!(target: "cypress::query::timing", %error,
                                      "field-aware token retrieval rejected");
                                rejected = true;
                                break;
                            }
                        }
                    }
                    joined.sort_unstable();
                    joined.dedup();
                    areas.sort_unstable();
                    areas.dedup();
                    if !rejected && !joined.is_empty() && joined.len() <= MAX_STRING_MATCHES {
                        field_join_only_ids = joined
                            .iter()
                            .copied()
                            .filter(|sid| prefix_string_ids.binary_search(sid).is_err())
                            .collect();
                        field_join_areas = areas;
                        token_candidate_ids = Some(joined);
                    }
                    debug!(
                        target: "cypress::query::timing",
                        splits_attempted, splits_matched,
                        matching_strings = token_candidate_ids.as_ref().map_or(0, Vec::len),
                        posting_entries_examined = examined,
                        mapping_links_examined = links,
                        rejected,
                        "field-aware token retrieval plan"
                    );
                }
            }
        }
        debug!(
            target: "cypress::query::timing",
            mode = if token_mode == TokenRetrievalMode::Shadow { "shadow" } else { "token" },
            has_sidecar = memdb_data.token_mmap.is_some(),
            token_candidates = token_candidate_ids.as_ref().map_or(0, Vec::len),
            token_count = token_candidate_stats.as_ref().map_or(0, |x| x.tokens),
            posting_entries_examined = token_candidate_stats.as_ref()
                .map_or(0, |x| x.posting_entries_examined),
            skip_block_seeks = token_candidate_stats.as_ref()
                .map_or(0, |x| x.block_seeks),
            anchor_document_frequency = token_candidate_stats.as_ref()
                .map_or(0, |x| x.anchor_df),
            "token retrieval plan"
        );
    }
    let use_token_fast_path =
        token_mode == TokenRetrievalMode::Token && token_candidate_ids.is_some();

    let mut results = Vec::<(u32, f64)>::new();

    GUESS_CONTEXT.with(|cell| {
        let phase_started = Instant::now();
        let mut ctx_guard = cell.borrow_mut();
        let ctx = &mut *ctx_guard;
        ctx.clear(string_count, place_count, &memdb_data.version);

        let needed_area_sets = db.area_set_offsets.len();
        if ctx.area_match_cache.len() < needed_area_sets {
            ctx.area_match_cache
                .resize(needed_area_sets, (0.0, 0, 0, 0));
        }
        ctx.area_phrase_scores
            .begin_query(db.area_name_offsets.len().saturating_sub(1));
        let mut work = SearchWorkStats {
            enabled: detailed_metrics,
            ..Default::default()
        };

        log_phase_timing("search_place_ids", "clear_context", phase_started);

        let phase_started = Instant::now();
        // The ADR query is capped to 128 bigram occurrences before dedup.
        // Cypress rollback keeps its original unbounded unique-gram set.
        let adr_query_bigram_total = adr_query_bigram_occurrences(&query);
        let adr_query_bigram_count = adr_query_bigram_total.min(128);
        match cosine_mode {
            CosineMode::Adr => collect_adr_query_bigrams(&query, &mut ctx.query_bigrams),
            CosineMode::Cypress => collect_query_bigrams(&query, &mut ctx.query_bigrams),
        }
        let query_bigram_count = ctx.query_bigrams.len();
        log_phase_timing("search_place_ids", "collect_query_bigrams", phase_started);

        if ctx.query_bigrams.is_empty() {
            debug!(
                target: "cypress::query::timing",
                op = "search_place_ids",
                "query has no bigrams after normalization"
            );
            return;
        }

        // Token mode bypasses *all* global bigram posting visits and
        // cosine processing. In legacy/shadow mode the original branch runs
        // unchanged to make latency and recall comparisons meaningful.
        let (
            cold_postings,
            posting_lists_read,
            posting_entries_visited,
            cache_exact_hit,
            cache_subset_hit,
            cache_restored_string_ids,
            max_matches,
            fuzzy_matches_before_cap,
            cache_snapshot_entries,
            cache_snapshot_bytes,
        ) = if use_token_fast_path {
            let ids = token_candidate_ids.as_ref().unwrap();
            ctx.string_matches
                .extend(ids.iter().copied().map(|string_idx| CosSimMatch {
                    string_idx,
                    cos_sim: 0.0,
                }));
            (
                if detailed_metrics {
                    cold_posting_cost(&db.bigram_offsets[..], &ctx.query_bigrams)
                } else {
                    ColdPostingCost::default()
                },
                0usize,
                0usize,
                false,
                false,
                0usize,
                ids.len(),
                ids.len(),
                0usize,
                0usize,
            )
        } else {
            let query_bigram_count_u32 = query_bigram_count as u32;
            let cypress_min_match_count =
                (2 + query_bigram_count_u32 / (4 + query_bigram_count_u32 / 10)) as u16;
            let adr_min_match_count = if adr_query_bigram_count != 0 {
                let n = adr_query_bigram_count as u32;
                (2 + n / (4 + n / 10)) as u16
            } else {
                u16::MAX
            };
            let query_bigrams = ctx.query_bigrams.clone();
            let cold_postings = if detailed_metrics {
                cold_posting_cost(&db.bigram_offsets[..], &query_bigrams)
            } else {
                ColdPostingCost::default()
            };
            let mut missing_bigrams = Vec::new();

            let phase_started = Instant::now();
            let cached_counts = ctx
                .query_cache
                .get_closest_ref(&query_bigrams, &mut missing_bigrams);
            let cache_base_reused = cached_counts.is_some();
            if let Some(cached) = cached_counts {
                ctx.restore_sparse_counts(&cached);
            }
            let missing_bigram_count = missing_bigrams.len();
            let cache_restored_string_ids = if detailed_metrics {
                ctx.touched_string_indices.len()
            } else {
                0
            };
            let cache_exact_hit = cache_base_reused && missing_bigram_count == 0;
            let cache_subset_hit = cache_base_reused && !cache_exact_hit;
            NGRAM_QUERIES.fetch_add(1, Ordering::Relaxed);
            if cache_exact_hit {
                NGRAM_EXACT_HITS.fetch_add(1, Ordering::Relaxed);
            } else if cache_subset_hit {
                NGRAM_SUBSET_HITS.fetch_add(1, Ordering::Relaxed);
            }
            log_phase_timing("search_place_ids", "cache_lookup_restore", phase_started);
            debug!(
                target: "cypress::query::timing",
                op = "search_place_ids",
                cache_hit = cache_exact_hit,
                cache_subset_hit,
                missing_bigram_count,
                "cache lookup summary"
            );

            let phase_started = Instant::now();
            let mut posting_lists_read = 0usize;
            let mut posting_entries_visited = 0usize;
            let sidecar = memdb_data.bigram_mmap.as_ref().map(|mmap| {
                cypress::models::bigram_index::BigramIndexView::parse(mmap)
                    .expect("bigram index validated on generation load")
            });
            for missing_idx in missing_bigrams.into_iter() {
                let start = db.bigram_offsets[missing_idx as usize] as usize;
                let end = db.bigram_offsets[(missing_idx + 1) as usize] as usize;

                let bigrams = match sidecar.as_ref() {
                    Some(sidecar) => sidecar.postings(start, end),
                    None => db.bigram_data.get(start..end),
                };
                if let Some(bigrams) = bigrams {
                    if detailed_metrics {
                        posting_lists_read += 1;
                        posting_entries_visited += bigrams.len();
                    }
                    for &string_idx in bigrams {
                        let string_idx = string_idx as usize;
                        ctx.increment_string_match(string_idx);
                    }
                }
            }
            log_phase_timing("search_place_ids", "expand_missing_bigrams", phase_started);

            let phase_started = Instant::now();
            let mut cache_snapshot_entries = 0usize;
            let mut cache_snapshot_bytes = 0usize;
            let cache_snapshot_needed = !ctx.query_cache.has_exact(&query_bigrams);
            if cache_snapshot_needed {
                let snapshot = ctx.snapshot_sparse_counts();
                cache_snapshot_entries = snapshot.string_ids.len();
                cache_snapshot_bytes = snapshot.string_ids.len() * std::mem::size_of::<u32>()
                    + snapshot.counts.len() * std::mem::size_of::<u8>();
                ctx.query_cache.put(&query_bigrams, Arc::new(snapshot));
            }
            log_phase_timing("search_place_ids", "cache_store_counts", phase_started);
            debug!(
                target: "cypress::query::timing",
                op = "search_place_ids",
                cache_snapshot_needed,
                cache_snapshot_entries,
                cache_snapshot_bytes,
                "bigram snapshot admission summary"
            );

            // Evaluate only the production cosine formula. Prefix FST candidates
            // are added separately below and do not depend on this cutoff.
            let phase_started = Instant::now();
            let mut string_matches = Vec::<CosSimMatch>::new();
            for touched_idx in 0..ctx.touched_string_indices.len() {
                let i = ctx.touched_string_indices[touched_idx] as usize;
                let match_count = ctx.string_match_count(i);
                let shared = match_count as u16;
                let score = match cosine_mode {
                    CosineMode::Adr if shared >= adr_min_match_count => {
                        match (
                            db.string_name_offsets.get(i).copied(),
                            db.string_name_offsets.get(i + 1).copied(),
                        ) {
                            (Some(start), Some(end)) if end >= start => adr_cosine_score(
                                match_count,
                                (end - start) as usize,
                                adr_query_bigram_count,
                            )
                            .filter(|&similarity| similarity >= COS_SIM_CUTOFF),
                            _ => None,
                        }
                    }
                    CosineMode::Cypress if shared >= cypress_min_match_count => {
                        let stored_unique_grams = db.string_bigram_counts[i] as f32;
                        if stored_unique_grams > 0.0 {
                            let count = match_count as f32;
                            let similarity = count * count
                                / (stored_unique_grams * query_bigram_count_u32 as f32);
                            (similarity >= COS_SIM_CUTOFF).then_some(similarity)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some(cos_sim) = score {
                    string_matches.push(CosSimMatch {
                        string_idx: i as u32,
                        cos_sim,
                    });
                }
            }
            let fuzzy_matches_before_cap = string_matches.len();
            ctx.string_matches.extend(string_matches);
            log_phase_timing(
                "search_place_ids",
                "compute_cosine_candidates",
                phase_started,
            );

            let phase_started = Instant::now();
            let max_matches = MAX_STRING_MATCHES.min(ctx.string_matches.len());
            if ctx.string_matches.len() > max_matches {
                ctx.string_matches
                    .select_nth_unstable_by(max_matches, |a, b| {
                        b.cos_sim
                            .partial_cmp(&a.cos_sim)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                ctx.string_matches.truncate(max_matches);
            }
            ctx.string_matches.sort_unstable_by(|a, b| {
                b.cos_sim
                    .partial_cmp(&a.cos_sim)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            log_phase_timing(
                "search_place_ids",
                "restrict_sort_string_matches",
                phase_started,
            );

            (
                cold_postings,
                posting_lists_read,
                posting_entries_visited,
                cache_exact_hit,
                cache_subset_hit,
                cache_restored_string_ids,
                max_matches,
                fuzzy_matches_before_cap,
                cache_snapshot_entries,
                cache_snapshot_bytes,
            )
        };

        let phase_started = Instant::now();
        if !prefix_string_ids.is_empty() {
            ctx.string_matches.reserve(prefix_string_ids.len());
            for &string_idx in &prefix_string_ids {
                if (string_idx as usize) < string_count {
                    ctx.string_matches.push(CosSimMatch {
                        string_idx,
                        cos_sim: 0.0,
                    });
                }
            }
            // Prefix IDs can overlap fuzzy candidates. Cosine is no longer used
            // after this point, so deduplicate by ID before expensive scoring.
            ctx.string_matches.sort_unstable_by_key(|m| m.string_idx);
            ctx.string_matches.dedup_by_key(|m| m.string_idx);
        }
        log_phase_timing("search_place_ids", "union_prefix_candidates", phase_started);
        // Experimental ranked candidate admission. The baseline remains exact.
        // With 'shadow', record how the cheap lexical/importance shortlist
        // compares against the final ranked place IDs, without changing results.
        let shortlist_mode = super::candidate_budget::ShortlistMode::from_env(
            std::env::var("CYPRESS_CANDIDATE_SHORTLIST")
                .unwrap_or_default()
                .as_str(),
        );
        let mut shadow_shortlist = None::<Vec<u32>>;
        if shortlist_mode != super::candidate_budget::ShortlistMode::Off {
            let query_bytes = query.as_bytes();
            let mut current_ids: Vec<u32> =
                ctx.string_matches.iter().map(|m| m.string_idx).collect();
            current_ids.sort_unstable();
            current_ids.dedup();
            let budget: usize = std::env::var("CYPRESS_CANDIDATE_BUDGET")
                .ok()
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(256)
                .clamp(32, 4096);
            // A StringId from any retrieval path must have valid dictionary
            // ranges before entering an optional optimization. Ignore malformed
            // entries rather than crashing a search worker.
            current_ids.retain(|&sid| {
                let pos = sid as usize;
                let (Some(&start), Some(&end)) = (
                    db.string_name_offsets.get(pos),
                    db.string_name_offsets.get(pos + 1),
                ) else {
                    return false;
                };
                let (Some(&p_start), Some(&p_end)) = (
                    db.string_to_places_offsets.get(pos),
                    db.string_to_places_offsets.get(pos + 1),
                ) else {
                    return false;
                };
                (start as usize) <= (end as usize)
                    && (end as usize) <= db.string_name_bytes.len()
                    && (p_start as usize) <= (p_end as usize)
                    && (p_end as usize) <= db.string_to_places_data.len()
            });
            let chosen = super::candidate_budget::select(&current_ids, budget, |sid| {
                let pos = sid as usize;
                let start = db.string_name_offsets[pos] as usize;
                let end = db.string_name_offsets[pos + 1] as usize;
                let name = &db.string_name_bytes[start..end];
                let phrase_exact = phrases.iter().any(|p| p.bytes.as_slice() == name);
                let full_prefix = name.starts_with(query_bytes);
                let multiword_prefix = phrases
                    .iter()
                    .filter(|p| p.mask.count_ones() > 1)
                    .any(|p| name.starts_with(&p.bytes));
                let begin = db.string_to_places_offsets[pos] as usize;
                let finish = db.string_to_places_offsets[pos + 1] as usize;
                // Do not dereference db's archived Vec directly: the latest
                // compiler keeps place_importances in cypress_dense.bin.
                let importance = super::candidate_budget::best_mapped_importance(
                    &db.string_to_places_data[begin..finish],
                    db.place_importances,
                    place_count,
                    |p| place_prior_bonus(db, p),
                );
                super::candidate_budget::Quality {
                    full_exact: name == query_bytes,
                    phrase_exact,
                    multiword_prefix,
                    full_prefix,
                    importance,
                    bytes: name.len(),
                }
            });
            debug!(
                target: "cypress::query::timing",
                mode = ?shortlist_mode,
                budget,
                before = chosen.candidates_before,
                retained = chosen.ids.len(),
                exact_retained = chosen.exact_retained,
                "experimental ranked shortlist"
            );
            if shortlist_mode == super::candidate_budget::ShortlistMode::On {
                ctx.string_matches
                    .retain(|m| chosen.ids.binary_search(&m.string_idx).is_ok());
            } else {
                shadow_shortlist = Some(chosen.ids);
            }
        }

        let (ngram_cache_entries, ngram_cache_bytes) = ctx.query_cache.residency();
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            posting_lists_read,
            posting_entries_visited,
            touched_string_count = ctx.touched_string_indices.len(),
            fuzzy_matches_before_cap,
            fuzzy_matches_after_cap = max_matches,
            string_matches_after_prefix_union = ctx.string_matches.len(),
            cache_snapshot_entries,
            cache_snapshot_bytes,
            ngram_cache_entries,
            ngram_cache_bytes,
            process_ngram_queries = NGRAM_QUERIES.load(Ordering::Relaxed),
            process_ngram_exact_hits = NGRAM_EXACT_HITS.load(Ordering::Relaxed),
            process_ngram_subset_hits = NGRAM_SUBSET_HITS.load(Ordering::Relaxed),
            "bigram retrieval work summary"
        );

        let phase_started = Instant::now();
        let sift4_first_pass_before = sift4_guard.count();
        let query_epoch = ctx.query_epoch;

        let match_snapshot: Vec<(u32, f32)> = ctx
            .string_matches
            .iter()
            .map(|m| (m.string_idx, m.cos_sim))
            .collect();

        // Exact name + area evidence can prevent worldwide brand names from
        // expanding into thousands of unrelated place scoring candidates.
        // Independent of token conjunction success and shortlist mode.
        let area_gate_mode = super::area_fanout::Mode::from_env(
            std::env::var("CYPRESS_AREA_FANOUT")
                .unwrap_or_default()
                .as_str(),
        );
        let mut area_gate_plan = if area_gate_mode != super::area_fanout::Mode::Off {
            if let (Some(name_fst), Some(area_fst)) =
                (prefix_fst.as_ref(), admin_alias_fst.as_ref())
            {
                let selected: Vec<u32> = match_snapshot.iter().map(|m| m.0).collect();
                super::area_fanout::build(
                    name_fst,
                    area_fst,
                    &db.admin_alias_area_offsets[..],
                    &db.admin_alias_area_data[..],
                    &query_tokens,
                    &selected,
                    &join_index,
                )
            } else {
                super::area_fanout::Plan::default()
            }
        } else {
            super::area_fanout::Plan::default()
        };
        let mut area_gate_checked = 0usize;
        let mut area_gate_rejected = 0usize;
        let mut name_evidence = Vec::<NameEvidence>::new();
        let mut street_evidence = Vec::<StreetEvidence>::new();
        let mut component_options = Vec::<ComponentMatchOption>::with_capacity(36);

        for (string_idx, _cos_sim) in &match_snapshot {
            let sid = *string_idx as usize;
            let string_name = if sid < db.string_name_offsets.len().saturating_sub(1) {
                let start = db.string_name_offsets[sid] as usize;
                let end = db.string_name_offsets[sid + 1] as usize;
                if start <= end && end <= db.string_name_bytes.len() {
                    &db.string_name_bytes[start..end]
                } else {
                    continue;
                }
            } else {
                continue;
            };

            let prepared = cypress::models::scoring::PreparedMatchCandidate::new(string_name);
            if work.enabled {
                work.first_pass_strings_scored += 1;
                work.first_pass_scorer_calls += phrases.len() as u64;
            }
            collect_component_match_options(
                &prepared,
                &phrases,
                &mut ctx.sift4_offset_arr,
                &mut component_options,
            );
            let Some(best_option) = component_options.iter().min_by(|a, b| {
                a.cost
                    .partial_cmp(&b.cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) else {
                continue;
            };
            let match_score = best_option.cost;
            let matched_mask = best_option.mask;

            let p_start = db.string_to_places_offsets[sid] as usize;
            let p_end = db.string_to_places_offsets[sid + 1] as usize;
            if work.enabled {
                work.name_mapping_entries_examined += p_end.saturating_sub(p_start) as u64;
            }
            for i in p_start..p_end {
                let place_id = db.string_to_places_data[i];
                if field_join_only_ids.binary_search(string_idx).is_ok() {
                    let Some(&area_set) = db.place_area_sets.get(place_id as usize) else {
                        continue;
                    };
                    if !field_aware::in_area(&join_index, area_set, &field_join_areas) {
                        continue;
                    }
                }
                let place_idx = place_id as usize;
                if place_idx >= place_count {
                    continue;
                }

                if let Some(&area_set) = db.place_area_sets.get(place_idx) {
                    if let Some(in_area) = area_gate_plan.check(*string_idx, area_set, &join_index)
                    {
                        area_gate_checked += 1;
                        if !in_area {
                            area_gate_rejected += 1;
                            if area_gate_mode == super::area_fanout::Mode::On {
                                continue;
                            }
                        }
                    }
                }

                for option in &component_options {
                    name_evidence.push(NameEvidence {
                        place_id,
                        mask: option.mask,
                        cost: option.cost,
                    });
                }

                let layer_rank = db.place_layer_ranks[place_idx];
                let population = db.place_populations[place_idx] as f32;
                let layer_bonus = cypress::models::scoring::get_layer_score_bonus(layer_rank);

                let is_extra = layer_rank == 0;
                let importance_bonus = if is_extra {
                    (population / 2_000.0).clamp(1.2, 5.0)
                } else {
                    (population / 200_000.0).clamp(0.0, 3.0)
                };

                let place_bonus = 5.0_f32;

                let mut total_score = match_score;

                // Area matching for unmatched tokens
                if is_multi_token && place_idx < db.place_area_sets.len() {
                    let area_set_idx = db.place_area_sets[place_idx] as usize;
                    let (area_score, final_mask) = if area_set_idx as u32 == u32::MAX {
                        (0.0, matched_mask)
                    } else {
                        let (c_score, c_mask, c_matched, c_epoch) =
                            ctx.area_match_cache[area_set_idx];
                        if c_epoch == query_epoch && c_matched == matched_mask {
                            (c_score, c_mask)
                        } else {
                            let res = score_area_matches(
                                db,
                                area_set_idx,
                                &phrases,
                                matched_mask,
                                query_epoch,
                                &mut ctx.area_phrase_scores,
                                &mut ctx.sift4_offset_arr,
                            );
                            ctx.area_match_cache[area_set_idx] =
                                (res.0, res.1, matched_mask, query_epoch);
                            res
                        }
                    };
                    total_score += area_score;

                    // Penalty for tokens that matched neither name nor area
                    let all_matched = final_mask == query_token_mask(query_tokens.len());
                    if !all_matched {
                        for (t_idx, token) in query_tokens.iter().enumerate().take(8) {
                            if final_mask & (1 << t_idx) == 0 {
                                total_score += token.len() as f32 * 3.0;
                            }
                        }
                    }

                    // Bonus when all tokens matched by the primary name explicitly, no area needed
                    if matched_mask == query_token_mask(query_tokens.len()) {
                        total_score -= 2.5;
                    }
                }

                total_score -= layer_bonus;
                total_score -= importance_bonus;
                total_score -= place_bonus;

                let neg_score = -total_score;
                ctx.place_scores.push((place_id, neg_score));
            }

            if sid + 1 < db.string_to_street_offsets.len() {
                let s_start = db.string_to_street_offsets[sid] as usize;
                let s_end = db.string_to_street_offsets[sid + 1] as usize;
                if s_start <= s_end && s_end <= db.string_to_street_data.len() {
                    if work.enabled {
                        work.street_mapping_entries_examined +=
                            s_end.saturating_sub(s_start) as u64;
                    }
                    for pos in s_start..s_end {
                        let street_group_id = db.string_to_street_data[pos];
                        if field_join_only_ids.binary_search(string_idx).is_ok() {
                            let Some(&area_set) = db.street_area_sets.get(street_group_id as usize)
                            else {
                                continue;
                            };
                            if !field_aware::in_area(&join_index, area_set, &field_join_areas) {
                                continue;
                            }
                        }
                        for option in &component_options {
                            street_evidence.push(StreetEvidence {
                                street_group_id,
                                mask: option.mask,
                                cost: option.cost,
                            });
                        }
                    }
                }
            }
        }
        if area_gate_mode != super::area_fanout::Mode::Off {
            debug!(
                target: "cypress::query::timing",
                mode = ?area_gate_mode,
                exact_name_area_interpretations = area_gate_plan.interpretations,
                gated_name_strings = area_gate_plan.gated_names(),
                preflight_place_links = area_gate_plan.preflight_links,
                checked_place_links = area_gate_checked,
                would_reject_place_links = area_gate_rejected,
                "area-aware place fanout plan"
            );
        }
        log_phase_timing("search_place_ids", "sift4_rescore_places", phase_started);
        let first_pass_sift4_calls = sift4_guard.count() - sift4_first_pass_before;
        let raw_name_evidence = name_evidence.len();
        let raw_street_evidence = street_evidence.len();

        // Canonicalize evidence before address seeding and final DP. For the
        // same semantic component and token mask, keeping the cheapest alias is
        // lossless.
        name_evidence.sort_unstable_by(|a, b| {
            a.place_id.cmp(&b.place_id).then(a.mask.cmp(&b.mask)).then(
                a.cost
                    .partial_cmp(&b.cost)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
        });
        name_evidence.dedup_by(|a, b| a.place_id == b.place_id && a.mask == b.mask);

        street_evidence.sort_unstable_by(|a, b| {
            a.street_group_id
                .cmp(&b.street_group_id)
                .then(a.mask.cmp(&b.mask))
                .then(
                    a.cost
                        .partial_cmp(&b.cost)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
        });
        street_evidence.dedup_by(|a, b| a.street_group_id == b.street_group_id && a.mask == b.mask);
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            strings_scored = work.first_pass_strings_scored,
            name_scorer_invocations = work.first_pass_scorer_calls,
            first_pass_sift4_calls,
            raw_name_evidence,
            dedup_name_evidence = name_evidence.len(),
            raw_street_evidence,
            dedup_street_evidence = street_evidence.len(),
            "first-pass matching and evidence summary"
        );

        // Structured address candidate generation. A global fuzzy hit only
        // resolves a street group. House lookup then happens locally with two
        // binary searches, never by expanding every house into the global
        // string postings.
        let phase_started = Instant::now();
        let sift4_address_before = sift4_guard.count();
        let mut house_number_lookups = 0usize;
        let mut house_number_lookup_hits = 0usize;
        let mut address_candidates_seeded = 0usize;
        let mut evidence_pos = 0usize;
        while evidence_pos < street_evidence.len() {
            let street_group_id = street_evidence[evidence_pos].street_group_id;
            let group_start = evidence_pos;
            while evidence_pos < street_evidence.len()
                && street_evidence[evidence_pos].street_group_id == street_group_id
            {
                evidence_pos += 1;
            }
            let street_options = &street_evidence[group_start..evidence_pos];
            let group_idx = street_group_id as usize;
            if group_idx >= db.street_area_sets.len() {
                continue;
            }

            for house_phrase in phrases.iter().filter(|p| is_house_number_phrase(p)) {
                let Some(best_street) = street_options
                    .iter()
                    .filter(|option| option.mask & house_phrase.mask == 0)
                    .min_by(|a, b| {
                        a.cost
                            .partial_cmp(&b.cost)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                else {
                    continue;
                };

                house_number_lookups += 1;
                let Some((house_start, house_end)) =
                    find_exact_house_range(db, group_idx, &house_phrase.bytes)
                else {
                    continue;
                };
                house_number_lookup_hits += 1;

                let matched_mask = best_street.mask | house_phrase.mask;
                let area_set_idx = db.street_area_sets[group_idx] as usize;
                let (area_score, final_mask) = if area_set_idx as u32 == u32::MAX {
                    (0.0, matched_mask)
                } else {
                    let (cached_score, cached_mask, cached_matched, cached_epoch) =
                        ctx.area_match_cache[area_set_idx];
                    if cached_epoch == query_epoch && cached_matched == matched_mask {
                        (cached_score, cached_mask)
                    } else {
                        let result = score_area_matches(
                            db,
                            area_set_idx,
                            &phrases,
                            matched_mask,
                            query_epoch,
                            &mut ctx.area_phrase_scores,
                            &mut ctx.sift4_offset_arr,
                        );
                        ctx.area_match_cache[area_set_idx] =
                            (result.0, result.1, matched_mask, query_epoch);
                        result
                    }
                };

                let base_structural_cost = best_street.cost
                    + cypress::models::scoring::get_exact_match_score(house_phrase.bytes.len())
                    - 2.0
                    + area_score;

                for house_idx in house_start..house_end {
                    let place_id = db.house_place_ids[house_idx];
                    let place_idx = place_id as usize;
                    if place_idx >= place_count {
                        continue;
                    }

                    // Postcode is house-specific, so score it before the
                    // candidate cap. This prevents a common street/number pair
                    // in the wrong city from evicting an exact postcode match
                    // before the exact component planner gets to run.
                    let mut structural_cost = base_structural_cost;
                    let mut house_mask = final_mask;
                    if let Some(postcode) =
                        get_house_postcode(db, house_idx).filter(|p| !p.is_empty())
                    {
                        let prepared =
                            cypress::models::scoring::PreparedMatchCandidate::new(postcode);
                        let mut best_postcode = cypress::models::scoring::NO_MATCH;
                        let mut best_postcode_mask = 0u8;
                        for phrase in &phrases {
                            if phrase.mask & house_mask != 0 {
                                continue;
                            }
                            let score = prepared.score(&phrase.bytes, &mut ctx.sift4_offset_arr);
                            if score != cypress::models::scoring::NO_MATCH && score < best_postcode
                            {
                                best_postcode = score;
                                best_postcode_mask = phrase.mask;
                            }
                        }
                        if best_postcode != cypress::models::scoring::NO_MATCH {
                            structural_cost += best_postcode - 1.5;
                            house_mask |= best_postcode_mask;
                        }
                    }

                    for (token_idx, token) in query_tokens.iter().enumerate().take(8) {
                        if house_mask & (1 << token_idx) == 0 {
                            structural_cost += token.len() as f32 * 3.0;
                        }
                    }

                    let score = -structural_cost + place_prior_bonus(db, place_idx);
                    ctx.place_scores.push((place_id, score));
                    address_candidates_seeded += 1;
                }
            }
        }
        log_phase_timing("search_place_ids", "structured_address_seed", phase_started);
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            house_number_lookups,
            house_number_lookup_hits,
            address_candidates_seeded,
            address_seed_sift4_calls = sift4_guard.count() - sift4_address_before,
            "structured address retrieval summary"
        );

        let phase_started = Instant::now();
        ctx.place_scores.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then(b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
        });
        ctx.place_scores.dedup_by(|a, b| a.0 == b.0);
        let unique_base_places = ctx.place_scores.len();

        results = ctx
            .place_scores
            .iter()
            .map(|&(place_id, score)| (place_id, score as f64))
            .collect();
        if results.len() > MAX_SCORED_MATCHES {
            results.select_nth_unstable_by(MAX_SCORED_MATCHES, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(MAX_SCORED_MATCHES);
        }
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            unique_base_places,
            rerank_candidates = results.len(),
            capped_by_preliminary_rank = unique_base_places.saturating_sub(results.len()),
            "place candidate fanout summary"
        );
        log_phase_timing(
            "search_place_ids",
            "truncate_base_candidates",
            phase_started,
        );

        let phase_started = Instant::now();
        let sift4_rerank_before = sift4_guard.count();
        let mut area_component_cache = HashMap::<u32, Vec<Vec<ComponentMatchOption>>>::new();
        let mut street_option_cache = HashMap::<u32, Vec<ComponentMatchOption>>::new();
        let mut planner_scratch = Vec::<ComponentMatchOption>::with_capacity(36);
        let mut rescored = Vec::with_capacity(results.len());
        for (place_id, base_score) in results.drain(..) {
            let place_idx = place_id as usize;
            if place_idx >= place_count {
                continue;
            }

            let name_start = name_evidence.partition_point(|e| e.place_id < place_id);
            let name_end = name_evidence.partition_point(|e| e.place_id <= place_id);
            let candidate_name_evidence = &name_evidence[name_start..name_end];

            let area_set = db
                .place_area_sets
                .get(place_idx)
                .copied()
                .unwrap_or(u32::MAX);
            if !area_component_cache.contains_key(&area_set) {
                let components = build_area_component_options(
                    db,
                    area_set,
                    &phrases,
                    query_epoch,
                    &mut ctx.area_phrase_scores,
                    &mut ctx.sift4_offset_arr,
                );
                area_component_cache.insert(area_set, components);
            }
            let area_components = area_component_cache
                .get(&area_set)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            // The DP is exact over the retained candidate's semantic
            // components. With at most eight query tokens there are only 256
            // token masks, so order-independent optimal assignment is bounded
            // and deterministic rather than heuristic/greedy.
            if work.enabled {
                work.dp_candidates += 1;
            }
            let exact_cost = score_structured_candidate(
                db,
                place_idx,
                &phrases,
                &query_tokens,
                candidate_name_evidence,
                area_components,
                &mut planner_scratch,
                &mut ctx.sift4_offset_arr,
                &mut street_option_cache,
                &mut work,
            );
            if work.enabled && exact_cost.is_none() {
                work.dp_fallbacks += 1;
            }
            let mut score = exact_cost
                .map(|cost| (-cost + place_prior_bonus(db, place_idx)) as f64)
                .unwrap_or(base_score);

            let lat = db.place_latitudes[place_idx] as f64;
            let lon = db.place_longitudes[place_idx] as f64;

            if let Some(bb) = bbox {
                if lon < bb[0] || lat < bb[1] || lon > bb[2] || lat > bb[3] {
                    continue;
                }
            }
            if let Some(focus_point) = focus {
                let dist_m = haversine_distance_km(focus_point, (lat, lon)) * 1000.0;
                let bonus = if dist_m < 2_000.0 {
                    2.5 * safe_focus_weight
                } else if dist_m < 10_000.0 {
                    2.0 * safe_focus_weight
                } else if dist_m < 100_000.0 {
                    1.0 * safe_focus_weight
                } else if dist_m < 1_000_000.0 {
                    0.5 * safe_focus_weight
                } else {
                    0.0
                };
                score += bonus;
            }

            rescored.push((place_id, score));
        }
        results = rescored;
        log_phase_timing(
            "search_place_ids",
            "apply_geographic_scoring",
            phase_started,
        );
        let (area_cache_hits, area_cache_misses) = ctx.area_phrase_scores.stats();
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            reranked_places = work.dp_candidates,
            dp_fallbacks = work.dp_fallbacks,
            distinct_area_sets = area_component_cache.len(),
            dp_component_steps = work.dp_component_steps,
            dp_reachable_state_visits = work.dp_reachable_states,
            dp_option_checks = work.dp_option_checks,
            dp_legal_transitions = work.dp_legal_transitions,
            local_street_scorer_invocations = work.local_street_scorer_calls,
            local_house_scorer_invocations = work.local_house_scorer_calls,
            local_postcode_scorer_invocations = work.local_postcode_scorer_calls,
            rerank_sift4_calls = sift4_guard.count() - sift4_rerank_before,
            total_sift4_calls = sift4_guard.count(),
            "structured DP work summary"
        );
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            area_cache_hits,
            area_cache_misses,
            "ADR-style administrative area phrase cache summary"
        );

        let phase_started = Instant::now();
        let final_limit = if focus.is_some() {
            max_results
                .saturating_mul(FOCUS_RESCORE_MULTIPLIER)
                .max(FOCUS_RESCORE_FLOOR)
                .min(MAX_SCORED_MATCHES)
        } else {
            max_results
        };
        if results.len() > final_limit {
            results.select_nth_unstable_by(final_limit, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(final_limit);
        }

        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        results.truncate(max_results);
        // Determine whether the shadow shortlist retained at least one
        // name alias for each baseline final result. A missing alias is
        // a potential recall regression, NOT proof of an altered top-K.
        if let Some(ids) = shadow_shortlist.as_ref() {
            // Query baseline results against the shortlisted name mappings.
            // Compiler place-postings are sorted, so each membership check is
            // O(log postings) instead of a full linear scan.
            let covered = super::candidate_budget::shadow_name_coverage(
                ids,
                &db.string_to_places_offsets[..],
                db.string_to_places_data,
                results.iter().map(|(place_id, _)| *place_id),
            );
            debug!(target: "cypress::query::timing",
                baseline_topk = results.len(), covered,
                potentially_missing = results.len()-covered,
                "shortlist shadow coverage estimate");
        }
        log_phase_timing("search_place_ids", "final_sort_truncate", phase_started);

        // Cache savings are counterfactual posting visits, not CPU or RAM savings.
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            query_utf8_bytes = query.len(),
            query_bigrams = query_bigram_count,
            cold_bigram_posting_entries = cold_postings.entries,
            largest_bigram_posting_entries = cold_postings.largest_list_len,
            largest_bigram = ?cold_postings.largest_gram,
            posting_lists_read,
            posting_entries_examined = posting_entries_visited,
            posting_entries_avoided_by_cache =
                cold_postings.entries.saturating_sub(posting_entries_visited as u64),
            cache_exact_hit,
            cache_subset_hit,
            cache_restored_string_ids,
            touched_string_count = ctx.touched_string_indices.len(),
            prefix_candidate_count = prefix_string_ids.len(),
            name_mapping_entries_examined = work.name_mapping_entries_examined,
            street_mapping_entries_examined = work.street_mapping_entries_examined,
            string_candidates = ctx.string_matches.len(),
            preliminary_place_candidates = unique_base_places,
            dp_candidates = work.dp_candidates,
            total_sift4_calls = sift4_guard.count(),
            final_result_count = results.len(),
            search_elapsed_ms = total_started.elapsed().as_millis(),
            "query retrieval baseline"
        );
    });

    log_total_timing("search_place_ids", total_started, results.len());
    results
}

/// Run the bigram cosine similarity search over the rkyv database,
/// returning scored source IDs with their final scores.
fn bigram_search(
    memdb: &Memdb,
    text: &str,
    focus: Option<(f64, f64)>,
    focus_weight: f64,
    bbox: Option<[f64; 4]>,
    max_results: usize,
) -> Vec<(String, f64)> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "bigram_search",
        query_len = text.len(),
        max_results,
        has_focus = focus.is_some(),
        has_bbox = bbox.is_some(),
        "search start"
    );

    let phase_started = Instant::now();
    let scored_places = search_place_ids(memdb, text, focus, focus_weight, bbox, max_results);
    log_phase_timing("bigram_search", "search_place_ids", phase_started);

    let memdb_data = memdb.get_data();

    let phase_started = Instant::now();
    let mut results = Vec::with_capacity(scored_places.len());
    for (place_id, score) in scored_places {
        let Some(source_id) = memdb_data.get_place_source_id(place_id as usize) else {
            continue;
        };
        results.push((source_id, score));
    }
    log_phase_timing("bigram_search", "resolve_source_ids", phase_started);

    log_total_timing("bigram_search", total_started, results.len());
    results
}

/// Use the spatial grid for reverse geocoding — find places near a coordinate.
fn spatial_search_place_ids(
    memdb: &Memdb,
    lon: f64,
    lat: f64,
    max_results: usize,
    radius_km: f64,
) -> Vec<(u32, f64)> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "spatial_search_place_ids",
        lon,
        lat,
        max_results,
        radius_km,
        "reverse search start"
    );

    let memdb_data = memdb.get_data();
    let db = &memdb_data.get_view();

    let delta_deg = (radius_km / 111.0) as f32;
    let min_lat = (lat as f32 - delta_deg).max(-90.0);
    let max_lat = (lat as f32 + delta_deg).min(89.999);
    let min_lon = (lon as f32 - delta_deg).max(-180.0);
    let max_lon = (lon as f32 + delta_deg).min(179.999);

    let cell_size = CypressMemDb::GRID_CELL_SIZE;
    let cols = CypressMemDb::GRID_COLS;

    let row_start = ((min_lat + 90.0) / cell_size) as usize;
    let row_end = ((max_lat + 90.0) / cell_size) as usize;
    let col_start = ((min_lon + 180.0) / cell_size) as usize;
    let col_end = ((max_lon + 180.0) / cell_size) as usize;

    debug!(
        target: "cypress::query::timing",
        op = "spatial_search_place_ids",
        row_start,
        row_end,
        col_start,
        col_end,
        "grid bounds"
    );

    let phase_started = Instant::now();
    let mut candidates: Vec<(u32, f64)> = Vec::new();

    let grid = memdb_data.grid_mmap.as_ref().map(|mmap| {
        cypress::models::spatial_grid::SpatialGridView::from_validated(mmap)
            .expect("spatial grid validated during generation load")
    });
    for row in row_start..=row_end {
        for col in col_start..=col_end {
            let cell_id = (row * cols + col) as u32;
            let ids: Box<dyn Iterator<Item = u32> + '_> = if let Some(grid) = &grid {
                match grid.cell_places(cell_id) {
                    Some(places) => Box::new(places),
                    None => continue,
                }
            } else if let Ok(pos) = db.active_cells.binary_search_by(|c| c.cmp(&cell_id)) {
                let start = db.cell_offsets[pos] as usize;
                let end = db.cell_offsets[pos + 1] as usize;
                Box::new(db.cell_places[start..end].iter().copied())
            } else {
                continue;
            };
            for place_id in ids {
                let place_idx = place_id as usize;
                if place_idx >= db.place_latitudes.len() {
                    continue;
                }

                let place_lat = db.place_latitudes[place_idx] as f64;
                let place_lon = db.place_longitudes[place_idx] as f64;

                let dist = haversine_distance_km((lat, lon), (place_lat, place_lon));
                if dist <= radius_km {
                    candidates.push((place_id, dist));
                }
            }
        }
    }
    log_phase_timing("spatial_search_place_ids", "scan_grid_cells", phase_started);

    let phase_started = Instant::now();
    candidates.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.truncate(max_results);
    log_phase_timing(
        "spatial_search_place_ids",
        "sort_and_truncate",
        phase_started,
    );
    log_total_timing("spatial_search_place_ids", total_started, candidates.len());
    candidates
}

/// Use the spatial grid for reverse geocoding — find places near a coordinate.
fn spatial_search(
    memdb: &Memdb,
    lon: f64,
    lat: f64,
    max_results: usize,
    radius_km: f64,
) -> Vec<(String, f64)> {
    let total_started = Instant::now();

    let phase_started = Instant::now();
    let nearby = spatial_search_place_ids(memdb, lon, lat, max_results, radius_km);
    log_phase_timing("spatial_search", "spatial_search_place_ids", phase_started);

    let memdb_data = memdb.get_data();

    let phase_started = Instant::now();
    let mut resolved = Vec::with_capacity(nearby.len());
    for (place_id, dist) in nearby {
        let Some(source_id) = memdb_data.get_place_source_id(place_id as usize) else {
            continue;
        };
        resolved.push((source_id, dist));
    }
    log_phase_timing("spatial_search", "resolve_source_ids", phase_started);
    log_total_timing("spatial_search", total_started, resolved.len());

    resolved
}

pub async fn execute_search(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    cpu: &SearchCpu,
    params: SearchParams,
) -> Result<TimedSearchResults> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "execute_search",
        query_len = params.text.len(),
        size = params.size,
        has_focus = params.focus_lat.is_some() && params.focus_lon.is_some(),
        has_bbox = params.bbox.is_some(),
        layers_count = params.layers.as_ref().map_or(0, Vec::len),
        "request start"
    );

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };
    let focus_weight = params.focus_weight.unwrap_or(50.0);

    let phase_started = Instant::now();
    let memdb_for_search = Arc::clone(memdb);
    let text = params.text.clone();
    let bbox = params.bbox;
    let size = params.size;
    let scored_ids = cpu
        .run(move || bigram_search(&memdb_for_search, &text, focus, focus_weight, bbox, size))
        .await?;
    log_phase_timing("execute_search", "bigram_search", phase_started);
    debug!(
        target: "cypress::query::timing",
        op = "execute_search",
        scored_ids = scored_ids.len(),
        "search candidates ready"
    );

    let mut results = Vec::new();
    let mut admin_ids = std::collections::HashSet::new();

    let phase_started = Instant::now();
    // Preserve ranking order while limiting Scylla fan-out per request.
    // Build futures directly: an iterator map closure captured across await
    // triggers a higher-ranked FnOnce lifetime error in the Axum handler.
    // The IDs stay alive through the buffered fetch, so borrowing is safe.
    let mut fetch_futures = Vec::with_capacity(scored_ids.len());
    for (id, _) in &scored_ids {
        fetch_futures.push(scylla_client.get_place(id));
    }
    let fetched = stream::iter(fetch_futures)
        .buffered(8)
        .collect::<Vec<_>>()
        .await;
    log_phase_timing("execute_search", "fetch_places", phase_started);

    let phase_started = Instant::now();
    let mut places_with_scores = Vec::new();
    for (i, fetch_result) in fetched.into_iter().enumerate() {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                places_with_scores.push((place, scored_ids[i].1));
            }
        }
    }
    log_phase_timing("execute_search", "decode_places", phase_started);

    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();

    let phase_started = Instant::now();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;
    log_phase_timing("execute_search", "fetch_admin_areas", phase_started);

    let phase_started = Instant::now();
    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();
    log_phase_timing("execute_search", "decode_admin_areas", phase_started);

    let phase_started = Instant::now();
    for (place, score) in places_with_scores {
        if let Some(result) = place_to_search_result(place, score, &params.lang, &parsed_admin_map)
        {
            results.push(result);
        }
    }
    log_phase_timing("execute_search", "build_response", phase_started);
    log_total_timing("execute_search", total_started, results.len());

    let took_ms = total_started.elapsed().as_millis();

    Ok(TimedSearchResults { results, took_ms })
}

pub async fn execute_search_v2(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    cpu: &SearchCpu,
    params: SearchParams,
) -> Result<TimedSearchResultsV2> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "execute_search_v2",
        query_len = params.text.len(),
        size = params.size,
        has_focus = params.focus_lat.is_some() && params.focus_lon.is_some(),
        has_bbox = params.bbox.is_some(),
        layers_count = params.layers.as_ref().map_or(0, Vec::len),
        "request start"
    );

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };
    let focus_weight = params.focus_weight.unwrap_or(50.0);

    let phase_started = Instant::now();
    let memdb_for_search = Arc::clone(memdb);
    let text = params.text.clone();
    let bbox = params.bbox;
    let size = params.size;
    let scored_ids = cpu
        .run(move || bigram_search(&memdb_for_search, &text, focus, focus_weight, bbox, size))
        .await?;
    log_phase_timing("execute_search_v2", "bigram_search", phase_started);
    debug!(
        target: "cypress::query::timing",
        op = "execute_search_v2",
        scored_ids = scored_ids.len(),
        "search candidates ready"
    );

    let mut admin_ids = std::collections::HashSet::new();

    let phase_started = Instant::now();
    let mut fetch_futures = Vec::with_capacity(scored_ids.len());
    for (id, _) in &scored_ids {
        fetch_futures.push(scylla_client.get_place(id));
    }
    let fetched = stream::iter(fetch_futures)
        .buffered(8)
        .collect::<Vec<_>>()
        .await;
    log_phase_timing("execute_search_v2", "fetch_places", phase_started);

    let phase_started = Instant::now();
    let mut places_with_scores = Vec::new();
    for (i, fetch_result) in fetched.into_iter().enumerate() {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                places_with_scores.push((place, scored_ids[i].1));
            }
        }
    }
    log_phase_timing("execute_search_v2", "decode_places", phase_started);

    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();

    let phase_started = Instant::now();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;
    log_phase_timing("execute_search_v2", "fetch_admin_areas", phase_started);

    let phase_started = Instant::now();
    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();
    log_phase_timing("execute_search_v2", "decode_admin_areas", phase_started);

    let phase_started = Instant::now();
    let mut results = Vec::new();
    for (place, score) in places_with_scores {
        if let Some(result) =
            place_to_search_result_v2(place, score, &params.lang, &parsed_admin_map)
        {
            results.push(result);
        }
    }
    log_phase_timing("execute_search_v2", "build_response", phase_started);
    log_total_timing("execute_search_v2", total_started, results.len());

    let took_ms = total_started.elapsed().as_millis();

    Ok(TimedSearchResultsV2 { results, took_ms })
}

pub async fn execute_reverse(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    cpu: &SearchCpu,
    lon: f64,
    lat: f64,
    size: usize,
    _layers: Option<Vec<String>>,
) -> Result<Vec<SearchResult>> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "execute_reverse",
        lon,
        lat,
        size,
        "request start"
    );

    let phase_started = Instant::now();
    let memdb_for_search = Arc::clone(memdb);
    let nearby = cpu
        .run(move || spatial_search(&memdb_for_search, lon, lat, size, 1.0))
        .await?;
    log_phase_timing("execute_reverse", "spatial_search", phase_started);

    let phase_started = Instant::now();
    let mut fetch_futures = Vec::with_capacity(nearby.len());
    for (id, _) in &nearby {
        fetch_futures.push(scylla_client.get_place(id));
    }
    let fetched = stream::iter(fetch_futures)
        .buffered(8)
        .collect::<Vec<_>>()
        .await;
    log_phase_timing("execute_reverse", "fetch_places", phase_started);

    let mut admin_ids = std::collections::HashSet::new();
    let mut normalized_places = Vec::new();

    let phase_started = Instant::now();
    for fetch_result in fetched {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                normalized_places.push(place);
            }
        }
    }
    log_phase_timing("execute_reverse", "decode_places", phase_started);

    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();

    let phase_started = Instant::now();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;
    log_phase_timing("execute_reverse", "fetch_admin_areas", phase_started);

    let phase_started = Instant::now();
    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();
    log_phase_timing("execute_reverse", "decode_admin_areas", phase_started);

    let phase_started = Instant::now();
    let mut results = Vec::new();
    for place in normalized_places {
        if let Some(result) = place_to_search_result(place, 1.0, &None, &parsed_admin_map) {
            results.push(result);
        }
    }
    log_phase_timing("execute_reverse", "build_response", phase_started);
    log_total_timing("execute_reverse", total_started, results.len());

    Ok(results)
}

pub async fn execute_reverse_v2(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    cpu: &SearchCpu,
    lon: f64,
    lat: f64,
    size: usize,
    _layers: Option<Vec<String>>,
    lang: Option<String>,
) -> Result<Vec<SearchResultV2>> {
    let total_started = Instant::now();
    debug!(
        target: "cypress::query::timing",
        op = "execute_reverse_v2",
        lon,
        lat,
        size,
        has_lang = lang.is_some(),
        "request start"
    );

    let phase_started = Instant::now();
    let memdb_for_search = Arc::clone(memdb);
    let nearby = cpu
        .run(move || spatial_search(&memdb_for_search, lon, lat, size, 1.0))
        .await?;
    log_phase_timing("execute_reverse_v2", "spatial_search", phase_started);

    let phase_started = Instant::now();
    let mut fetch_futures = Vec::with_capacity(nearby.len());
    for (id, _) in &nearby {
        fetch_futures.push(scylla_client.get_place(id));
    }
    let fetched = stream::iter(fetch_futures)
        .buffered(8)
        .collect::<Vec<_>>()
        .await;
    log_phase_timing("execute_reverse_v2", "fetch_places", phase_started);

    let mut admin_ids = std::collections::HashSet::new();
    let mut normalized_places = Vec::new();

    let phase_started = Instant::now();
    for fetch_result in fetched {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                normalized_places.push(place);
            }
        }
    }
    log_phase_timing("execute_reverse_v2", "decode_places", phase_started);

    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();

    let phase_started = Instant::now();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;
    log_phase_timing("execute_reverse_v2", "fetch_admin_areas", phase_started);

    let phase_started = Instant::now();
    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();
    log_phase_timing("execute_reverse_v2", "decode_admin_areas", phase_started);

    let phase_started = Instant::now();
    let mut results = Vec::new();
    for place in normalized_places {
        if let Some(result) = place_to_search_result_v2(place, 1.0, &lang, &parsed_admin_map) {
            results.push(result);
        }
    }
    log_phase_timing("execute_reverse_v2", "build_response", phase_started);
    log_total_timing("execute_reverse_v2", total_started, results.len());

    Ok(results)
}

fn collect_admin_ids(place: &NormalizedPlace, ids: &mut std::collections::HashSet<String>) {
    if let Some(ref id) = place.parent.country {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.macro_region {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.region {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.macro_county {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.county {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.local_admin {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.locality {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.borough {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.neighbourhood {
        ids.insert(id.clone());
    }
}

fn resolve_admin_name(
    id: &Option<String>,
    map: &HashMap<String, AdminEntry>,
    lang: &Option<String>,
) -> Option<String> {
    id.as_ref().and_then(|id_str| {
        map.get(id_str).and_then(|entry| {
            lang.as_ref()
                .and_then(|l| entry.names.get(l))
                .cloned()
                .or_else(|| entry.names.get("default").cloned())
                .or_else(|| entry.name.clone())
        })
    })
}

fn resolve_admin_names(
    id: &Option<String>,
    map: &HashMap<String, AdminEntry>,
) -> Option<HashMap<String, String>> {
    id.as_ref()
        .and_then(|id_str| map.get(id_str).map(|entry| entry.names.clone()))
}

fn resolve_place_display_name(place: &NormalizedPlace, preferred_lang: &Option<String>) -> String {
    preferred_lang
        .as_ref()
        .and_then(|lang| place.name.get(lang))
        .or_else(|| place.name.get("default"))
        .or_else(|| place.name.values().next())
        .cloned()
        .or_else(|| {
            let address = place.address.as_ref()?;
            let anchor = address
                .street
                .as_deref()
                .or(address.place.as_deref())?
                .trim();
            if anchor.is_empty() {
                return None;
            }
            match address.housenumber.as_deref().map(str::trim) {
                Some(house) if !house.is_empty() => Some(format!("{} {}", house, anchor)),
                _ => Some(anchor.to_string()),
            }
        })
        .unwrap_or_default()
}

fn place_to_search_result(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResult> {
    let display_name = resolve_place_display_name(&place, preferred_lang);

    let place_rank = get_layer_rank(place.layer);

    let resolve_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_name(id, admin_map, preferred_lang)
        } else {
            None
        }
    };

    Some(SearchResult {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [place.center_point.lon, place.center_point.lat],
        },
        properties: Properties {
            id: place.source_id,
            layer: format!("{:?}", place.layer).to_lowercase(),
            name: display_name,
            names: place.name,
            housenumber: place.address.as_ref().and_then(|a| a.housenumber.clone()),
            street: place.address.as_ref().and_then(|a| a.street.clone()),
            postcode: place.address.as_ref().and_then(|a| a.postcode.clone()),
            country: resolve_if_larger(Layer::Country, &place.parent.country),
            region: resolve_if_larger(Layer::Region, &place.parent.region),
            county: resolve_if_larger(Layer::County, &place.parent.county),
            locality: resolve_if_larger(Layer::Locality, &place.parent.locality),
            neighbourhood: resolve_if_larger(Layer::Neighbourhood, &place.parent.neighbourhood),
            categories: place.categories,
            confidence: score,
        },
    })
}

fn place_to_search_result_v2(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResultV2> {
    let display_name = resolve_place_display_name(&place, preferred_lang);

    let place_rank = get_layer_rank(place.layer);

    let resolve_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_name(id, admin_map, preferred_lang)
        } else {
            None
        }
    };

    let resolve_names_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_names(id, admin_map)
        } else {
            None
        }
    };

    Some(SearchResultV2 {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [place.center_point.lon, place.center_point.lat],
        },
        properties: PropertiesV2 {
            id: place.source_id,
            layer: format!("{:?}", place.layer).to_lowercase(),
            name: display_name,
            names: place.name,
            housenumber: place.address.as_ref().and_then(|a| a.housenumber.clone()),
            street: place.address.as_ref().and_then(|a| a.street.clone()),
            postcode: place.address.as_ref().and_then(|a| a.postcode.clone()),
            country: resolve_if_larger(Layer::Country, &place.parent.country),
            country_names: resolve_names_if_larger(Layer::Country, &place.parent.country),
            region: resolve_if_larger(Layer::Region, &place.parent.region),
            region_names: resolve_names_if_larger(Layer::Region, &place.parent.region),
            county: resolve_if_larger(Layer::County, &place.parent.county),
            county_names: resolve_names_if_larger(Layer::County, &place.parent.county),
            locality: resolve_if_larger(Layer::Locality, &place.parent.locality),
            locality_names: resolve_names_if_larger(Layer::Locality, &place.parent.locality),
            neighbourhood: resolve_if_larger(Layer::Neighbourhood, &place.parent.neighbourhood),
            neighbourhood_names: resolve_names_if_larger(
                Layer::Neighbourhood,
                &place.parent.neighbourhood,
            ),
            categories: place.categories,
            confidence: score,
        },
    })
}

fn get_layer_rank(layer: Layer) -> u8 {
    match layer {
        Layer::Country => 100,
        Layer::MacroRegion => 90,
        Layer::Region => 80,
        Layer::MacroCounty => 70,
        Layer::County => 60,
        Layer::LocalAdmin => 50,
        Layer::Locality => 40,
        Layer::Borough => 30,
        Layer::Neighbourhood => 20,
        Layer::Street | Layer::Address | Layer::Venue => 10,
        Layer::Admin => 50,
    }
}

pub fn haversine_distance_km(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let (lat1, lon1) = p1;
    let (lat2, lon2) = p2;

    let r = 6371.0;

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_posting_cost_counts_sparse_and_duplicate_keys() {
        let offsets = [0, 3, 3, 8];
        let cost = cold_posting_cost(&offsets, &[0, 1, 2]);
        assert_eq!(cost.entries, 8);
        assert_eq!(cost.largest_list_len, 5);
        assert_eq!(cost.largest_gram, Some(2));
        assert_eq!(cold_posting_cost(&offsets, &[0, 0]).entries, 6);
        assert_eq!(cold_posting_cost(&offsets, &[]), ColdPostingCost::default());
        assert_eq!(
            cold_posting_cost(&offsets, &[5]),
            ColdPostingCost::default()
        );
    }

    use cypress::models::normalized::{AdminHierarchyIds, NormalizedPlace};
    use cypress::models::place::{Address, GeoPoint, Layer, OsmType};
    use std::collections::HashMap;

    #[test]
    fn prefix_mode_supports_legacy_rollback() {
        assert_eq!(PrefixMode::parse("bounded"), Some(PrefixMode::Bounded));
        assert_eq!(PrefixMode::parse(" LEGACY "), Some(PrefixMode::Legacy));
        assert_eq!(PrefixMode::parse("invalid"), None);
    }

    #[test]
    fn broad_prefix_is_not_arbitrarily_truncated() {
        let mut bytes = Vec::new();
        {
            let mut builder = fst::MapBuilder::new(&mut bytes).unwrap();
            for (i, s) in ["aa", "aaa", "aab", "aac", "abc"].iter().enumerate() {
                builder.insert(*s, i as u64).unwrap();
            }
            builder.finish().unwrap();
        }
        let fst = Map::new(bytes.as_slice()).unwrap();
        let mut ids = vec![0]; // exact `aa` survives an overbroad prefix.
        assert!(probe_bounded_prefix(&fst, "aa", 2, &mut ids).is_none());
        assert_eq!(ids, vec![0]);
        assert_eq!(probe_bounded_prefix(&fst, "abc", 2, &mut ids), Some(1));
        assert_eq!(ids, vec![0, 4]);
    }

    #[test]
    fn cosine_mode_ab_defaults_to_adr_and_supports_explicit_rollback() {
        assert_eq!(CosineMode::parse("adr"), Some(CosineMode::Adr));
        assert_eq!(CosineMode::parse(" ADR "), Some(CosineMode::Adr));
        assert_eq!(CosineMode::parse("cypress"), Some(CosineMode::Cypress));
        assert_eq!(CosineMode::parse("CYPRESS"), Some(CosineMode::Cypress));
        assert_eq!(CosineMode::parse("unexpected"), None);
    }

    #[test]
    fn adr_query_bigram_set_caps_at_128_occurrences_before_dedup() {
        let mut grams = Vec::new();
        collect_adr_query_bigrams("aaaa a", &mut grams);
        assert_eq!(grams, vec![(b'a' as u16) | ((b'a' as u16) << 8)]);

        let input = "ab".repeat(100);
        let mut adr_grams = Vec::new();
        collect_adr_query_bigrams(&input, &mut adr_grams);
        let mut cypress_grams = Vec::new();
        collect_query_bigrams(&input, &mut cypress_grams);
        assert_eq!(adr_query_bigram_occurrences(&input), 199);
        assert_eq!(adr_query_bigram_occurrences(&input).min(128), 128);
        assert_eq!(adr_grams, cypress_grams);

        // A bigram appearing only after the first 128 occurrences is excluded.
        let input = format!("{}xy", "a".repeat(130));
        collect_adr_query_bigrams(&input, &mut adr_grams);
        let aa = (b'a' as u16) | ((b'a' as u16) << 8);
        assert_eq!(adr_grams, vec![aa]);
        collect_query_bigrams(&input, &mut cypress_grams);
        assert!(cypress_grams.len() > adr_grams.len());
    }

    #[test]
    fn adr_cosine_query_counts_repeated_bigrams() {
        let mut unique = Vec::new();
        collect_query_bigrams("aaaa a", &mut unique);
        assert_eq!(unique.len(), 1); // The only bigram is "aa".
        assert_eq!(adr_query_bigram_occurrences("aaaa a"), 3);
        // ADR's stored-string denominator for "aaaa" is 4 - 1 = 3,
        // rather than Cypress's unique-bigram denominator of 1.
        let score = adr_cosine_score(1, 4, 3).unwrap();
        assert!((score - 1.0 / 9.0).abs() < 0.00001);
    }

    #[test]
    fn adr_cosine_guards_empty_denominators_and_caps_byte_length() {
        assert_eq!(adr_cosine_score(1, 0, 2), None);
        assert_eq!(adr_cosine_score(1, 1, 2), None);
        assert_eq!(adr_cosine_score(1, 20, 0), None);
        assert_eq!(adr_cosine_score(2, 300, 2), Some(4.0 / (255.0 * 2.0)));
    }

    #[test]
    fn component_dp_finds_global_optimum_instead_of_greedy_choice() {
        let mut dp = [f32::INFINITY; 256];
        dp[0] = 0.0;
        let mut work = SearchWorkStats::default();

        // A greedy first component would consume both tokens for -5. The
        // globally optimal assignment leaves token 2 for the second component:
        // 0 + (-10) = -10.
        let component_a = [
            ComponentMatchOption {
                mask: 0b11,
                cost: -5.0,
            },
            ComponentMatchOption {
                mask: 0b01,
                cost: 0.0,
            },
        ];
        let component_b = [ComponentMatchOption {
            mask: 0b10,
            cost: -10.0,
        }];

        advance_component_dp(&mut dp, &component_a, 0b11, &mut work);
        advance_component_dp(&mut dp, &component_b, 0b11, &mut work);

        assert_eq!(dp[0b11], -10.0);
    }

    #[test]
    fn component_planner_is_address_order_invariant() {
        fn solve(street_mask: u8, house_mask: u8, admin_mask: u8) -> f32 {
            let mut dp = [f32::INFINITY; 256];
            dp[0] = 0.0;
            let mut work = SearchWorkStats::default();
            advance_component_dp(
                &mut dp,
                &[ComponentMatchOption {
                    mask: street_mask,
                    cost: -8.0,
                }],
                0b111,
                &mut work,
            );
            advance_component_dp(
                &mut dp,
                &[ComponentMatchOption {
                    mask: house_mask,
                    cost: -3.0,
                }],
                0b111,
                &mut work,
            );
            advance_component_dp(
                &mut dp,
                &[ComponentMatchOption {
                    mask: admin_mask,
                    cost: -6.0,
                }],
                0b111,
                &mut work,
            );
            dp[0b111]
        }

        // street-house-admin vs house-street-admin consume different token
        // positions but represent the same semantic components.
        assert_eq!(solve(0b001, 0b010, 0b100), solve(0b010, 0b001, 0b100));
        assert_eq!(query_token_mask(8), u8::MAX);
    }

    #[test]
    fn address_only_place_has_useful_display_name() {
        let place = NormalizedPlace {
            source_id: "node/1".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 1,
            wikidata_id: None,
            importance: None,
            population: None,
            layer: Layer::Address,
            categories: vec![],
            name: HashMap::new(),
            phrase: None,
            address: Some(Address {
                housenumber: Some("10".to_string()),
                street: Some("Bahnhofstrasse".to_string()),
                ..Default::default()
            }),
            center_point: GeoPoint { lat: 0.0, lon: 0.0 },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };

        assert_eq!(
            resolve_place_display_name(&place, &None),
            "10 Bahnhofstrasse"
        );
    }

    #[test]
    fn test_place_to_search_result_v2() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "London".to_string());
        names.insert("fr".to_string(), "Londres".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "United Kingdom".to_string());
        country_names.insert("de".to_string(), "Vereinigtes Königreich".to_string());

        let country_entry = AdminEntry {
            name: Some("United Kingdom".to_string()),
            abbr: Some("UK".to_string()),
            id: Some(1),
            bbox: None,
            names: country_names.clone(),
            population: None,
        };

        let mut admin_map = HashMap::new();
        admin_map.insert("relation/1".to_string(), country_entry);

        let mut place = NormalizedPlace {
            source_id: "test:1".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 123,
            wikidata_id: None,
            importance: Some(0.5),
            population: None,
            layer: Layer::Locality,
            categories: vec![],
            name: names.clone(),
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 0.1,
                lat: 51.5,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string());

        let result =
            place_to_search_result_v2(place, 1.0, &Some("fr".to_string()), &admin_map).unwrap();

        assert_eq!(result.properties.name, "Londres");
        assert_eq!(result.properties.names.get("default").unwrap(), "London");
        assert_eq!(result.properties.names.get("fr").unwrap(), "Londres");

        assert!(result.properties.country_names.is_some());
        let c_names = result.properties.country_names.unwrap();
        assert_eq!(c_names.get("default").unwrap(), "United Kingdom");
        assert_eq!(c_names.get("de").unwrap(), "Vereinigtes Königreich");
    }

    #[test]
    fn test_place_to_search_result_v2_full_hierarchy() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "Paris".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "France".to_string());

        let mut region_names = HashMap::new();
        region_names.insert("default".to_string(), "Ile-de-France".to_string());
        region_names.insert("en".to_string(), "Isle of France".to_string());

        let admin_map = HashMap::from([
            (
                "relation/1".to_string(),
                AdminEntry {
                    name: Some("France".to_string()),
                    names: country_names,
                    ..Default::default()
                },
            ),
            (
                "relation/2".to_string(),
                AdminEntry {
                    name: Some("Ile-de-France".to_string()),
                    names: region_names,
                    ..Default::default()
                },
            ),
        ]);

        let mut place = NormalizedPlace {
            source_id: "test:2".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 456,
            wikidata_id: None,
            importance: Some(0.8),
            population: None,
            layer: Layer::Locality,
            categories: vec![],
            name: names,
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 2.35,
                lat: 48.85,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string());
        place.parent.region = Some("relation/2".to_string());

        let result =
            place_to_search_result_v2(place, 1.0, &Some("en".to_string()), &admin_map).unwrap();

        assert_eq!(
            result
                .properties
                .country_names
                .unwrap()
                .get("default")
                .unwrap(),
            "France"
        );
        assert_eq!(
            result.properties.region_names.unwrap().get("en").unwrap(),
            "Isle of France"
        );
    }

    #[test]
    fn test_place_to_search_result_v2_hierarchy_filtering() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "Catalonia".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "Spain".to_string());

        let mut county_names = HashMap::new();
        county_names.insert("default".to_string(), "Barcelona".to_string());

        let admin_map = HashMap::from([
            (
                "relation/1".to_string(),
                AdminEntry {
                    name: Some("Spain".to_string()),
                    names: country_names,
                    ..Default::default()
                },
            ),
            (
                "relation/3".to_string(),
                AdminEntry {
                    name: Some("Barcelona".to_string()),
                    names: county_names,
                    ..Default::default()
                },
            ),
        ]);

        let mut place = NormalizedPlace {
            source_id: "test:region".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Relation,
            osm_id: 111,
            wikidata_id: None,
            importance: Some(1.0),
            population: None,
            layer: Layer::Region,
            categories: vec![],
            name: names,
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 2.0,
                lat: 41.0,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string());
        place.parent.county = Some("relation/3".to_string());

        let result = place_to_search_result_v2(place, 1.0, &None, &admin_map).unwrap();

        // Country (Rank 100) > Region (Rank 80) -> Should be present
        assert_eq!(result.properties.country.as_deref(), Some("Spain"));

        // County (Rank 60) <= Region (Rank 80) -> Should be filtered out
        assert_eq!(result.properties.county, None);
    }
}
