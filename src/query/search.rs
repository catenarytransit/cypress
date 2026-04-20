use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

use cypress::models::memdb::CypressMemDb;
use cypress::models::normalized::NormalizedPlace;
use cypress::models::place::Layer;
use cypress::models::AdminEntry;
use cypress::scylla::ScyllaClient;

use super::memdb::{CosSimMatch, Memdb, GUESS_CONTEXT};

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
const MAX_STRING_MATCHES: usize = 500;
const MAX_SCORED_MATCHES: usize = 500;
const FOCUS_RESCORE_MULTIPLIER: usize = 5;
const FOCUS_RESCORE_FLOOR: usize = 50;
const SLOW_PHASE_WARN_MS: u128 = 250;
const SLOW_SEARCH_WARN_MS: u128 = 1000;

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

/// Tokenize a query string into words.
fn tokenize_query(query: &str) -> Vec<&[u8]> {
    query
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .filter(|s| !s.is_empty())
        .map(|s| s.as_bytes())
        .collect()
}

/// Try to match unmatched query tokens against area names for a place.
fn score_area_matches(
    db: &cypress::models::memdb::ArchivedCypressMemDb,
    area_set_idx: usize,
    query_tokens: &[&[u8]],
    matched_mask: u8,
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

    let max_tokens = query_tokens.len().min(8);
    let mut current_mask = matched_mask;
    let mut area_score = 0.0f32;
    let mut areas_matched = 0u32;

    for t_idx in 0..max_tokens {
        if current_mask & (1 << t_idx) != 0 {
            continue;
        }

        let token = query_tokens[t_idx];
        let mut best_area_score = cypress::models::scoring::NO_MATCH;

        for ai in as_start..as_end {
            let area_idx = db.area_set_data[ai] as usize;
            if area_idx >= db.area_name_offsets.len().saturating_sub(1) {
                continue;
            }

            let name_start = db.area_name_offsets[area_idx] as usize;
            let name_end = db.area_name_offsets[area_idx + 1] as usize;
            if name_start > name_end || name_end > db.area_name_bytes.len() {
                continue;
            }

            let area_name = &db.area_name_bytes[name_start..name_end];
            let score = cypress::models::scoring::get_area_match_score(area_name, token);
            if score < best_area_score {
                best_area_score = score;
            }
        }

        if best_area_score != cypress::models::scoring::NO_MATCH {
            current_mask |= 1 << t_idx;
            area_score += best_area_score;

            let area_pop_bonus = {
                let mut best_pop = 0u32;
                for ai in as_start..as_end {
                    let area_idx = db.area_set_data[ai] as usize;
                    if area_idx < db.area_populations.len() {
                        let p = db.area_populations[area_idx];
                        if p > best_pop {
                            best_pop = p;
                        }
                    }
                }
                (best_pop as f32 / 10_000_000.0) * 2.0
            };
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
    let db = memdb_data.get_archived();
    let string_count = db.string_bigram_counts.len();
    let place_count = db.place_latitudes.len();
    let safe_focus_weight = focus_weight.max(0.001);

    let query_tokens = tokenize_query(&query);
    let is_multi_token = query_tokens.len() > 1;

    let mut results = Vec::<(u32, f64)>::new();

    GUESS_CONTEXT.with(|cell| {
        let phase_started = Instant::now();
        let mut ctx = cell.borrow_mut();
        ctx.clear(string_count, place_count);
        log_phase_timing("search_place_ids", "clear_context", phase_started);

        let phase_started = Instant::now();
        collect_query_bigrams(&query, &mut ctx.query_bigrams);
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

        let query_bigram_count_u32 = query_bigram_count as u32;
        let min_match_count =
            (2 + query_bigram_count_u32 / (4 + query_bigram_count_u32 / 10)) as u16;

        let query_bigrams = ctx.query_bigrams.clone();
        let mut missing_bigrams = Vec::new();

        let phase_started = Instant::now();
        let cached_counts = ctx
            .query_cache
            .get_closest_sparse_ref(&query_bigrams, &mut missing_bigrams);
        if let Some(cached) = cached_counts {
            for &(idx, count) in cached.iter() {
                ctx.string_match_counts[idx as usize] = count;
                ctx.touched_string_indices.push(idx);
            }
        } else {
            // Already cleared sparsely in `clear`
        }
        let missing_bigram_count = missing_bigrams.len();
        log_phase_timing("search_place_ids", "cache_lookup_restore", phase_started);
        debug!(
            target: "cypress::query::timing",
            op = "search_place_ids",
            cache_hit = missing_bigram_count == 0,
            missing_bigram_count,
            "cache lookup summary"
        );

        let phase_started = Instant::now();
        for missing_idx in missing_bigrams.into_iter() {
            let start = db.bigram_offsets[missing_idx as usize] as usize;
            let end = db.bigram_offsets[(missing_idx + 1) as usize] as usize;

            for i in start..end {
                let string_idx = db.bigram_data[i] as usize;
                if ctx.string_match_counts[string_idx] == 0 {
                    ctx.touched_string_indices.push(string_idx as u32);
                }
                ctx.string_match_counts[string_idx] =
                    ctx.string_match_counts[string_idx].saturating_add(1);
            }
        }
        log_phase_timing("search_place_ids", "expand_missing_bigrams", phase_started);

        let phase_started = Instant::now();
        if !ctx.query_cache.has_exact(&query_bigrams) {
            let sparse_counts = Arc::new(
                ctx.touched_string_indices
                    .iter()
                    .map(|&idx| (idx, ctx.string_match_counts[idx as usize]))
                    .collect::<Vec<_>>(),
            );
            ctx.query_cache.put_sparse(&query_bigrams, sparse_counts);
        }
        log_phase_timing("search_place_ids", "cache_store_counts", phase_started);

        let phase_started = Instant::now();
        for i in 0..ctx.touched_string_indices.len() {
            let string_idx = ctx.touched_string_indices[i] as usize;
            let match_count = ctx.string_match_counts[string_idx];
            if (match_count as u16) < min_match_count {
                continue;
            }
            let str_bigram_count = db.string_bigram_counts[string_idx] as f32;
            if str_bigram_count == 0.0 {
                continue;
            }
            let cos_sim = (match_count as f32 * match_count as f32)
                / (str_bigram_count * query_bigram_count_u32 as f32);
            if cos_sim >= COS_SIM_CUTOFF {
                ctx.string_matches.push(CosSimMatch {
                    string_idx: string_idx as u32,
                    cos_sim,
                });
            }
        }
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

        let phase_started = Instant::now();
        let query_epoch = ctx.query_epoch;

        let match_snapshot: Vec<(u32, f32)> = ctx
            .string_matches
            .iter()
            .map(|m| (m.string_idx, m.cos_sim))
            .collect();

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

            let (match_score, matched_mask) = if is_multi_token {
                cypress::models::scoring::get_multi_token_match_score(string_name, &query_tokens)
            } else {
                let s = cypress::models::scoring::get_match_score(string_name, query_tokens[0]);
                let mask = if s != cypress::models::scoring::NO_MATCH {
                    1u8
                } else {
                    0u8
                };
                (s, mask)
            };

            if match_score == cypress::models::scoring::NO_MATCH {
                continue;
            }

            let p_start = db.string_to_places_offsets[sid] as usize;
            let p_end = db.string_to_places_offsets[sid + 1] as usize;

            for i in p_start..p_end {
                let place_id = db.string_to_places_data[i];
                let place_idx = place_id as usize;
                if place_idx >= place_count {
                    continue;
                }

                let layer_rank = db.place_layer_ranks[place_idx];
                let importance = db.place_importances[place_idx];
                let layer_bonus = cypress::models::scoring::get_layer_score_bonus(layer_rank);
                let importance_bonus = (importance * 3.0).clamp(0.0, 3.0);
                let place_bonus = 5.0_f32;

                let mut total_score = match_score;

                // Area matching for unmatched tokens
                if is_multi_token && place_idx < db.place_area_sets.len() {
                    let area_set_idx = db.place_area_sets[place_idx] as usize;
                    let (area_score, final_mask) = if area_set_idx as u32 == u32::MAX {
                        (0.0, matched_mask)
                    } else if let Some(&cached) =
                        ctx.area_match_scores.get(&(area_set_idx, matched_mask))
                    {
                        cached
                    } else {
                        let res = score_area_matches(db, area_set_idx, &query_tokens, matched_mask);
                        ctx.area_match_scores
                            .insert((area_set_idx, matched_mask), res);
                        res
                    };
                    total_score += area_score;

                    // Penalty for tokens that matched neither name nor area
                    let all_matched = final_mask == ((1u8 << query_tokens.len().min(8)) - 1);
                    if !all_matched {
                        for (t_idx, token) in query_tokens.iter().enumerate().take(8) {
                            if final_mask & (1 << t_idx) == 0 {
                                total_score += token.len() as f32 * 3.0;
                            }
                        }
                    }

                    // Bonus when all tokens matched
                    if all_matched {
                        total_score -= 2.5;
                    }
                }

                total_score -= layer_bonus;
                total_score -= importance_bonus;
                total_score -= place_bonus;

                let neg_score = -total_score;

                let current_best = if ctx.place_score_epochs[place_idx] == query_epoch {
                    ctx.place_best_scores[place_idx]
                } else {
                    f32::NEG_INFINITY
                };
                if neg_score <= current_best {
                    continue;
                }

                if ctx.place_score_epochs[place_idx] != query_epoch {
                    ctx.place_score_epochs[place_idx] = query_epoch;
                    ctx.touched_place_indices.push(place_id);
                }

                ctx.place_best_scores[place_idx] = neg_score;
            }
        }
        log_phase_timing("search_place_ids", "sift4_rescore_places", phase_started);

        let phase_started = Instant::now();
        results = ctx
            .touched_place_indices
            .iter()
            .map(|&place_id| (place_id, ctx.place_best_scores[place_id as usize] as f64))
            .collect();
        if results.len() > MAX_SCORED_MATCHES {
            results.select_nth_unstable_by(MAX_SCORED_MATCHES, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(MAX_SCORED_MATCHES);
        }
        log_phase_timing(
            "search_place_ids",
            "truncate_base_candidates",
            phase_started,
        );

        let phase_started = Instant::now();
        let mut rescored = Vec::with_capacity(results.len());
        for (place_id, base_score) in results.drain(..) {
            let place_idx = place_id as usize;
            if place_idx >= place_count {
                continue;
            }

            let lat = db.place_latitudes[place_idx] as f64;
            let lon = db.place_longitudes[place_idx] as f64;

            if let Some(bb) = bbox {
                if lon < bb[0] || lat < bb[1] || lon > bb[2] || lat > bb[3] {
                    continue;
                }
            }

            let mut score = base_score;
            if let Some(focus_point) = focus {
                let dist = haversine_distance_km(focus_point, (lat, lon));
                let decay = (-(dist * dist) / (2.0 * safe_focus_weight * safe_focus_weight)).exp();
                score *= decay;
            }

            rescored.push((place_id, score));
        }
        results = rescored;
        log_phase_timing(
            "search_place_ids",
            "apply_geographic_scoring",
            phase_started,
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
        log_phase_timing("search_place_ids", "final_sort_truncate", phase_started);
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
    let db = memdb_data.get_archived();

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

    for row in row_start..=row_end {
        for col in col_start..=col_end {
            let cell_id = (row * cols + col) as u32;

            // Binary search for this cell in the sorted active_cells array.
            if let Ok(pos) = db.active_cells.binary_search_by(|c| c.cmp(&cell_id)) {
                let start = db.cell_offsets[pos] as usize;
                let end = db.cell_offsets[pos + 1] as usize;

                for i in start..end {
                    let place_id = db.cell_places[i];
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
    let scored_ids = bigram_search(
        memdb,
        &params.text,
        focus,
        focus_weight,
        params.bbox,
        params.size,
    );
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
    let fetch_futures = scored_ids.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;
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
    let scored_ids = bigram_search(
        memdb,
        &params.text,
        focus,
        focus_weight,
        params.bbox,
        params.size,
    );
    log_phase_timing("execute_search_v2", "bigram_search", phase_started);
    debug!(
        target: "cypress::query::timing",
        op = "execute_search_v2",
        scored_ids = scored_ids.len(),
        "search candidates ready"
    );

    let mut admin_ids = std::collections::HashSet::new();

    let phase_started = Instant::now();
    let fetch_futures = scored_ids.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;
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
    let nearby = spatial_search(memdb, lon, lat, size, 1.0);
    log_phase_timing("execute_reverse", "spatial_search", phase_started);

    let phase_started = Instant::now();
    let fetch_futures = nearby.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;
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
    let nearby = spatial_search(memdb, lon, lat, size, 1.0);
    log_phase_timing("execute_reverse_v2", "spatial_search", phase_started);

    let phase_started = Instant::now();
    let fetch_futures = nearby.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;
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

fn place_to_search_result(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResult> {
    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| place.name.get(lang))
        .or_else(|| place.name.get("default"))
        .or_else(|| place.name.values().next())
        .cloned()
        .unwrap_or_default();

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
    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| place.name.get(lang))
        .or_else(|| place.name.get("default"))
        .or_else(|| place.name.values().next())
        .cloned()
        .unwrap_or_default();

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
    use cypress::models::normalized::{AdminHierarchyIds, NormalizedPlace};
    use cypress::models::place::{GeoPoint, Layer, OsmType};
    use std::collections::HashMap;

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
