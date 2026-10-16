//! Exact administrative suffix joins for selective token retrieval.
//!
//! Token postings refer to a single StringId, whereas a query can match a
//! place name and an administrative ancestor using different strings. This
//! module implements a bounded query-time join without another compiled index.
//!
//! It deliberately requires an EXACT match in the multilingual admin alias
//! FST. Prefix/fuzzy administrative interpretations remain in legacy search.
//! A StringId is retained only if at least one of its places or street groups
//! belongs to an area named by the suffix; the existing scoring DP still
//! makes the final semantic and geographic ranking decision.
use fst::Map;

const MAX_AMBIGUOUS_AREAS: usize = 128;
const MAX_JOIN_LINKS: usize = 250_000;

/// Views of existing mmap-backed Cypress arrays. All names use existing IDs.
pub struct JoinIndex<'a> {
    pub string_place_offsets: &'a [u32],
    pub string_place_ids: &'a [u32],
    pub place_area_sets: &'a [u32],
    pub string_street_offsets: &'a [u32],
    pub string_street_groups: &'a [u32],
    pub street_area_sets: &'a [u32],
    pub area_set_offsets: &'a [u32],
    pub area_set_data: &'a [u32],
}

#[derive(Default, Debug)]
pub struct JoinStats {
    pub examined_links: usize,
    pub matching_strings: usize,
    pub name_matches: usize,
    pub street_matches: usize,
}

pub fn suffix_splits(tokens: &[&[u8]]) -> Vec<(Vec<u8>, Vec<u8>)> {
    // Contiguous name prefix and admin suffix; no assumption that only the
    // final word can be an administrative area ("New York", "Los Angeles").
    if tokens.len() < 2 || tokens.len() > 8 {
        return Vec::new();
    }
    (1..tokens.len())
        .rev()
        .map(|split| {
            (
                tokens[..split].join(&[b' '][..]),
                tokens[split..].join(&[b' '][..]),
            )
        })
        .collect()
}

/// Exact lookup, not an administrative prefix scan. An alias can map to
/// multiple real-world areas ("Georgia", "Springfield", "Berlin").
pub fn exact_admin_areas(
    fst: &Map<&[u8]>,
    offsets: &[u32],
    data: &[u32],
    suffix: &[u8],
) -> Option<Vec<u32>> {
    let group = usize::try_from(fst.get(suffix)?).ok()?;
    let start = *offsets.get(group)? as usize;
    let end = *offsets.get(group + 1)? as usize;
    if start >= end || end > data.len() || end - start > MAX_AMBIGUOUS_AREAS {
        return None;
    }
    let mut areas = data[start..end].to_vec();
    areas.sort_unstable();
    areas.dedup();
    Some(areas)
}

pub fn in_area(index: &JoinIndex<'_>, set_id: u32, candidate_areas: &[u32]) -> bool {
    if set_id == u32::MAX {
        return false;
    }
    let idx = set_id as usize;
    let (Some(&start), Some(&end)) = (
        index.area_set_offsets.get(idx),
        index.area_set_offsets.get(idx + 1),
    ) else {
        return false;
    };
    let (start, end) = (start as usize, end as usize);
    if start > end || end > index.area_set_data.len() {
        return false;
    }
    let members = &index.area_set_data[start..end];
    // Area membership arrays were sorted/deduplicated by the compiler.
    candidate_areas
        .iter()
        .any(|area| members.binary_search(area).is_ok())
}

/// Filter name StringIds against a geographic area without scanning all
/// places worldwide. A matching StringId may represent several places; the
/// existing scorer still evaluates the precise place/area association.
/// Return None on excessive fanout rather than silently truncating results.
pub fn retain_joined_strings(
    index: &JoinIndex<'_>,
    string_ids: &[u32],
    candidate_areas: &[u32],
) -> Option<(Vec<u32>, JoinStats)> {
    if candidate_areas.is_empty() {
        return Some((Vec::new(), JoinStats::default()));
    }
    let mut filtered = Vec::new();
    let mut stats = JoinStats::default();
    for &sid in string_ids {
        let sid = sid as usize;
        let (Some(&place_start), Some(&place_end)) = (
            index.string_place_offsets.get(sid),
            index.string_place_offsets.get(sid + 1),
        ) else {
            continue;
        };
        let (place_start, place_end) = (place_start as usize, place_end as usize);
        if place_start > place_end || place_end > index.string_place_ids.len() {
            continue;
        }
        let street_range = (
            index.string_street_offsets.get(sid),
            index.string_street_offsets.get(sid + 1),
        );
        let (street_start, street_end) = match street_range {
            (Some(&start), Some(&end))
                if start <= end && (end as usize) <= index.string_street_groups.len() =>
            {
                (start as usize, end as usize)
            }
            _ => (0, 0),
        };

        let links = (place_end - place_start).checked_add(street_end - street_start)?;
        stats.examined_links = stats.examined_links.checked_add(links)?;
        if stats.examined_links > MAX_JOIN_LINKS {
            return None;
        }

        let name_match = index.string_place_ids[place_start..place_end]
            .iter()
            .any(|&place_id| {
                index
                    .place_area_sets
                    .get(place_id as usize)
                    .is_some_and(|&set| in_area(index, set, candidate_areas))
            });
        let street_match = index.string_street_groups[street_start..street_end]
            .iter()
            .any(|&group| {
                index
                    .street_area_sets
                    .get(group as usize)
                    .is_some_and(|&set| in_area(index, set, candidate_areas))
            });
        if name_match || street_match {
            stats.matching_strings += 1;
            stats.name_matches += usize::from(name_match);
            stats.street_matches += usize::from(street_match);
            filtered.push(sid as u32);
        }
    }
    filtered.sort_unstable();
    filtered.dedup();
    Some((filtered, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::MapBuilder;

    #[test]
    fn recognizes_multitoken_admin_suffix() {
        let tokens = [&b"central"[..], &b"park"[..], &b"new"[..], &b"york"[..]];
        let variants = suffix_splits(&tokens);
        assert!(variants.contains(&(b"central park".to_vec(), b"new york".to_vec())));
        assert!(variants.contains(&(b"central park new".to_vec(), b"york".to_vec())));
    }

    #[test]
    fn resolves_ambiguous_aliases_exactly() {
        let mut builder = MapBuilder::memory();
        builder.insert("berlin", 0).unwrap();
        builder.insert("münchen", 1).unwrap();
        let bytes = builder.into_inner().unwrap();
        let fst = Map::new(bytes.as_slice()).unwrap();
        assert_eq!(
            exact_admin_areas(&fst, &[0, 2, 3], &[5, 7, 9], b"berlin"),
            Some(vec![5, 7])
        );
        assert_eq!(
            exact_admin_areas(&fst, &[0, 2, 3], &[5, 7, 9], "münchen".as_bytes()),
            Some(vec![9])
        );
        assert!(exact_admin_areas(&fst, &[0, 2, 3], &[5, 7, 9], b"berl").is_none());
        assert!(exact_admin_areas(&fst, &[0, 2, 3], &[5, 7, 9], b"munchen").is_none());
    }

    #[test]
    fn joins_name_and_street_fields_via_area_sets() {
        // String 0="brandenburger tor" at place 0 (Berlin).
        // String 1="brandenburger tor" at place 1 (another city).
        // String 2 is a street group in Berlin, with no direct place.
        let index = JoinIndex {
            string_place_offsets: &[0, 1, 2, 2],
            string_place_ids: &[0, 1],
            place_area_sets: &[0, 1],
            string_street_offsets: &[0, 0, 0, 1],
            string_street_groups: &[0],
            street_area_sets: &[0],
            area_set_offsets: &[0, 1, 2],
            area_set_data: &[5, 6],
        };
        let (ids, stats) = retain_joined_strings(&index, &[0, 1, 2], &[5]).unwrap();
        assert_eq!(ids, vec![0, 2]);
        assert_eq!(stats.name_matches, 1);
        assert_eq!(stats.street_matches, 1);
        assert_eq!(stats.examined_links, 3);
        assert_eq!(
            retain_joined_strings(&index, &[0, 1, 2], &[100])
                .unwrap()
                .0
                .len(),
            0
        );
    }

    #[test]
    fn rejects_unbounded_fanout_without_truncation() {
        let all_places = vec![0u32; MAX_JOIN_LINKS + 1];
        let index = JoinIndex {
            string_place_offsets: &[0, (MAX_JOIN_LINKS + 1) as u32],
            string_place_ids: &all_places,
            place_area_sets: &[0],
            string_street_offsets: &[0, 0],
            string_street_groups: &[],
            street_area_sets: &[],
            area_set_offsets: &[0, 1],
            area_set_data: &[5],
        };
        assert!(retain_joined_strings(&index, &[0], &[5]).is_none());
    }
}
