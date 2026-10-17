//! Optional place-level scoping of globally repeated exact names.
//!
//! In "starbucks chicago", the StringId for "starbucks" can expand to many
//! thousands of PlaceIds, even though an exact Chicago alias is available.
//! Restrict that *specific name StringId* to area-compatible places before
//! name evidence and DP scoring. The full literal query and other candidates
//! remain eligible. All filtering is opt-in; shadow mode changes no results.

use std::collections::HashMap;
use fst::Map;
use super::field_aware::{self, JoinIndex};

/// Low-fanout names do not justify a restrictive geographic interpretation.
pub const MIN_SCOPED_NAME_FANOUT: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off,
    Shadow,
    On,
}

impl Mode {
    pub fn from_env(value: &str) -> Self {
        match value {
            "shadow" => Self::Shadow,
            "on" => Self::On,
            _ => Self::Off,
        }
    }
}

struct Gate {
    areas: Vec<u32>,
    cached_sets: HashMap<u32, bool>,
}

impl Gate {
    fn matches(&mut self, area_set: u32, index: &JoinIndex<'_>) -> bool {
        *self.cached_sets.entry(area_set).or_insert_with(||
            field_aware::in_area(index, area_set, &self.areas)
        )
    }
}

#[derive(Default)]
pub struct Plan {
    gates: HashMap<u32, Gate>,
    /// Exact, selected name + administrative alias interpretations.
    pub interpretations: usize,
    /// Place mappings consulted to ensure each gate has a real local match.
    pub preflight_links: usize,
}

impl Plan {
    pub fn gated_names(&self) -> usize { self.gates.len() }

    /// None: the StringId has no geographic constraint.
    /// Some(false): may exclude this mapping in "on" mode only.
    pub fn check(&mut self, string_id: u32, area_set: u32, index: &JoinIndex<'_>) -> Option<bool> {
        self.gates.get_mut(&string_id)
            .map(|gate| gate.matches(area_set, index))
    }
}

/// Build exact name/area partitions in either order. An alias can contain
/// multiple tokens; a name or area can occur first. Only selected exact name
/// StringIds with at least 64 place mappings may be gated.
///
/// Require a real area-matching place for each gate. If the alias is unknown,
/// its target is absent, or no matching PlaceId is found, leave the broad name
/// untouched. Other candidate StringIds are never restricted by this plan.
pub fn build(
    name_fst: &Map<&[u8]>,
    area_fst: &Map<&[u8]>,
    alias_offsets: &[u32],
    alias_data: &[u32],
    tokens: &[&[u8]],
    selected_string_ids: &[u32], // sorted, deduplicated
    index: &JoinIndex<'_>,
) -> Plan {
    let mut plan = Plan::default();
    if !(2..=8).contains(&tokens.len()) {
        return plan;
    }

    let mut candidates = HashMap::<u32, Vec<u32>>::new();
    for split in 1..tokens.len() {
        let lhs = tokens[..split].join(&[b' '][..]);
        let rhs = tokens[split..].join(&[b' '][..]);
        for (name, admin) in [(&lhs[..], &rhs[..]), (&rhs[..], &lhs[..])] {
            let Some(raw_sid) = name_fst.get(name) else { continue; };
            let Ok(sid) = u32::try_from(raw_sid) else { continue; };
            if selected_string_ids.binary_search(&sid).is_err() { continue; }

            let pos = sid as usize;
            let (Some(&start), Some(&end)) = (
                index.string_place_offsets.get(pos),
                index.string_place_offsets.get(pos + 1),
            ) else { continue; };
            if start > end || (end as usize) > index.string_place_ids.len()
                || ((end - start) as usize) < MIN_SCOPED_NAME_FANOUT
            {
                continue;
            }
            let Some(areas) = field_aware::exact_admin_areas(
                area_fst, alias_offsets, alias_data, admin,
            ) else { continue; };
            plan.interpretations += 1;
            candidates.entry(sid).or_default().extend(areas);
        }
    }

    for (sid, mut areas) in candidates {
        areas.sort_unstable();
        areas.dedup();
        let pos = sid as usize;
        let (Some(&start), Some(&end)) = (
            index.string_place_offsets.get(pos),
            index.string_place_offsets.get(pos + 1),
        ) else { continue; };
        if start > end { continue; }
        let Some(postings) = index.string_place_ids.get(start as usize..end as usize) else {
            continue;
        };

        let mut gate = Gate { areas, cached_sets: HashMap::new() };
        let any_in_area = postings.iter().any(|&place_id| {
            plan.preflight_links += 1;
            index.place_area_sets.get(place_id as usize)
                .is_some_and(|&set| gate.matches(set, index))
        });
        if any_in_area {
            plan.gates.insert(sid, gate);
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::MapBuilder;

    fn fixture() -> (Vec<u8>, Vec<u8>, Vec<u32>, Vec<u32>, Vec<u32>) {
        let mut names = MapBuilder::memory();
        names.insert("chicago starbucks", 0).unwrap();
        names.insert("starbucks", 1).unwrap();
        names.insert("starbucks chicago", 2).unwrap();
        let mut aliases = MapBuilder::memory();
        aliases.insert("chicago", 0).unwrap();
        aliases.insert("new york", 1).unwrap();
        (
            names.into_inner().unwrap(),
            aliases.into_inner().unwrap(),
            vec![0, 1, 2],
            vec![55, 77],
            (0..MIN_SCOPED_NAME_FANOUT as u32).collect(),
        )
    }

    #[test]
    fn exact_name_scoped_in_both_orders_but_literal_names_unrestricted() {
        let (names, aliases, offsets, areas, postings) = fixture();
        let name_fst = Map::new(names.as_slice()).unwrap();
        let area_fst = Map::new(aliases.as_slice()).unwrap();
        let count = postings.len();
        let mut memberships = vec![1; count];
        memberships[0] = 0;
        let positions = [0, 0, count as u32, count as u32];
        let index = JoinIndex {
            string_place_offsets: &positions,
            string_place_ids: &postings,
            place_area_sets: &memberships,
            string_street_offsets: &[0, 0, 0, 0],
            string_street_groups: &[],
            street_area_sets: &[],
            area_set_offsets: &[0, 1, 2],
            area_set_data: &[55, 88],
        };
        let mut plan = build(
            &name_fst, &area_fst, &offsets, &areas,
            &[b"starbucks", b"chicago"], &[1, 2], &index,
        );
        assert_eq!(plan.gated_names(), 1);
        assert_eq!(plan.check(1, 0, &index), Some(true));
        assert_eq!(plan.check(1, 1, &index), Some(false));
        assert_eq!(plan.check(2, 1, &index), None);
        assert_eq!(plan.check(0, 1, &index), None);
        let mut reverse = build(
            &name_fst, &area_fst, &offsets, &areas,
            &[b"chicago", b"starbucks"], &[1], &index,
        );
        assert_eq!(reverse.check(1, 0, &index), Some(true));
        assert_eq!(reverse.check(1, 1, &index), Some(false));
    }

    #[test]
    fn no_local_match_or_incomplete_alias_keeps_broad_search() {
        let (names, aliases, offsets, areas, postings) = fixture();
        let name_fst = Map::new(names.as_slice()).unwrap();
        let area_fst = Map::new(aliases.as_slice()).unwrap();
        let count = postings.len();
        let memberships = vec![1; count];
        let positions = [0, 0, count as u32, count as u32];
        let index = JoinIndex {
            string_place_offsets: &positions,
            string_place_ids: &postings,
            place_area_sets: &memberships,
            string_street_offsets: &[0, 0, 0, 0],
            string_street_groups: &[],
            street_area_sets: &[],
            area_set_offsets: &[0, 1, 2],
            area_set_data: &[55, 88],
        };
        assert_eq!(build(&name_fst, &area_fst, &offsets, &areas,
            &[b"starbucks", b"chicago"], &[1], &index).gated_names(), 0);
        assert_eq!(build(&name_fst, &area_fst, &offsets, &areas,
            &[b"starbucks", b"chic"], &[1], &index).gated_names(), 0);
        assert_eq!(build(&name_fst, &area_fst, &offsets, &areas,
            &[b"starbucks", b"chicago"], &[], &index).gated_names(), 0);
    }

    #[test]
    fn mode_defaults_off() {
        assert_eq!(Mode::from_env(""), Mode::Off);
        assert_eq!(Mode::from_env("shadow"), Mode::Shadow);
        assert_eq!(Mode::from_env("on"), Mode::On);
    }
}
