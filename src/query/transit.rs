//! Request-specific transit policies. Suppression runs on hydrated candidates,
//! so an unavailable or filtered station can never erase a valid city result.
use cypress::models::{normalized::NormalizedPlace, place::Layer, transit::TransitKind};

#[derive(Clone, Copy, Debug, Default)]
pub struct TransitOptions {
    pub prefer_train_stations: bool,
    pub suppress_cities_near_stations: bool,
}

pub fn candidate_limit(size: usize, options: TransitOptions, filtered: bool) -> usize {
    if size == 0 {
        return 0;
    }
    if options.suppress_cities_near_stations || filtered {
        size.saturating_mul(4).max(40).min(160)
    } else {
        size
    }
}

pub fn finalize(
    mut places: Vec<(NormalizedPlace, f64)>,
    text: &str,
    size: usize,
    options: TransitOptions,
    layers: Option<&[String]>,
) -> Vec<(NormalizedPlace, f64)> {
    if let Some(layers) = layers {
        places.retain(|(p, _)| {
            let layer = serde_json::to_value(p.layer).expect("Layer serializes");
            layers.iter().any(|l| Some(l.as_str()) == layer.as_str())
        });
    }
    // Deterministic ties are also used in the mmap ranking stage.
    places.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then_with(|| a.0.source_id.cmp(&b.0.source_id))
    });
    let mut accepted: Vec<(NormalizedPlace, f64)> = Vec::with_capacity(size.min(places.len()));
    for (place, score) in places {
        if accepted.len() >= size {
            break;
        }
        if options.suppress_cities_near_stations
            && accepted.iter().any(|(station, station_score)| {
                *station_score > score && dominates(station, &place, text)
            })
        {
            continue;
        }
        accepted.push((place, score));
    }
    accepted
}

fn normalized(s: &str) -> String {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Strip only station designators, never distinguishing words such as North,
/// Central or Airport. Apply this to station aliases, not city names.
fn base_name(s: &str) -> String {
    let mut name = normalized(s);
    for prefix in ["gare de ", "gare d ", "gare ", "station ", "bahnhof "] {
        if let Some(rest) = name.strip_prefix(prefix) {
            name = rest.to_string();
            break;
        }
    }
    for suffix in [
        " railway station",
        " train station",
        " metro station",
        " tram stop",
        " station",
        " bahnhof",
        " gare",
    ] {
        if let Some(rest) = name.strip_suffix(suffix) {
            name = rest.to_string();
            break;
        }
    }
    name
}

fn similar(a: &str, b: &str) -> bool {
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if a == b {
        return true;
    }
    // At most one edit for long names, and identical token counts. Short names
    // and extra qualifiers require an exact match to avoid nearby false merges.
    if a.split_whitespace().count() != b.split_whitespace().count() {
        return false;
    }
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().min(b.len()) < 8 || a.len().abs_diff(b.len()) > 1 {
        return false;
    }
    let (mut i, mut j, mut edits) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            i += 1;
            j += 1;
            continue;
        }
        edits += 1;
        if edits > 1 {
            return false;
        }
        match a.len().cmp(&b.len()) {
            std::cmp::Ordering::Greater => i += 1,
            std::cmp::Ordering::Less => j += 1,
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    edits + usize::from(i < a.len() || j < b.len()) <= 1
}

fn same_context(station: &NormalizedPlace, area: &NormalizedPlace) -> bool {
    // Missing country/context is insufficient evidence for destructive dedup.
    if station.parent.country.is_none() || station.parent.country != area.parent.country {
        return false;
    }
    if let (Some(a), Some(b)) = (&station.parent.region, &area.parent.region) {
        if a != b {
            return false;
        }
    }
    let container = match area.layer {
        Layer::Locality => &station.parent.locality,
        Layer::Neighbourhood => &station.parent.neighbourhood,
        _ => return false,
    };
    if container.as_ref() == Some(&area.source_id) {
        return true;
    }
    if let (Some(a), Some(b)) = (&station.parent.locality, &area.parent.locality) {
        return a == b;
    }
    // place=* nodes have a different ID from the enclosing admin relation.
    station.parent.local_admin.is_some() && station.parent.local_admin == area.parent.local_admin
}

fn name_cost(name: &str, text: &str) -> f32 {
    let mut offsets = Vec::new();
    cypress::models::scoring::get_match_score(name.as_bytes(), text.as_bytes(), &mut offsets)
}

fn dominates(station: &NormalizedPlace, area: &NormalizedPlace, text: &str) -> bool {
    let kind = TransitKind::from_categories(&station.categories);
    let radius_km = match (kind, area.layer) {
        (TransitKind::Rail, Layer::Locality) => 3.0,
        (TransitKind::Metro | TransitKind::Tram, Layer::Neighbourhood) => 1.0,
        _ => return false,
    };
    if !same_context(station, area) {
        return false;
    }
    let distance = super::search::haversine_distance_km(
        (station.center_point.lat, station.center_point.lon),
        (area.center_point.lat, area.center_point.lon),
    );
    if !distance.is_finite() || distance > radius_km {
        return false;
    }
    let query = base_name(text);
    station.name.values().chain(station.phrase.iter()).any(|s| {
        let station_name = base_name(s);
        area.name.values().chain(area.phrase.iter()).any(|a| {
            let area_name = normalized(a);
            let station_cost = name_cost(&station_name, &query);
            similar(&station_name, &area_name)
                && station_cost != cypress::models::scoring::NO_MATCH
                && station_cost <= name_cost(&area_name, &query)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cypress::models::{
        normalized::AdminHierarchyIds,
        place::{GeoPoint, OsmType},
    };
    use std::collections::HashMap;

    fn place(id: &str, name: &str, layer: Layer, tags: &[&str]) -> NormalizedPlace {
        NormalizedPlace {
            source_id: id.into(),
            source_file: "fixture".into(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 1,
            wikidata_id: None,
            importance: None,
            population: None,
            layer,
            categories: tags.iter().map(|s| s.to_string()).collect(),
            name: HashMap::from([("default".into(), name.into())]),
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lat: 52.2,
                lon: 0.12,
            },
            bbox: None,
            parent: AdminHierarchyIds {
                country: Some("relation/uk".into()),
                locality: Some("relation/cambridge".into()),
                ..Default::default()
            },
        }
    }
    fn enabled() -> TransitOptions {
        TransitOptions {
            suppress_cities_near_stations: true,
            ..Default::default()
        }
    }
    #[test]
    fn suppresses_city_and_refills_but_options_are_independent() {
        let station = place(
            "node/1",
            "Cambridge Station",
            Layer::Venue,
            &["railway:station"],
        );
        let city = place("node/2", "Cambridge", Layer::Locality, &[]);
        let other = place("node/3", "Cambridge Museum", Layer::Venue, &[]);
        let candidates = vec![(station.clone(), 20.0), (city.clone(), 19.0), (other, 10.0)];
        let result = finalize(candidates.clone(), "Cambridge", 2, enabled(), None);
        assert_eq!(
            result
                .iter()
                .map(|x| x.0.source_id.as_str())
                .collect::<Vec<_>>(),
            ["node/1", "node/3"]
        );
        assert_eq!(
            finalize(
                candidates,
                "Cambridge",
                2,
                TransitOptions {
                    prefer_train_stations: true,
                    ..Default::default()
                },
                None
            )[1]
            .0
            .source_id,
            "node/2"
        );
        assert_eq!(
            finalize(
                vec![(station, 20.0), (city, 19.0)],
                "Cambridge",
                1,
                enabled(),
                None
            )
            .len(),
            1
        );
    }
    #[test]
    fn missing_filtered_or_lower_ranked_station_keeps_city() {
        let station = place(
            "node/1",
            "Cambridge Station",
            Layer::Venue,
            &["railway:station"],
        );
        let city = place("node/2", "Cambridge", Layer::Locality, &[]);
        assert_eq!(
            finalize(vec![(city.clone(), 19.0)], "Cambridge", 2, enabled(), None).len(),
            1
        );
        let filtered = finalize(
            vec![(station.clone(), 20.0), (city.clone(), 19.0)],
            "Cambridge",
            2,
            enabled(),
            Some(&["locality".into()]),
        );
        assert_eq!(filtered[0].0.source_id, "node/2");
        assert_eq!(
            finalize(
                vec![(station, 18.0), (city, 19.0)],
                "Cambridge",
                2,
                enabled(),
                None
            )
            .len(),
            2
        );
    }
    #[test]
    fn metro_and_tram_suppress_neighbourhoods_only() {
        for tags in [
            vec!["railway:station", "station:subway"],
            vec!["railway:tram_stop"],
        ] {
            let station = place("node/1", "Cambridge", Layer::Venue, &tags);
            let neighbourhood = place("node/2", "Cambridge", Layer::Neighbourhood, &[]);
            assert!(dominates(&station, &neighbourhood, "Cambridge"));
            let mut city = neighbourhood.clone();
            city.layer = Layer::Locality;
            assert!(!dominates(&station, &city, "Cambridge"));
            let mut far = neighbourhood;
            far.center_point.lat += 0.02;
            assert!(!dominates(&station, &far, "Cambridge"));
        }
        let bus = place("node/1", "Cambridge", Layer::Venue, &["highway:bus_stop"]);
        assert!(!dominates(
            &bus,
            &place("node/2", "Cambridge", Layer::Neighbourhood, &[]),
            "Cambridge"
        ));
    }
    #[test]
    fn rejects_qualified_remote_and_wrong_context_stations() {
        let city = place("node/2", "Cambridge", Layer::Locality, &[]);
        for name in ["Cambridge North", "Cambridge Airport", "Cambridge Central"] {
            assert!(!dominates(
                &place("node/1", name, Layer::Venue, &["railway:station"]),
                &city,
                "Cambridge"
            ));
        }
        let mut station = place("node/1", "Cambridge", Layer::Venue, &["railway:station"]);
        station.center_point.lat += 0.04;
        assert!(!dominates(&station, &city, "Cambridge"));
        station.center_point = city.center_point;
        station.parent.country = Some("relation/us".into());
        assert!(!dominates(&station, &city, "Cambridge"));
        station.parent.country = city.parent.country.clone();
        station.parent.locality = Some("relation/other".into());
        assert!(!dominates(&station, &city, "Cambridge"));
        station.parent.country = None;
        assert!(!dominates(&station, &city, "Cambridge"));
    }
    #[test]
    fn multilingual_aliases_and_conservative_fuzzy_names() {
        let mut station = place(
            "node/1",
            "Gare de Cambridge",
            Layer::Venue,
            &["railway:station"],
        );
        let mut city = place("node/2", "Other display name", Layer::Locality, &[]);
        city.name.insert("en".into(), "Cambridge".into());
        assert!(dominates(&station, &city, "Cambridge"));
        station.name.insert("default".into(), "Cambrigde".into());
        assert!(!dominates(&station, &city, "Cambridge")); // two edits, worse query match
        assert!(similar("cambridge", "cambrige"));
        assert!(!similar("bern", "berm"));
        assert!(!similar("cambridge", "cambridge north"));
    }
    #[test]
    fn pool_is_bounded_and_size_zero_is_empty() {
        assert_eq!(candidate_limit(40, enabled(), false), 160);
        assert_eq!(candidate_limit(1, enabled(), false), 40);
        assert_eq!(candidate_limit(0, enabled(), false), 0);
        assert_eq!(candidate_limit(10, TransitOptions::default(), false), 10);
    }
}
