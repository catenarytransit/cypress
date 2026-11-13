//! Station-level transit classification shared by ingest, compiler and query.
//! Values are persisted as a one-byte dense column; do not renumber them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum TransitKind {
    #[default]
    None = 0,
    Bus = 1,
    Tram = 2,
    Metro = 3,
    Rail = 4,
    Ferry = 5,
}

impl TransitKind {
    pub fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Bus,
            2 => Self::Tram,
            3 => Self::Metro,
            4 => Self::Rail,
            5 => Self::Ferry,
            _ => Self::None,
        }
    }

    pub fn from_tags(tags: &osmpbfreader::Tags) -> Self {
        classify(|key| tags.get(key).map(|s| s.as_str()))
    }

    pub fn from_categories(categories: &[String]) -> Self {
        classify(|key| {
            categories.iter().find_map(|s| {
                let (k, v) = s.split_once(':')?;
                (k == key).then_some(v)
            })
        })
    }

    /// A modest default hierarchy; the request option strengthens it. Text,
    /// administrative context and geographic relevance still affect ranking.
    pub fn bonus(self, prefer: bool) -> f32 {
        let rank = match self {
            Self::Rail => 4.0,
            Self::Metro => 3.0,
            Self::Tram => 2.0,
            Self::Bus => 1.0,
            _ => 0.0,
        };
        rank * if prefer { 2.0 } else { 0.25 }
    }
}

/// Exclude these even if they also carry amenity/address tags. In particular,
/// mode tags on a platform must never turn it into a station.
pub fn excluded<'a>(get: impl Fn(&str) -> Option<&'a str>) -> bool {
    get("operator") == Some("Disney Parks")
        || matches!(get("public_transport"), Some("platform" | "stop_position"))
        || matches!(
            get("railway"),
            Some(
                "platform"
                    | "stop"
                    | "subway_entrance"
                    | "proposed"
                    | "construction"
                    | "abandoned"
                    | "disused"
                    | "razed"
            )
        )
        || get("highway") == Some("platform")
        || ["disused", "abandoned", "proposed", "construction"]
            .iter()
            .any(|k| get(k) == Some("yes"))
}

fn classify<'a>(get: impl Fn(&str) -> Option<&'a str>) -> TransitKind {
    if excluded(&get) {
        return TransitKind::None;
    }
    let railway = get("railway");
    let station = get("station");
    let facility = matches!(railway, Some("station" | "halt" | "tram_stop" | "subway"))
        || matches!(
            station,
            Some("subway" | "light_rail" | "train" | "tram" | "monorail")
        )
        || get("public_transport") == Some("station")
        || matches!(get("amenity"), Some("bus_station" | "ferry_terminal"))
        || get("highway") == Some("bus_stop");
    if !facility {
        return TransitKind::None;
    }
    // Explicit mode tags take priority over the generic railway=station.
    // A shared mainline/metro station keeps its highest served mode.
    if get("train") == Some("yes") || station == Some("train") {
        TransitKind::Rail
    } else if matches!(station, Some("subway" | "light_rail" | "monorail"))
        || get("subway") == Some("yes")
        || get("light_rail") == Some("yes")
        || get("monorail") == Some("yes")
        || railway == Some("subway")
    {
        TransitKind::Metro
    } else if station == Some("tram") || get("tram") == Some("yes") || railway == Some("tram_stop")
    {
        TransitKind::Tram
    } else if get("amenity") == Some("ferry_terminal") || get("ferry") == Some("yes") {
        TransitKind::Ferry
    } else if get("highway") == Some("bus_stop")
        || get("amenity") == Some("bus_station")
        || get("bus") == Some("yes")
    {
        TransitKind::Bus
    } else if matches!(railway, Some("station" | "halt")) {
        TransitKind::Rail
    } else {
        // An unclassified public_transport station is not necessarily rail.
        TransitKind::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn modes_and_platform_exclusion() {
        for (tags, expected) in [
            (vec!["railway:station"], TransitKind::Rail),
            (vec!["railway:halt"], TransitKind::Rail),
            (
                vec!["railway:station", "station:subway"],
                TransitKind::Metro,
            ),
            (
                vec!["railway:station", "light_rail:yes"],
                TransitKind::Metro,
            ),
            (vec!["railway:tram_stop"], TransitKind::Tram),
            (vec!["highway:bus_stop"], TransitKind::Bus),
            (
                vec!["public_transport:station", "bus:yes"],
                TransitKind::Bus,
            ),
            (vec!["public_transport:station"], TransitKind::None),
            (
                vec!["railway:station", "public_transport:platform", "train:yes"],
                TransitKind::None,
            ),
            (
                vec!["railway:station", "public_transport:stop_position"],
                TransitKind::None,
            ),
            (vec!["railway:station", "disused:yes"], TransitKind::None),
        ] {
            assert_eq!(
                TransitKind::from_categories(
                    &tags.iter().map(|s| s.to_string()).collect::<Vec<_>>()
                ),
                expected
            );
        }
    }
    #[test]
    fn ordered_priors() {
        for prefer in [false, true] {
            assert!(TransitKind::Rail.bonus(prefer) > TransitKind::Metro.bonus(prefer));
            assert!(TransitKind::Metro.bonus(prefer) > TransitKind::Tram.bonus(prefer));
            assert!(TransitKind::Tram.bonus(prefer) > TransitKind::Bus.bonus(prefer));
        }
    }
}
