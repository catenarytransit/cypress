use anyhow::{Context, Result};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::info;

/// Load importance scores from Wikimedia CSV/SQL dump
pub fn load_importance(path: &Path) -> Result<HashMap<String, f64>> {
    info!("Loading importance data from {}", path.display());

    let file = File::open(path).context("Failed to open importance file")?;
    let reader: Box<dyn Read> = if path.extension().map_or(false, |e| e == "gz") {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };

    //schema

    //language        type    title   importance      wikidata_id
    //en      a       !!!     0.41523351747267634     Q371
    //en      a       $4.50_Theater_Company   0.42547726432748295     Q56348256
    //en      a       ¥$      0.40816634963515963     Q124493747
    //en      a       01099   0.2869812649426343      Q106604851
    //en      a       013     0.3807705141182751      Q2319846
    //en      a       015B    0.354703308323595       Q12579912
    //en      a       01.AI   0.31461790426364866     Q130692307
    //en      a       01_Communique   0.07194870560319186     Q4545554
    //en      a       01_Distribution 0.45833613469028417     Q1554656

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b'\t')
        .from_reader(reader);

    let mut map = HashMap::new();
    let headers = csv_reader.headers()?.clone();

    // Find column indices
    let importance_idx = headers
        .iter()
        .position(|h| h == "importance")
        .context("Column 'importance' not found")?;
    let wikidata_idx = headers
        .iter()
        .position(|h| h == "wikidata_id")
        .context("Column 'wikidata_id' not found")?;

    for result in csv_reader.records() {
        let record = result?;
        let wikidata_id = &record[wikidata_idx];
        let importance_str = &record[importance_idx];

        if let Ok(importance) = importance_str.parse::<f64>() {
            map.insert(wikidata_id.to_string(), importance);
        }
    }

    info!("Loaded {} importance scores", map.len());
    Ok(map)
}

/// Calculate default importance based on feature type (OSM tags)
/// See: Table 1: Default Importance by Feature Type
pub fn calculate_default_importance(tags: &osmpbfreader::Tags) -> f64 {
    // Continent / Ocean
    if tags.contains("place", "continent") || tags.contains("place", "ocean") {
        return 0.5;
    }
    // Sea
    if tags.contains("place", "sea") {
        return 0.4;
    }
    // Country
    if tags.contains("place", "country") {
        return 0.4;
    }
    // State / Region
    if tags.contains("place", "state") {
        return 0.3;
    }
    // Region (Sub-State)
    if tags.contains("place", "region") {
        return 0.25;
    }
    // County
    if tags.contains("place", "county") {
        return 0.2;
    }
    // City
    if tags.contains("place", "city") {
        return 0.2;
    }
    // Town
    if tags.contains("place", "town") {
        return 0.15;
    }
    // Village / Suburb
    if tags.contains("place", "village") || tags.contains("place", "suburb") {
        return 0.1;
    }
    // Hamlet / Farm
    if tags.contains("place", "hamlet") || tags.contains("place", "farm") {
        return 0.05;
    }
    // Locality
    if tags.contains("place", "locality") {
        return 0.05;
    }
    // Street (highway=residential in table, but we apply to all roads generally if not specific)
    // The table says "Street: highway=residential -> 0.100".
    // We'll map common highway tags to this.
    if tags.contains_key("highway") {
        let v = tags.get("highway").map(|s| s.as_str()).unwrap_or("");
        if v == "path" || v == "cycleway" || v == "footway" || v == "track" {
            return 0.075;
        }
        return 0.100;
    }

    // Restaurants / Shops / Common POIs
    if tags.contains_key("shop")
        || tags
            .get("amenity")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "restaurant" | "cafe" | "fast_food" | "bar" | "pub" | "marketplace"
                )
            })
            .unwrap_or(false)
    {
        return 0.05;
    }

    // House / POI
    // place=house or amenity=* etc.
    0.01
}
