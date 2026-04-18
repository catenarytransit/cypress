pub const POPULATION_COMPRESSION_FACTOR: u32 = 200;

pub fn parse_osm_population(value: &str) -> Option<u32> {
    let mut digits = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        }
    }

    if digits.is_empty() {
        return None;
    }

    digits.parse::<u32>().ok()
}

pub fn compress_population(population: Option<u32>) -> Option<u32> {
    population.map(|p| (p / POPULATION_COMPRESSION_FACTOR).min(u16::MAX as u32))
}

pub fn decompress_population(population: Option<u32>) -> Option<u32> {
    population.map(|p| p.saturating_mul(POPULATION_COMPRESSION_FACTOR))
}
