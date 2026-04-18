use rkyv::{Archive, Deserialize, Serialize};

pub const PLACE_RECORD_DISK_BYTES: usize = 64 + 1 + 128 + 1 + 4 + 4 + 4 + 1 + 4;

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct PlaceRecord {
    pub source_id_bytes: [u8; 64],
    pub source_id_len: u8,
    pub name_bytes: [u8; 128],
    pub name_len: u8,
    pub lat: f32,
    pub lon: f32,
    pub importance: f32,
    pub layer_rank: u8,
    pub population: u32,
}

impl PlaceRecord {
    pub fn parse_source_id(&self) -> String {
        String::from_utf8_lossy(&self.source_id_bytes[..self.source_id_len as usize]).into_owned()
    }

    pub fn parse_name(&self) -> String {
        String::from_utf8_lossy(&self.name_bytes[..self.name_len as usize]).into_owned()
    }

    pub fn from_disk_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PLACE_RECORD_DISK_BYTES {
            return None;
        }

        let mut offset = 0usize;

        let mut source_id_bytes = [0u8; 64];
        source_id_bytes.copy_from_slice(&bytes[offset..offset + 64]);
        offset += 64;

        let source_id_len = bytes[offset];
        offset += 1;

        let mut name_bytes = [0u8; 128];
        name_bytes.copy_from_slice(&bytes[offset..offset + 128]);
        offset += 128;

        let name_len = bytes[offset];
        offset += 1;

        let lat = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
        offset += 4;

        let lon = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
        offset += 4;

        let importance = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
        offset += 4;

        let layer_rank = bytes[offset];
        offset += 1;

        let population = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);

        Some(Self {
            source_id_bytes,
            source_id_len,
            name_bytes,
            name_len,
            lat,
            lon,
            importance,
            layer_rank,
            population,
        })
    }
}

/// A fully flattened, zero-copy architecture utilizing parallel arrays
/// and index-offset structures for O(1) traversal with 0 allocations.
#[derive(Archive, Serialize, Deserialize)]
pub struct CypressMemDb {
    // ==== Bigram Inverted Index ====
    pub string_bigram_counts: Vec<u8>,

    // We have 65536 possible bigrams.
    // bigram_offsets has len 65537.
    // The items for bigram `k` are bigram_data[bigram_offsets[k] .. bigram_offsets[k+1]]
    pub bigram_offsets: Vec<u32>,
    pub bigram_data: Vec<u32>,

    // string ID -> List of Place IDs
    pub string_to_places_offsets: Vec<u32>,
    pub string_to_places_data: Vec<u32>,

    // ==== Structure of Arrays (SoA) for Places ====
    pub place_latitudes: Vec<f32>,
    pub place_longitudes: Vec<f32>,
    pub place_importances: Vec<f32>,

    // For string IDs, use a contiguous byte array and parallel offset array
    pub place_source_id_offsets: Vec<u32>,
    pub place_source_id_bytes: Vec<u8>,

    pub place_layer_ranks: Vec<u8>,
    pub place_populations: Vec<u32>,

    // Normalized string text (lowercased) for SIFT4 re-scoring
    pub string_name_offsets: Vec<u32>,
    pub string_name_bytes: Vec<u8>,

    // ==== Area-Set Hierarchy (ADR-aligned) ====
    // Area name bytes: area_name_bytes[area_name_offsets[i]..area_name_offsets[i+1]]
    pub area_name_offsets: Vec<u32>,
    pub area_name_bytes: Vec<u8>,

    pub area_admin_levels: Vec<u8>,
    pub area_populations: Vec<u32>,

    // Area sets (deduplicated): area_set_data[area_set_offsets[i]..area_set_offsets[i+1]]
    pub area_set_offsets: Vec<u32>,
    pub area_set_data: Vec<u32>,

    // place_idx → area_set_idx
    pub place_area_sets: Vec<u32>,

    // ==== Sparse Spatial Grid ====
    // Sorted array of cell IDs that contain at least one place
    pub active_cells: Vec<u32>,
    pub cell_offsets: Vec<u32>,
    pub cell_places: Vec<u32>,
}

impl CypressMemDb {
    // 0.01 degree = approx 1.1km at equator
    pub const GRID_CELL_SIZE: f32 = 0.01;
    pub const GRID_COLS: usize = 36000;

    pub fn coord_to_cell(lat: f32, lon: f32) -> u32 {
        let lat = lat.clamp(-90.0, 89.999);
        let lon = lon.clamp(-180.0, 179.999);

        let row = ((lat + 90.0) / Self::GRID_CELL_SIZE) as usize;
        let col = ((lon + 180.0) / Self::GRID_CELL_SIZE) as usize;

        (row * Self::GRID_COLS + col) as u32
    }

    // Reverse bounds calculation for radius searching
    pub fn cell_to_bounds(cell: u32) -> (f32, f32, f32, f32) {
        let row = (cell as usize) / Self::GRID_COLS;
        let col = (cell as usize) % Self::GRID_COLS;

        let min_lat = (row as f32 * Self::GRID_CELL_SIZE) - 90.0;
        let min_lon = (col as f32 * Self::GRID_CELL_SIZE) - 180.0;
        (
            min_lat,
            min_lon,
            min_lat + Self::GRID_CELL_SIZE,
            min_lon + Self::GRID_CELL_SIZE,
        )
    }
}
