use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct PlaceRecord {
    pub source_id_bytes: [u8; 64],
    pub source_id_len: u8,
    pub name_bytes: [u8; 128],
    pub name_len: u8,
    pub lat: f32,
    pub lon: f32,
    pub importance: f32,
    pub layer_rank: u8, // Useful for filtering
}

impl PlaceRecord {
    pub fn parse_source_id(&self) -> String {
        String::from_utf8_lossy(&self.source_id_bytes[..self.source_id_len as usize]).into_owned()
    }

    pub fn parse_name(&self) -> String {
        String::from_utf8_lossy(&self.name_bytes[..self.name_len as usize]).into_owned()
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

    // ==== Places and Entity Resolution ====
    pub places: Vec<PlaceRecord>,

    // string ID -> List of Place IDs
    pub string_to_places_offsets: Vec<u32>,
    pub string_to_places_data: Vec<u32>,

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
