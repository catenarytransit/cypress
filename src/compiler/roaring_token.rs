//! Build Roaring-style mmap postings from the canonical multilingual token
//! dictionary. Hybrid u16-array / 65536-bit bitmap containers split by the
//! high 16 bits of StringId. No prior frequency-bucket physical reordering.
use anyhow::{anyhow, ensure, Result};
use cypress::models::roaring_token::{
    RoaringTokenIndexView, ARRAY_MAX, BITMAP_BYTES, CONTAINER_BYTES, FILE, HEADER_BYTES,
    MAGIC, META_BYTES, VERSION,
};
use cypress::models::token_index::{TokenIndexView, TOKEN_ANALYZER_VERSION, TOKEN_INDEX_HEADER_BYTES};
use fst::Streamer;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

pub struct Report {
    pub tokens: u64,
    pub postings: u64,
    pub arrays: u64,
    pub bitmaps: u64,
    pub bytes: u64,
}

fn encode_list(ids: &[u32]) -> Result<(Vec<u8>, u64, u64)> {
    ensure!(!ids.is_empty(), "empty postings are unexpected");
    ensure!(ids.windows(2).all(|w| w[0] < w[1]), "StringIds must increase");
    let mut bounds = Vec::new();
    let mut cursor = 0;
    while cursor < ids.len() {
        let high = (ids[cursor] >> 16) as u16;
        let start = cursor;
        while cursor < ids.len() && (ids[cursor] >> 16) as u16 == high {
            cursor += 1;
        }
        bounds.push((high, start, cursor));
    }
    let header = 4usize + bounds.len() * CONTAINER_BYTES;
    let mut out = Vec::with_capacity(header + ids.len() * 2);
    out.extend_from_slice(&u32::try_from(bounds.len())?.to_le_bytes());
    // Container offset refers to the beginning of this token's posting list,
    // which lets the read path access only the desired container.
    let mut data_offset = u32::try_from(header)?;
    let mut bodies = Vec::new();
    let mut array_count = 0u64;
    let mut bitmap_count = 0u64;
    for (high, start, end) in &bounds {
        let len = end - start;
        // Choose the smallest exact Roaring container for this 16-bit shard.
        // Run containers store [start, inclusive-extra-length] pairs.
        let mut runs = Vec::<(u16, u16)>::new();
        for &id in &ids[*start..*end] {
            let low = id as u16;
            if let Some((begin, extra)) = runs.last_mut() {
                if (*begin as u32) + (*extra as u32) + 1 == low as u32 {
                    *extra += 1;
                    continue;
                }
            }
            runs.push((low, 0));
        }
        let run_bytes = 2 + runs.len() * 4;
        let array_bytes = len * 2;
        let dense = len > ARRAY_MAX && BITMAP_BYTES <= run_bytes;
        let run_encoded = runs.len() <= u16::MAX as usize
            && run_bytes < array_bytes.min(BITMAP_BYTES);
        let kind = if run_encoded { 2 } else if dense || (len > ARRAY_MAX && !run_encoded) { 1 } else { 0 };
        out.extend_from_slice(&high.to_le_bytes());
        out.push(kind);
        out.push(0);
        out.extend_from_slice(&u32::try_from(len)?.to_le_bytes());
        out.extend_from_slice(&data_offset.to_le_bytes());
        if kind == 2 {
            bodies.extend_from_slice(&(runs.len() as u16).to_le_bytes());
            for (begin, extra) in runs {
                bodies.extend_from_slice(&begin.to_le_bytes());
                bodies.extend_from_slice(&extra.to_le_bytes());
            }
            data_offset = data_offset.checked_add(u32::try_from(run_bytes)?)
                .ok_or_else(|| anyhow!("run offset overflow"))?;
        } else if kind == 1 {
            bitmap_count += 1;
            let mut bitmap = [0u8; BITMAP_BYTES];
            for &id in &ids[*start..*end] {
                let low = id as u16 as usize;
                bitmap[low >> 3] |= 1 << (low & 7);
            }
            bodies.extend_from_slice(&bitmap);
            data_offset = data_offset.checked_add(BITMAP_BYTES as u32).ok_or_else(|| anyhow!("bitmap offset overflow"))?;
        } else {
            array_count += 1;
            for &id in &ids[*start..*end] {
                bodies.extend_from_slice(&(id as u16).to_le_bytes());
            }
            data_offset = data_offset.checked_add(u32::try_from(len * 2)?).ok_or_else(|| anyhow!("array offset overflow"))?;
        }
    }
    ensure!(out.len() == header, "container header count mismatch");
    out.extend_from_slice(&bodies);
    ensure!(out.len() == data_offset as usize, "container offset mismatch");
    Ok((out, array_count, bitmap_count))
}

pub fn build(source: &Path, destination: &Path, scratch: &Path) -> Result<Report> {
    let file = File::open(source)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let index = TokenIndexView::parse(&mmap)?;
    // FST retains original Unicode term ordering and all canonical StringIds.
    let fst_len = usize::try_from(u64::from_le_bytes(mmap[48..56].try_into()?))?;
    let fst_start = TOKEN_INDEX_HEADER_BYTES;
    let fst_end = fst_start.checked_add(fst_len).ok_or_else(|| anyhow!("FST overflow"))?;
    let fst_bytes = mmap.get(fst_start..fst_end).ok_or_else(|| anyhow!("FST truncated"))?;
    let stage = scratch.join("roaring-tokens-stage.bin");
    let mut payload = BufWriter::new(File::create(&stage)?);
    let mut offsets = Vec::with_capacity(usize::try_from(index.token_count)?);
    let mut byte_position = 0u64;
    let mut token_count = 0u64;
    let mut posting_count = 0u64;
    let mut arrays = 0;
    let mut bitmaps = 0;
    let mut terms = index.fst.stream();
    while let Some((token, ordinal)) = terms.next() {
        ensure!(ordinal == token_count, "token ordinal out of order");
        let term = std::str::from_utf8(token)?;
        let posting = index.lookup(term)?.ok_or_else(|| anyhow!("missing term"))?;
        let df = posting.document_frequency;
        let ids = posting.decode_ids()?;
        let (bytes, a, b) = encode_list(&ids)?;
        offsets.push((byte_position, u32::try_from(bytes.len())?, df));
        payload.write_all(&bytes)?;
        byte_position = byte_position.checked_add(bytes.len() as u64).ok_or_else(|| anyhow!("payload length overflow"))?;
        posting_count += ids.len() as u64;
        arrays += a;
        bitmaps += b;
        token_count += 1;
    }
    payload.flush()?;
    drop(payload);
    ensure!(token_count == index.token_count && posting_count == index.posting_count,
            "incomplete posting conversion");
    let temp = destination.with_extension("roaring.tmp");
    {
        let mut out = BufWriter::new(File::create(&temp)?);
        out.write_all(MAGIC)?;
        out.write_all(&VERSION.to_le_bytes())?;
        out.write_all(&TOKEN_ANALYZER_VERSION.to_le_bytes())?;
        for number in [
            index.string_count, index.fingerprint, token_count, posting_count,
            fst_len as u64, byte_position,
        ] {
            out.write_all(&number.to_le_bytes())?;
        }
        out.write_all(fst_bytes)?;
        for (start, size, df) in offsets {
            out.write_all(&start.to_le_bytes())?;
            out.write_all(&size.to_le_bytes())?;
            out.write_all(&df.to_le_bytes())?;
        }
        let count = std::io::copy(&mut BufReader::new(File::open(&stage)?), &mut out)?;
        ensure!(count == byte_position, "incomplete posting arena");
        out.flush()?;
    }
    let result = File::open(&temp)?;
    let mapped = unsafe { memmap2::Mmap::map(&result)? };
    let view = RoaringTokenIndexView::parse(&mapped)?;
    ensure!(view.token_count == token_count && view.fingerprint == index.fingerprint,
            "compiled index generation mismatch");
    drop(mapped);
    drop(result);
    fs::rename(&temp, destination)?;
    fs::remove_file(stage)?;
    let bytes = fs::metadata(destination)?.len();
    ensure!(bytes >= HEADER_BYTES as u64 + token_count * META_BYTES as u64, "invalid serialized index size");
    Ok(Report { tokens: token_count, postings: posting_count, arrays, bitmaps, bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token_index::build_token_index;

    #[test]
    fn array_and_bitmap_container_membership() {
        let sparse = [1u32, 7, 100, 65538, 65550];
        let (bytes, arrays, bitmaps) = encode_list(&sparse).unwrap();
        assert_eq!((arrays,bitmaps), (2,0));
        assert_eq!(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 2);
        let dense: Vec<u32> = (0..8194).filter(|n| n % 2 == 0).collect();
        let (bytes, arrays, bitmaps) = encode_list(&dense).unwrap();
        assert_eq!((arrays,bitmaps), (0,1));
        assert_eq!(bytes.len(), 4 + CONTAINER_BYTES + BITMAP_BYTES);
    }
    #[test]
    fn complete_bitmap_sparse_and_cross_shard_roundtrip() {
        use cypress::models::roaring_token::{MAGIC, VERSION};
        use cypress::models::token_index::TOKEN_ANALYZER_VERSION;
        use fst::MapBuilder;
        let mut ids: Vec<u32> = (0..8194).filter(|n| n % 2 == 0).collect();
        ids.extend([65539, 65586]);
        let (encoded, arrays, bitmaps) = encode_list(&ids).unwrap();
        assert_eq!((arrays, bitmaps), (1, 1));
        let mut fst_builder = MapBuilder::memory();
        fst_builder.insert("common", 0).unwrap();
        let fst = fst_builder.into_inner().unwrap();
        let names = b"common";
        let offsets = [0u32, 6];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&TOKEN_ANALYZER_VERSION.to_le_bytes());
        for value in [
            1u64,
            cypress::models::token_index::dictionary_fingerprint(names, &offsets),
            1,
            ids.len() as u64,
            fst.len() as u64,
            encoded.len() as u64,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&fst);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&encoded);
        let view = RoaringTokenIndexView::parse(&bytes).unwrap();
        assert!(view.matches_dictionary(names, &offsets));
        let p = view.lookup("common").unwrap().unwrap();
        for id in [0, 250, 4096, 65539, 65586] {
            assert!(p.contains(id), "missing included id {id}");
        }
        for id in [4097, 5001, 65538, 65587, 131073] {
            assert!(!p.contains(id), "unexpected id {id}");
        }
        assert_eq!(p.anchor_ids(), ids);
        // Missing highs, dense/array boundary, and exhausted cursor.
        let mut candidates = vec![0, 4096, 4097, 65538, 65539, 65586, 65587, 131073];
        let expected: Vec<_> = candidates.iter().copied().filter(|id| p.contains(*id)).collect();
        assert_eq!(p.retain_sorted(&mut candidates), 8);
        assert_eq!(candidates, expected);
        assert_eq!(p.retain_sorted(&mut Vec::new()), 0);
    }

    #[test]
    fn run_container_compresses_ranges_and_checks_boundaries() {
        let ids: Vec<u32> = (100..700).chain(65530..65536).chain(65538..65547).collect();
        let (encoded, arrays, bitmaps) = encode_list(&ids).unwrap();
        assert_eq!((arrays, bitmaps), (0, 0), "two run containers");
        assert_eq!(encoded[6], 2);
        let mut fst_builder = fst::MapBuilder::memory();
        fst_builder.insert("run", 0).unwrap();
        let fst = fst_builder.into_inner().unwrap();
        let names = b"run";
        let offsets = [0u32, 3];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&TOKEN_ANALYZER_VERSION.to_le_bytes());
        for number in [1u64,
            cypress::models::token_index::dictionary_fingerprint(names, &offsets),
            1, ids.len() as u64, fst.len() as u64, encoded.len() as u64] {
            bytes.extend_from_slice(&number.to_le_bytes());
        }
        bytes.extend_from_slice(&fst);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&encoded);
        let view = RoaringTokenIndexView::parse(&bytes).unwrap();
        let posting = view.lookup("run").unwrap().unwrap();
        assert_eq!(posting.anchor_ids(), ids);
        for id in [99,100,699,700,65529,65530,65535,65536,65537,65538,65546,65547] {
            assert_eq!(posting.contains(id), ids.binary_search(&id).is_ok(), "id {id}");
        }
    }
    #[test]
    fn multilingual_roundtrip() {
        let names = ["berlin", "berlin hauptbahnhof", "paris", "zürich", "東京駅"];
        let mut data = Vec::new();
        let mut offsets = vec![0];
        for name in names {
            data.extend_from_slice(name.as_bytes());
            offsets.push(data.len() as u32);
        }
        let temp = tempfile::tempdir().unwrap();
        let tokens = temp.path().join("v1.bin");
        build_token_index(&offsets, &data, temp.path(), &tokens).unwrap();
        let target = temp.path().join(FILE);
        let report = build(&tokens, &target, temp.path()).unwrap();
        assert_eq!(report.postings, 6);
        let bytes = fs::read(&target).unwrap();
        let view = RoaringTokenIndexView::parse(&bytes).unwrap();
        assert!(view.matches_dictionary(&data, &offsets));
        let station = view.lookup("berlin").unwrap().unwrap();
        assert_eq!(station.anchor_ids(), [0, 1]);
        assert!(station.contains(0));
        assert!(!station.contains(2));
        assert_eq!(view.lookup("東京駅").unwrap().unwrap().anchor_ids(), [4]);
    }
}
