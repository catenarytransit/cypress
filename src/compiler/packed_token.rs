//! Experimental frequency-bucketed posting pack compiler.
//!
//! Converts the canonical, generation-bound multilingual token dictionary into
//! fixed-width, mmap-addressable StringId posting arrays. Physical disk order
//! is by document-frequency bucket; the FST and metadata map original lexical
//! term ordinals to each bucket position. This tests a genuinely different
//! index layout, not a query-time FST prefix-expansion loop.
use anyhow::{anyhow, ensure, Result};
use cypress::models::packed_token::{
    PackedTokenIndexView, PACK_FILE, PACK_HEADER_BYTES, PACK_MAGIC, PACK_VERSION,
};
use cypress::models::token_index::{
    TokenIndexView, TOKEN_ANALYZER_VERSION,
};
use fst::Streamer;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

const BUCKETS: usize = 8;
const PAGE_ALIGN: u64 = 4096;

fn frequency_bucket(df: u32) -> usize {
    match df {
        0..=8 => 0,
        9..=32 => 1,
        33..=128 => 2,
        129..=512 => 3,
        513..=2048 => 4,
        2049..=8192 => 5,
        8193..=32768 => 6,
        _ => 7,
    }
}

fn align(value: u64) -> u64 {
    (value + PAGE_ALIGN - 1) & !(PAGE_ALIGN - 1)
}

pub struct PackReport {
    pub tokens: usize,
    pub postings: u64,
    pub file_bytes: u64,
    pub bucket_bytes: [u64; BUCKETS],
}

pub fn build_from_token_index(source: &Path, output: &Path, scratch: &Path) -> Result<PackReport> {
    let file = File::open(source)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let index = TokenIndexView::parse(&mmap)?;
    let fst_bytes = u64::from_le_bytes(mmap[48..56].try_into()?) as usize;
    let fst_end = 72usize.checked_add(fst_bytes).ok_or_else(|| anyhow!("FST overflow"))?;
    let fst = mmap.get(72..fst_end).ok_or_else(|| anyhow!("token FST truncated"))?;

    let bucket_dir = scratch.join("packed-posting-buckets");
    fs::create_dir_all(&bucket_dir)?;
    let mut writers = (0..BUCKETS)
        .map(|i| File::create(bucket_dir.join(format!("bucket_{i}.bin"))).map(BufWriter::new))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut bucket_bytes = [0u64; BUCKETS];
    let mut positions = Vec::<(u8, u64, u32)>::with_capacity(usize::try_from(index.token_count)?);
    let mut visited = 0u64;
    let mut total_postings = 0u64;

    let mut stream = index.fst.stream();
    while let Some((token, ordinal)) = stream.next() {
        ensure!(ordinal == visited, "nonsequential lexical token ordinals");
        let term = std::str::from_utf8(token)?;
        let posting = index.lookup(term)?.ok_or_else(|| anyhow!("missing token posting"))?;
        let df = posting.document_frequency;
        let bucket = frequency_bucket(df);
        positions.push((bucket as u8, bucket_bytes[bucket], df));
        let ids = posting.decode_ids()?;
        ensure!(ids.len() == df as usize, "token posting frequency mismatch");
        ensure!(ids.windows(2).all(|pair| pair[0] < pair[1]), "token IDs not increasing");
        for id in ids {
            writers[bucket].write_all(&id.to_le_bytes())?;
        }
        bucket_bytes[bucket] += u64::from(df) * 4;
        total_postings += u64::from(df);
        visited += 1;
    }
    ensure!(visited == index.token_count, "pack missed tokens");
    ensure!(total_postings == index.posting_count, "pack posting count mismatch");
    for writer in &mut writers {
        writer.flush()?;
    }
    drop(writers);

    let mut bases = [0u64; BUCKETS];
    let mut cursor = 0u64;
    for i in 0..BUCKETS {
        bases[i] = align(cursor);
        cursor = bases[i]
            .checked_add(bucket_bytes[i])
            .ok_or_else(|| anyhow!("pack bucket length overflow"))?;
    }

    let temporary = output.with_extension("pack.tmp");
    {
        let mut output_file = BufWriter::new(File::create(&temporary)?);
        output_file.write_all(PACK_MAGIC)?;
        output_file.write_all(&PACK_VERSION.to_le_bytes())?;
        output_file.write_all(&TOKEN_ANALYZER_VERSION.to_le_bytes())?;
        for value in [
            index.string_count,
            index.fingerprint,
            index.token_count,
            index.posting_count,
            fst_bytes as u64,
            cursor,
        ] {
            output_file.write_all(&value.to_le_bytes())?;
        }
        output_file.write_all(fst)?;
        for &(bucket, local, df) in &positions {
            let start = bases[bucket as usize].checked_add(local).ok_or_else(|| anyhow!("postings offset overflow"))?;
            output_file.write_all(&start.to_le_bytes())?;
            output_file.write_all(&df.to_le_bytes())?;
            output_file.write_all(&0u32.to_le_bytes())?;
        }
        let mut wrote = 0u64;
        let zeroes = [0u8; PAGE_ALIGN as usize];
        for i in 0..BUCKETS {
            let padding = usize::try_from(bases[i] - wrote)?;
            output_file.write_all(&zeroes[..padding])?;
            let mut input = BufReader::new(File::open(bucket_dir.join(format!("bucket_{i}.bin")))?);
            let copied = std::io::copy(&mut input, &mut output_file)?;
            ensure!(copied == bucket_bytes[i], "pack bucket truncated");
            wrote = bases[i] + copied;
        }
        ensure!(wrote == cursor, "pack payload length mismatch");
        output_file.flush()?;
    }
    let published = File::open(&temporary)?;
    let mapped = unsafe { memmap2::Mmap::map(&published)? };
    let checked = PackedTokenIndexView::parse(&mapped)?;
    ensure!(
        checked.string_count == index.string_count && checked.fingerprint == index.fingerprint,
        "packed sidecar does not match original dictionary"
    );
    drop(mapped);
    drop(published);
    fs::rename(&temporary, output)?;
    let file_bytes = fs::metadata(output)?.len();
    ensure!(file_bytes >= PACK_HEADER_BYTES as u64, "pack file empty");
    fs::remove_dir_all(bucket_dir)?;
    Ok(PackReport { tokens: positions.len(), postings: total_postings, file_bytes, bucket_bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token_index::build_token_index;
    use cypress::models::packed_token::PackedTokenIndexView;

    #[test]
    fn packed_roundtrip_uses_distinct_physical_order() {
        let names = ["bern bahnhof", "paris", "bern", "paris est"];
        let mut bytes = Vec::new();
        let mut offsets = vec![0];
        for name in names { bytes.extend_from_slice(name.as_bytes()); offsets.push(bytes.len() as u32); }
        let temp = tempfile::tempdir().unwrap();
        let token = temp.path().join("tokens.bin");
        build_token_index(&offsets, &bytes, temp.path(), &token).unwrap();
        let pack = temp.path().join(PACK_FILE);
        let report = build_from_token_index(&token, &pack, temp.path()).unwrap();
        assert_eq!(report.postings, 6);
        let stored = fs::read(pack).unwrap();
        let view = PackedTokenIndexView::parse(&stored).unwrap();
        assert!(view.matches_dictionary(&bytes, &offsets));
        assert_eq!(view.lookup("bern").unwrap().unwrap().decode_ids(), vec![0, 2]);
        assert_eq!(view.lookup("paris").unwrap().unwrap().decode_ids(), vec![1, 3]);
        assert_eq!(view.lookup("bahnhof").unwrap().unwrap().decode_ids(), vec![0]);
    }

    #[test]
    fn frequency_buckets_are_ordered_for_locality() {
        assert_eq!(frequency_bucket(2), 0);
        assert_eq!(frequency_bucket(9), 1);
        assert_eq!(frequency_bucket(32769), 7);
        assert_eq!(align(4097), 8192);
    }
}
