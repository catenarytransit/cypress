//! Offline compact token index builder.
//! Bounded-memory external sort: one sorted run per fixed-size buffer,
//! then heap-merge runs into a token FST and delta-varint postings.
use anyhow::{anyhow, ensure, Result};
use cypress::models::token_index::{
    dictionary_fingerprint, tokenize_name, TokenIndexView,
    TOKEN_INDEX_MAGIC, TOKEN_INDEX_VERSION, TOKEN_ANALYZER_VERSION,
    TOKEN_SKIP_BLOCK,
};
use fst::MapBuilder;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const RUN_PAIR_LIMIT: usize = 500_000;
const MAX_OPEN_RUNS: usize = 32;
const MAX_TOKEN_BYTES: usize = 4096;

pub struct BuildReport {
    pub token_count: u64,
    pub posting_count: u64,
    pub posting_bytes: u64,
    pub fst_bytes: u64,
    pub skip_count: u64,
    pub total_bytes: u64,
}

fn write_pair<W: Write>(w: &mut W, token: &str, id: u32) -> Result<()> {
    ensure!(token.len() <= MAX_TOKEN_BYTES, "token too long");
    w.write_all(&(token.len() as u16).to_le_bytes())?;
    w.write_all(token.as_bytes())?;
    w.write_all(&id.to_le_bytes())?;
    Ok(())
}

fn read_pair<R: Read>(r: &mut R) -> Result<Option<(String, u32)>> {
    let mut len = [0; 2];
    match r.read_exact(&mut len) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let n = u16::from_le_bytes(len) as usize;
    ensure!(n > 0 && n <= MAX_TOKEN_BYTES, "corrupt token length");
    let mut bytes = vec![0; n];
    r.read_exact(&mut bytes)?;
    let mut id = [0; 4];
    r.read_exact(&mut id)?;
    Ok(Some((String::from_utf8(bytes)?, u32::from_le_bytes(id))))
}

fn flush_run(pairs: &mut Vec<(String, u32)>, dir: &Path, seq: usize) -> Result<PathBuf> {
    pairs.sort_unstable();
    pairs.dedup();
    let path = dir.join(format!("run_{seq:09}.bin"));
    let mut w = BufWriter::new(File::create(&path)?);
    for (token, id) in pairs.drain(..) {
        write_pair(&mut w, &token, id)?;
    }
    w.flush()?;
    Ok(path)
}

/// Merge pre-sorted runs. The callback receives globally ordered, unique pairs.
fn merge_runs(
    paths: &[PathBuf],
    mut emit: impl FnMut(String, u32) -> Result<()>,
) -> Result<()> {
    let mut readers = paths.iter().map(File::open)
        .map(|f| f.map(BufReader::new)).collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(String, u32, usize)>>::new();
    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some((token, id)) = read_pair(reader)? {
            heap.push(Reverse((token, id, i)));
        }
    }
    let mut last: Option<(String, u32)> = None;
    while let Some(Reverse((token, id, idx))) = heap.pop() {
        let different = last.as_ref().map_or(true, |(t, x)| t != &token || *x != id);
        if different {
            emit(token.clone(), id)?;
            last = Some((token, id));
        }
        if let Some((next, next_id)) = read_pair(&mut readers[idx])? {
            heap.push(Reverse((next, next_id, idx)));
        }
    }
    Ok(())
}

fn merge_to_file(paths: &[PathBuf], output: &Path) -> Result<()> {
    let mut writer = BufWriter::new(File::create(output)?);
    merge_runs(paths, |token, id| write_pair(&mut writer, &token, id))?;
    writer.flush()?;
    Ok(())
}

fn reduce_runs(mut runs: Vec<PathBuf>, dir: &Path) -> Result<Vec<PathBuf>> {
    let mut pass = 0usize;
    while runs.len() > MAX_OPEN_RUNS {
        let mut next = Vec::new();
        for (i, group) in runs.chunks(MAX_OPEN_RUNS).enumerate() {
            let path = dir.join(format!("merge_{pass}_{i}.bin"));
            merge_to_file(group, &path)?;
            next.push(path);
        }
        for path in runs {
            fs::remove_file(path)?;
        }
        runs = next;
        pass += 1;
    }
    Ok(runs)
}

fn append_varint(mut value: u32, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_u64s<W: Write>(w: &mut W, a: &[u64]) -> Result<()> {
    for &x in a { w.write_all(&x.to_le_bytes())?; }
    Ok(())
}
fn write_u32s<W: Write>(w: &mut W, a: &[u32]) -> Result<()> {
    for &x in a { w.write_all(&x.to_le_bytes())?; }
    Ok(())
}

pub fn build_token_index(
    string_name_offsets: &[u32],
    string_name_bytes: &[u8],
    scratch_root: &Path,
    output_path: &Path,
) -> Result<BuildReport> {
    ensure!(!string_name_offsets.is_empty(), "missing string boundaries");
    ensure!(string_name_offsets[0] == 0, "invalid first string offset");
    ensure!(string_name_offsets.last().copied() == Some(string_name_bytes.len() as u32),
        "last string offset mismatch");
    let run_dir = scratch_root.join("tokens");
    fs::create_dir_all(&run_dir)?;
    let mut pairs = Vec::<(String, u32)>::with_capacity(RUN_PAIR_LIMIT);
    let mut runs = Vec::new();
    for string_id in 0..string_name_offsets.len() - 1 {
        let begin = string_name_offsets[string_id] as usize;
        let end = string_name_offsets[string_id + 1] as usize;
        ensure!(begin <= end && end <= string_name_bytes.len(), "invalid string offsets");
        let name = std::str::from_utf8(&string_name_bytes[begin..end])?;
        let mut unique: Vec<&str> = tokenize_name(name)
            .filter(|token| token.len() <= MAX_TOKEN_BYTES).collect();
        unique.sort_unstable();
        unique.dedup();
        for token in unique {
            pairs.push((token.to_owned(), u32::try_from(string_id)?));
            if pairs.len() == RUN_PAIR_LIMIT {
                runs.push(flush_run(&mut pairs, &run_dir, runs.len())?);
            }
        }
    }
    if !pairs.is_empty() { runs.push(flush_run(&mut pairs, &run_dir, runs.len())?); }
    let runs = reduce_runs(runs, &run_dir)?;

    // Emit flattened sections to temporary files, keeping only one posting
    // list and the token FST in memory. Large postings spill through buffers.
    let fst_path = run_dir.join("tokens.fst");
    let postings_path = run_dir.join("postings.bin");
    let mut fst_builder = MapBuilder::new(BufWriter::new(File::create(&fst_path)?))?;
    let mut posting_writer = BufWriter::new(File::create(&postings_path)?);
    let mut offsets = vec![0u64];
    let mut frequencies = Vec::<u32>::new();
    let mut skip_offsets = vec![0u64];
    let mut skips = Vec::<(u32, u64)>::new();
    let mut current_token: Option<String> = None;
    let mut last_id = 0u32;
    let mut local_count = 0u32;
    let mut total_postings = 0u64;
    let mut byte_count = 0u64;
    let mut block_bytes = Vec::<u8>::with_capacity(TOKEN_SKIP_BLOCK * 5);

    // Flush the currently accumulated block. Delta coding runs continuously
    // across blocks, so seeking into a block needs previous last ID.
    // PR 4 will use skip metadata for this.
    let flush_block = |last: u32, buf: &mut Vec<u8>,
                           out: &mut BufWriter<File>, bytes: &mut u64,
                           skips: &mut Vec<(u32, u64)>| -> Result<()> {
        if buf.is_empty() { return Ok(()); }
        out.write_all(buf)?;
        *bytes += buf.len() as u64;
        skips.push((last, *bytes));
        buf.clear();
        Ok(())
    };

    merge_runs(&runs, |token, id| {
        if current_token.as_deref() != Some(token.as_str()) {
            if current_token.is_some() {
                flush_block(last_id, &mut block_bytes, &mut posting_writer,
                            &mut byte_count, &mut skips)?;
                offsets.push(byte_count);
                frequencies.push(local_count);
                skip_offsets.push(skips.len() as u64);
            }
            fst_builder.insert(token.as_bytes(), frequencies.len() as u64)?;
            current_token = Some(token);
            last_id = 0;
            local_count = 0;
        }
        ensure!(local_count == 0 || id > last_id, "posting list must be increasing");
        let delta = id - last_id;
        append_varint(delta, &mut block_bytes);
        local_count = local_count.checked_add(1).ok_or_else(|| anyhow!("docfreq overflow"))?;
        total_postings += 1;
        last_id = id;
        if local_count as usize % TOKEN_SKIP_BLOCK == 0 {
            flush_block(last_id, &mut block_bytes, &mut posting_writer,
                        &mut byte_count, &mut skips)?;
        }
        Ok(())
    })?;
    if current_token.is_some() {
        flush_block(last_id, &mut block_bytes, &mut posting_writer,
                    &mut byte_count, &mut skips)?;
        offsets.push(byte_count);
        frequencies.push(local_count);
        skip_offsets.push(skips.len() as u64);
    }

    posting_writer.flush()?;
    fst_builder.finish()?;
    let fst_bytes = fs::metadata(&fst_path)?.len();
    let posting_bytes = fs::metadata(&postings_path)?.len();
    ensure!(posting_bytes == byte_count, "posting byte accounting mismatch");
    ensure!(offsets.len() == frequencies.len() + 1, "incorrect token offsets");

    let temp_output = output_path.with_extension("bin.tmp");
    {
        let mut out = BufWriter::new(File::create(&temp_output)?);
        out.write_all(TOKEN_INDEX_MAGIC)?;
        out.write_all(&TOKEN_INDEX_VERSION.to_le_bytes())?;
        out.write_all(&TOKEN_ANALYZER_VERSION.to_le_bytes())?;
        for v in [
            (string_name_offsets.len() - 1) as u64,
            dictionary_fingerprint(string_name_bytes, string_name_offsets),
            frequencies.len() as u64,
            total_postings,
            fst_bytes,
            posting_bytes,
            skips.len() as u64,
        ] { out.write_all(&v.to_le_bytes())?; }
        std::io::copy(&mut BufReader::new(File::open(&fst_path)?), &mut out)?;
        write_u64s(&mut out, &offsets)?;
        write_u32s(&mut out, &frequencies)?;
        write_u64s(&mut out, &skip_offsets)?;
        for &(last, byte_end) in &skips {
            out.write_all(&last.to_le_bytes())?;
            out.write_all(&byte_end.to_le_bytes())?;
        }
        std::io::copy(&mut BufReader::new(File::open(&postings_path)?), &mut out)?;
        out.flush()?;
    }
    // Validate all structural offsets and sample token metadata before publish.
    let mmap_file = File::open(&temp_output)?;
    let mapped = unsafe { memmap2::MmapOptions::new().map(&mmap_file)? };
    let view = TokenIndexView::parse(&mapped)?;
    ensure!(view.matches_dictionary(string_name_bytes, string_name_offsets),
        "token index dictionary fingerprint mismatch");
    fs::rename(&temp_output, output_path)?;
    let size = fs::metadata(output_path)?.len();

    // Runs and scratch artifacts belong to this compiler invocation only.
    fs::remove_dir_all(&run_dir).ok();
    Ok(BuildReport {
        token_count: frequencies.len() as u64,
        posting_count: total_postings,
        posting_bytes, fst_bytes,
        skip_count: skips.len() as u64,
        total_bytes: size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postings_roundtrip_unicode_and_dedup() {
        let strings = ["hollywood walk", "walk hollywood", "東京駅", "Bruxelles Brussel",
                       "hollywood hollywood", "القاهرة"];
        let mut bytes = Vec::new();
        let mut offsets = vec![0];
        for s in strings {
            bytes.extend_from_slice(s.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join(cypress::models::token_index::TOKEN_INDEX_FILE);
        build_token_index(&offsets, &bytes, temp.path(), &file).unwrap();
        let stored = fs::read(file).unwrap();
        let view = TokenIndexView::parse(&stored).unwrap();
        assert_eq!(view.lookup("hollywood").unwrap().unwrap().decode_ids().unwrap(),
                   vec![0, 1, 4]);
        assert_eq!(view.lookup("東京駅").unwrap().unwrap().decode_ids().unwrap(), vec![2]);
        assert!(view.lookup("القاھرة").unwrap().is_none());
        assert!(view.matches_dictionary(&bytes, &offsets));
        assert!(!view.matches_dictionary(b"X", &[0, 1]));
    }
}
