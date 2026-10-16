//! Bounded-memory external merge sort for address buckets.
//! Uses the exact legacy lexicographic sort key and preserves duplicates.
use super::{read_address_bucket_entry, write_address_bucket_entry, AddressBucketEntry};
use anyhow::{ensure, Result};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

const FAN_IN: usize = 32;
const READER_BUFFER: usize = 16 * 1024;
const MAX_RECORD_BYTES: usize = 1024 * 1024;

fn cmp(a: &AddressBucketEntry, b: &AddressBucketEntry) -> Ordering {
    a.anchor
        .cmp(&b.anchor)
        .then(a.area_set.cmp(&b.area_set))
        .then(a.house_number.cmp(&b.house_number))
        .then(a.postcode.cmp(&b.postcode))
        .then(a.place_id.cmp(&b.place_id))
}

struct Item {
    entry: AddressBucketEntry,
    source: usize,
}
impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp(&self.entry, &other.entry).then(self.source.cmp(&other.source))
    }
}

fn record_bytes(e: &AddressBucketEntry) -> usize {
    std::mem::size_of::<AddressBucketEntry>()
        + e.anchor.capacity()
        + e.house_number.capacity()
        + e.postcode.capacity()
}

fn write_entry<W: Write>(out: &mut W, e: &AddressBucketEntry) -> Result<()> {
    write_address_bucket_entry(
        out,
        e.place_id,
        e.area_set,
        &e.anchor,
        &e.house_number,
        &e.postcode,
    )
}

fn merge(paths: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers = paths
        .iter()
        .map(|p| File::open(p).map(|f| BufReader::with_capacity(READER_BUFFER, f)))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<Item>>::new();
    for (source, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = read_address_bucket_entry(reader)? {
            ensure!(
                record_bytes(&entry) <= MAX_RECORD_BYTES,
                "address record exceeds memory limit"
            );
            heap.push(Reverse(Item { entry, source }));
        }
    }
    let mut out = BufWriter::new(File::create(output)?);
    while let Some(Reverse(Item { entry, source })) = heap.pop() {
        write_entry(&mut out, &entry)?;
        if let Some(next) = read_address_bucket_entry(&mut readers[source])? {
            ensure!(
                record_bytes(&next) <= MAX_RECORD_BYTES,
                "address record exceeds memory limit"
            );
            heap.push(Reverse(Item {
                entry: next,
                source,
            }));
        }
    }
    out.flush()?;
    Ok(())
}

/// Sort one arbitrary-size address spill with a fixed working-set budget.
/// The returned file belongs to scratch, which must be kept until consumed.
pub fn sort_bucket(input: &Path, scratch: &Path, memory_mib: usize) -> Result<PathBuf> {
    ensure!(
        memory_mib >= 2,
        "--address-sort-buffer-mib must be at least 2"
    );
    let budget = memory_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow::anyhow!("address sort budget overflow"))?;
    fs::create_dir_all(scratch)?;
    let mut reader = BufReader::new(File::open(input)?);
    let mut runs = Vec::<PathBuf>::new();
    let mut exhausted = false;
    while !exhausted {
        let mut chunk = Vec::<AddressBucketEntry>::new();
        let mut used = 0usize;
        while used < budget.saturating_sub(MAX_RECORD_BYTES + 64 * 1024) {
            match read_address_bucket_entry(&mut reader)? {
                Some(entry) => {
                    let bytes = record_bytes(&entry);
                    ensure!(
                        bytes <= MAX_RECORD_BYTES,
                        "address record exceeds memory limit"
                    );
                    used = used
                        .checked_add(bytes + std::mem::size_of::<AddressBucketEntry>())
                        .ok_or_else(|| anyhow::anyhow!("memory accounting overflow"))?;
                    chunk.push(entry);
                }
                None => {
                    exhausted = true;
                    break;
                }
            }
        }
        if chunk.is_empty() {
            break;
        }
        chunk.sort_unstable_by(cmp);
        let path = scratch.join(format!("run_{:09}.bin", runs.len()));
        let mut out = BufWriter::new(File::create(&path)?);
        for entry in &chunk {
            write_entry(&mut out, entry)?;
        }
        out.flush()?;
        runs.push(path);
    }
    let mut pass = 0usize;
    while runs.len() > FAN_IN {
        let mut next = Vec::new();
        for (i, group) in runs.chunks(FAN_IN).enumerate() {
            let path = scratch.join(format!("merge_{pass}_{i:09}.bin"));
            merge(group, &path)?;
            next.push(path);
        }
        for path in runs {
            fs::remove_file(path)?;
        }
        runs = next;
        pass += 1;
    }
    let final_path = scratch.join("sorted.bin");
    merge(&runs, &final_path)?;
    for path in runs {
        fs::remove_file(path)?;
    }
    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_original_order_and_duplicates() {
        let t = tempfile::tempdir().unwrap();
        let input = t.path().join("unsorted.bin");
        let mut w = BufWriter::new(File::create(&input).unwrap());
        for (place, area, anchor, number, postcode) in [
            (3, 2, "oak", "10", "c"),
            (2, 1, "oak", "10", "a"),
            (1, 1, "maple", "2", "b"),
            (2, 1, "oak", "10", "a"),
        ] {
            write_address_bucket_entry(&mut w, place, area, anchor, number, postcode).unwrap();
        }
        w.flush().unwrap();
        let path = sort_bucket(&input, &t.path().join("work"), 2).unwrap();
        let mut reader = BufReader::new(File::open(path).unwrap());
        let mut result = Vec::new();
        while let Some(entry) = read_address_bucket_entry(&mut reader).unwrap() {
            result.push(entry);
        }
        assert_eq!(
            result.iter().map(|x| x.place_id).collect::<Vec<_>>(),
            [1, 2, 2, 3]
        );
    }
}
