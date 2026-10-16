//! External sort of (place_id, house_idx) into a dense on-disk reverse map.
//! The first indexed house wins (minimum house index), matching the old Vec update.
use super::disk_columns::DiskColumn;
use anyhow::{anyhow, ensure, Result};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};
const FAN_IN: usize = 32;

pub fn write_pair(w: &mut impl Write, place: u32, house: u32) -> Result<()> {
    w.write_all(&place.to_le_bytes())?;
    w.write_all(&house.to_le_bytes())?;
    Ok(())
}
fn read_pair(r: &mut impl Read) -> Result<Option<(u32, u32)>> {
    let mut bytes = [0u8; 8];
    let mut n = 0;
    while n < 8 {
        let read = r.read(&mut bytes[n..])?;
        if read == 0 {
            ensure!(n == 0, "truncated reverse pair");
            return Ok(None);
        }
        n += read;
    }
    Ok(Some((
        u32::from_le_bytes(bytes[..4].try_into()?),
        u32::from_le_bytes(bytes[4..].try_into()?),
    )))
}
fn merge(paths: &[PathBuf], mut emit: impl FnMut(u32, u32) -> Result<()>) -> Result<()> {
    let mut readers = paths
        .iter()
        .map(|p| File::open(p).map(|f| BufReader::with_capacity(16 * 1024, f)))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some((place, house)) = read_pair(reader)? {
            heap.push(Reverse((place, house, i)));
        }
    }
    while let Some(Reverse((place, house, i))) = heap.pop() {
        emit(place, house)?;
        if let Some((p, h)) = read_pair(&mut readers[i])? {
            heap.push(Reverse((p, h, i)));
        }
    }
    Ok(())
}
pub fn build(
    input: &Path,
    scratch: &Path,
    output_dir: &Path,
    output_name: &str,
    total_places: u32,
    buffer_mib: usize,
) -> Result<()> {
    ensure!(buffer_mib > 0, "reverse sort buffer must be positive");
    ensure!(
        fs::metadata(input)?.len() % 8 == 0,
        "reverse pair input truncated"
    );
    fs::create_dir_all(scratch)?;
    let capacity = buffer_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("buffer overflow"))?
        / 8;
    ensure!(capacity > 0, "sort capacity zero");
    let mut reader = BufReader::new(File::open(input)?);
    let mut runs = Vec::new();
    loop {
        let mut records = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            if let Some(pair) = read_pair(&mut reader)? {
                records.push(pair);
            } else {
                break;
            }
        }
        if records.is_empty() {
            break;
        }
        records.sort_unstable();
        let path = scratch.join(format!("run_{:09}.bin", runs.len()));
        let mut out = BufWriter::new(File::create(&path)?);
        for (p, h) in records {
            write_pair(&mut out, p, h)?;
        }
        out.flush()?;
        runs.push(path);
    }
    let mut pass = 0;
    while runs.len() > FAN_IN {
        let mut next = Vec::new();
        for (i, group) in runs.chunks(FAN_IN).enumerate() {
            let path = scratch.join(format!("merge_{pass}_{i}.bin"));
            let mut out = BufWriter::new(File::create(&path)?);
            merge(group, |p, h| write_pair(&mut out, p, h))?;
            out.flush()?;
            next.push(path);
        }
        for path in runs {
            fs::remove_file(path)?;
        }
        runs = next;
        pass += 1;
    }
    let mut output = DiskColumn::<u32>::new(output_dir, output_name)?;
    let mut expected = 0u64;
    let mut previous = None;
    merge(&runs, |p, h| {
        ensure!(p < total_places, "reverse place ID out of bounds");
        if previous == Some(p) {
            return Ok(());
        } // sorted pairs => lowest house index wins
        while expected < p as u64 {
            output.push(u32::MAX)?;
            expected += 1;
        }
        output.push(h)?;
        expected += 1;
        previous = Some(p);
        Ok(())
    })?;
    while expected < total_places as u64 {
        output.push(u32::MAX)?;
        expected += 1;
    }
    ensure!(
        output.len() == total_places as usize,
        "wrong reverse map length"
    );
    output.finish()?;
    for path in runs {
        fs::remove_file(path)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_house_and_missing_place() {
        let t = tempfile::tempdir().unwrap();
        let input = t.path().join("pairs");
        let mut w = BufWriter::new(File::create(&input).unwrap());
        for (p, h) in [(3, 7), (1, 5), (3, 2), (1, 4)] {
            write_pair(&mut w, p, h).unwrap();
        }
        w.flush().unwrap();
        build(
            &input,
            &t.path().join("work"),
            &t.path().join("cols"),
            "reverse",
            5,
            1,
        )
        .unwrap();
        let data = fs::read(t.path().join("cols/reverse.bin")).unwrap();
        let got = data
            .chunks_exact(4)
            .map(|x| u32::from_le_bytes(x.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(got, [u32::MAX, 4, u32::MAX, 2, u32::MAX]);
    }
}
