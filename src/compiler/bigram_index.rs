//! External-sort bigram shard pairs, streaming final postings into a sidecar.
use anyhow::{anyhow, ensure, Result};
use cypress::models::bigram_index::{name_fingerprint, BIGRAM_INDEX_MAGIC, BIGRAM_INDEX_VERSION};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const FAN_IN: usize = 32;
fn pair<R: Read>(r: &mut R) -> Result<Option<(u16,u32)>> {
    let mut buf = [0u8;6];
    let mut got = 0;
    while got < 6 {
        let n = r.read(&mut buf[got..])?;
        if n == 0 {
            if got == 0 { return Ok(None); }
            return Err(anyhow!("truncated bigram pair"));
        }
        got += n;
    }
    Ok(Some((u16::from_le_bytes(buf[..2].try_into()?),
             u32::from_le_bytes(buf[2..].try_into()?))))
}
fn put<W: Write>(w: &mut W, p: (u16,u32)) -> Result<()> {
    w.write_all(&p.0.to_le_bytes())?;
    w.write_all(&p.1.to_le_bytes())?;
    Ok(())
}
fn merge(runs: &[PathBuf], mut emit: impl FnMut((u16,u32))->Result<()>) -> Result<()> {
    let mut readers = runs.iter().map(File::open).map(|x| x.map(BufReader::new))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(u16,u32,usize)>>::new();
    for (i,r) in readers.iter_mut().enumerate() {
        if let Some((bg,id)) = pair(r)? {heap.push(Reverse((bg,id,i)));}
    }
    let mut prev = None;
    while let Some(Reverse((bg,id,source))) = heap.pop() {
        if prev != Some((bg,id)) { emit((bg,id))?; prev = Some((bg,id)); }
        if let Some((next_bg,next_id)) = pair(&mut readers[source])? {
            heap.push(Reverse((next_bg,next_id,source)));
        }
    }
    Ok(())
}
fn sort_runs(input: &Path, scratch: &Path, pairs_per_chunk: usize)->Result<Vec<PathBuf>> {
    ensure!(pairs_per_chunk > 0, "sort buffer too small");
    ensure!(fs::metadata(input)?.len()%6==0,"truncated shard");
    let mut reader=BufReader::new(File::open(input)?);
    let mut runs=Vec::new();
    loop {
        let mut chunk=Vec::with_capacity(pairs_per_chunk);
        for _ in 0..pairs_per_chunk {
            if let Some(p)=pair(&mut reader)? {chunk.push(p);} else {break;}
        }
        if chunk.is_empty(){break;}
        chunk.sort_unstable();
        chunk.dedup();
        let path=scratch.join(format!("run_{:09}.bin",runs.len()));
        let mut w=BufWriter::new(File::create(&path)?);
        for p in chunk {put(&mut w,p)?;}
        w.flush()?;
        runs.push(path);
    }
    let mut pass=0;
    while runs.len()>FAN_IN {
        let mut next=Vec::new();
        for (i,group) in runs.chunks(FAN_IN).enumerate() {
            let path=scratch.join(format!("merged_{pass}_{i}.bin"));
            let mut w=BufWriter::new(File::create(&path)?);
            merge(group,|p|put(&mut w,p))?;
            w.flush()?;
            next.push(path);
        }
        for old in runs {fs::remove_file(old)?;}
        runs=next;
        pass+=1;
    }
    Ok(runs)
}
pub fn build(
    shards: &Path, scratch: &Path, output: &Path,
    name_bytes: &[u8], gram_counts: &[u8], sort_mib: usize
)->Result<(Vec<u32>,u64)> {
    ensure!(sort_mib>0,"--bigram-sort-buffer-mib must be positive");
    fs::create_dir_all(scratch)?;
    let capacity=sort_mib.checked_mul(1024*1024)
        .ok_or_else(||anyhow!("sort size overflow"))?/8;
    // Use 8-byte in-memory tuples, while spill records are 6 bytes.
    ensure!(capacity>0,"sort buffer too small");
    let postings=scratch.join("postings.u32");
    let mut w=BufWriter::new(File::create(&postings)?);
    let mut offsets=Vec::with_capacity(65537);
    offsets.push(0u32);
    let mut next_gram=0u32;
    let mut count=0u32;
    for shard in 0..256 {
        let input=shards.join(format!("bg_{shard:03}.bin"));
        if !input.exists() || fs::metadata(&input)?.len()==0 {continue;}
        let local=scratch.join(format!("shard_{shard:03}"));
        fs::create_dir_all(&local)?;
        let runs=sort_runs(&input,&local,capacity)?;
        merge(&runs, |(gram,id)| {
            let gram=gram as u32;
            ensure!(gram >= next_gram,"bigram shard order out of sequence");
            while next_gram < gram {
                offsets.push(count);
                next_gram+=1;
            }
            w.write_all(&id.to_le_bytes())?;
            count=count.checked_add(1).ok_or_else(||anyhow!("bigram postings exceed u32"))?;
            Ok(())
        })?;
        for run in runs {fs::remove_file(run)?;}
        fs::remove_dir(&local)?;
    }
    while next_gram<65536 {
        offsets.push(count);
        next_gram+=1;
    }
    ensure!(offsets.len()==65537,"incorrect bigram offset count");
    w.flush()?;
    drop(w);
    let pending=scratch.join("cypress_bigrams.pending");
    let mut out=BufWriter::new(File::create(&pending)?);
    out.write_all(BIGRAM_INDEX_MAGIC)?;
    out.write_all(&BIGRAM_INDEX_VERSION.to_le_bytes())?;
    out.write_all(&0u32.to_le_bytes())?;
    out.write_all(&(gram_counts.len() as u64).to_le_bytes())?;
    out.write_all(&(count as u64).to_le_bytes())?;
    out.write_all(&name_fingerprint(name_bytes,gram_counts).to_le_bytes())?;
    std::io::copy(&mut BufReader::new(File::open(&postings)?),&mut out)?;
    out.flush()?;
    drop(out);
    let file=File::open(&pending)?;
    let mmap=unsafe{memmap2::MmapOptions::new().map(&file)?};
    let view=cypress::models::bigram_index::BigramIndexView::parse(&mmap)?;
    view.validate_offsets(&offsets)?;
    ensure!(view.matches_names(name_bytes,gram_counts),"bigram name fingerprint mismatch");
    drop(mmap);
    drop(file);
    fs::rename(&pending,output)?;
    fs::remove_file(postings)?;
    Ok((offsets,count as u64))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sorted_dedup_across_runs() {
        let t=tempfile::tempdir().unwrap();
        let shards=t.path().join("shards");
        fs::create_dir_all(&shards).unwrap();
        let mut w=BufWriter::new(File::create(shards.join("bg_000.bin")).unwrap());
        for (gram,id) in [(2,7),(1,9),(2,7),(1,4),(2,3),(1,9)] {
            put(&mut w,(gram,id)).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        let dest=t.path().join("out.bin");
        let (offsets,count)=build(&shards,&t.path().join("scratch"),&dest,b"names",&[2,3],1).unwrap();
        assert_eq!(count,4);
        let bytes=fs::read(dest).unwrap();
        let view=cypress::models::bigram_index::BigramIndexView::parse(&bytes).unwrap();
        assert_eq!(view.postings(offsets[1] as usize,offsets[2] as usize),Some(&[4,9][..]));
        assert_eq!(view.postings(offsets[2] as usize,offsets[3] as usize),Some(&[3,7][..]));
    }
}
