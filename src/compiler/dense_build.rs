//! Assemble individually streamed columns into an aligned read-only mmap sidecar.
use anyhow::{anyhow, ensure, Result};
use cypress::models::dense_columns::{DenseColumnsView, DENSE_MAGIC, DENSE_VERSION,
    HEADER_SIZE, SECTION_COUNT, SECTION_NAMES, ELEMENT_BYTES};
use std::{fs::{self, File}, io::{BufReader, BufWriter, Read, Write}, path::Path};

pub fn assemble(columns: &Path, output: &Path, fingerprint: u64) -> Result<()> {
    let mut sections=Vec::with_capacity(SECTION_COUNT);
    let mut cursor=HEADER_SIZE as u64;
    for (i,name) in SECTION_NAMES.iter().enumerate() {
        let path=columns.join(format!("{name}.bin"));
        let bytes=fs::metadata(&path)?.len();
        ensure!(bytes % ELEMENT_BYTES[i] as u64 == 0,
                "invalid byte length in dense column {name}");
        sections.push((cursor, bytes / ELEMENT_BYTES[i] as u64));
        cursor=cursor.checked_add(bytes).ok_or_else(||anyhow!("sidecar size overflow"))?;
        cursor=cursor.checked_add(7).ok_or_else(||anyhow!("sidecar alignment overflow"))? & !7;
    }
    let mut out=BufWriter::new(File::create(output)?);
    out.write_all(DENSE_MAGIC)?;
    out.write_all(&DENSE_VERSION.to_le_bytes())?;
    out.write_all(&(SECTION_COUNT as u32).to_le_bytes())?;
    out.write_all(&fingerprint.to_le_bytes())?;
    for (offset,count) in &sections {
        out.write_all(&offset.to_le_bytes())?;
        out.write_all(&count.to_le_bytes())?;
    }
    ensure!(HEADER_SIZE==24+SECTION_COUNT*16, "dense header size mismatch");
    let zeros=[0u8;8];
    for (i,name) in SECTION_NAMES.iter().enumerate() {
        let src=columns.join(format!("{name}.bin"));
        let actual=std::io::copy(&mut BufReader::new(File::open(src)?),&mut out)?;
        ensure!(actual==sections[i].1*ELEMENT_BYTES[i] as u64,"dense column changed during copy");
        let pad=(8-actual%8)%8;
        out.write_all(&zeros[..pad as usize])?;
    }
    out.flush()?;
    drop(out);
    let file=File::open(output)?;
    let mmap=unsafe{memmap2::MmapOptions::new().map(&file)?};
    let view=DenseColumnsView::parse(&mmap)?;
    ensure!(view.dictionary_fingerprint==fingerprint, "dense fingerprint mismatch");
    drop(mmap);
    Ok(())
}
