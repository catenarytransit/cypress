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

#[cfg(test)]
mod tests {
    use super::*;
    use cypress::models::dense_columns::DenseColumnsView;

    fn le_u32(values: &[u32]) -> Vec<u8> {
        values.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn le_u64(values: &[u64]) -> Vec<u8> {
        values.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn le_f32(values: &[f32]) -> Vec<u8> {
        values.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    #[test]
    fn roundtrip_and_reject_malformed_offsets() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("columns");
        fs::create_dir(&dir).unwrap();
        let fields = [
            le_f32(&[34.0,35.0]), le_f32(&[-118.0,-119.0]),
            le_f32(&[1.0,2.0]), le_u32(&[0,1,3]),
            b"abc".to_vec(), vec![0u8,1], le_u32(&[5,6]),
            le_u32(&[0]), le_u64(&[0,2]), b"12".to_vec(),
            le_u64(&[0,5]), b"90210".to_vec(), le_u32(&[1]),
            le_u32(&[u32::MAX,0]), le_u32(&[1,2]),
        ];
        for (name,data) in SECTION_NAMES.iter().zip(fields.iter()) {
            fs::write(dir.join(format!("{name}.bin")),data).unwrap();
        }
        let path=temp.path().join("dense.bin");
        assemble(&dir,&path,0x1234_5678).unwrap();
        let mut data=fs::read(&path).unwrap();
        let view=DenseColumnsView::parse(&data).unwrap();
        assert_eq!(view.place_latitudes, &[34.0,35.0]);
        assert_eq!(view.house_number_offsets, &[0,2]);
        assert_eq!(view.house_place_ids, &[1]);
        assert_eq!(view.place_area_sets, &[1,2]);
        assert_eq!(view.dictionary_fingerprint, 0x1234_5678);
        data[24] ^= 1; // first section offset no longer equals header size
        assert!(DenseColumnsView::parse(&data).is_err());
    }
}
