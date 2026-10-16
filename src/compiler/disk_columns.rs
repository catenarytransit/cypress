//! Small fixed-memory, sequential on-disk columns. No growing Vec payload.
use anyhow::{ensure, Result};
use bytemuck::Pod;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;

pub struct DiskColumn<T: Pod> {
    writer: BufWriter<File>,
    len: usize,
    _type: PhantomData<T>,
}

impl<T: Pod> DiskColumn<T> {
    pub fn new(dir: &Path, name: &str) -> Result<Self> {
        ensure!(
            cfg!(target_endian = "little"),
            "disk columns require little-endian"
        );
        fs::create_dir_all(dir)?;
        Ok(Self {
            writer: BufWriter::with_capacity(
                64 * 1024,
                File::create(dir.join(format!("{name}.bin")))?,
            ),
            len: 0,
            _type: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, value: T) -> Result<()> {
        self.writer.write_all(bytemuck::bytes_of(&value))?;
        self.len = self
            .len
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("column length overflow"))?;
        Ok(())
    }

    pub fn extend_from_slice(&mut self, values: &[T]) -> Result<()> {
        self.writer.write_all(bytemuck::cast_slice(values))?;
        self.len = self
            .len
            .checked_add(values.len())
            .ok_or_else(|| anyhow::anyhow!("column length overflow"))?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}
