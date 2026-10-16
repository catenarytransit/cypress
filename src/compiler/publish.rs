//! Publish compiler artifacts when scratch and output directories may be on different filesystems.
//!
//! A rename is fast and atomic on one filesystem. Across mount points Linux
//! returns EXDEV, so copy into a temporary file *in the destination directory*
//! and rename that temporary file only after it has been fully written.

use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use tempfile::NamedTempFile;

/// Publish a completed scratch file without exposing a partially copied output.
pub(super) fn publish_file(source: &Path, destination: &Path) -> Result<()> {
    publish_with_rename(source, destination, |from, to| fs::rename(from, to))
}

fn publish_with_rename(
    source: &Path,
    destination: &Path,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<()> {
    match rename(source, destination) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::CrossesDevices => {
            tracing::info!(
                source = %source.display(),
                destination = %destination.display(),
                "Compiler artifact crosses filesystems; copying before atomic publish"
            );
            copy_across_filesystems(source, destination)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "Failed to publish compiler artifact {} -> {}",
                source.display(),
                destination.display()
            )
        }),
    }
}

fn copy_across_filesystems(source: &Path, destination: &Path) -> Result<()> {
    let parent = destination
        .parent()
        .context("Compiler artifact destination has no parent directory")?;
    let mut staged = NamedTempFile::new_in(parent)
        .with_context(|| format!("Failed to stage compiler artifact in {}", parent.display()))?;
    let mut input = File::open(source)
        .with_context(|| format!("Failed to open compiler artifact {}", source.display()))?;

    io::copy(&mut input, staged.as_file_mut()).with_context(|| {
        format!(
            "Failed to copy compiler artifact {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    staged.as_file().sync_all().with_context(|| {
        format!(
            "Failed to sync staged compiler artifact for {}",
            destination.display()
        )
    })?;

    // This rename is always within the destination filesystem. In particular,
    // an existing destination is not truncated if an earlier copy fails.
    staged
        .persist(destination)
        .map_err(|err| err.error)
        .with_context(|| {
            format!(
                "Failed to publish staged compiler artifact {}",
                destination.display()
            )
        })?;

    // Only discard the scratch file after the destination is fully published.
    fs::remove_file(source).with_context(|| {
        format!(
            "Failed to remove published scratch artifact {}",
            source.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn same_filesystem_rename_removes_source_and_replaces_destination() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("scratch.bin");
        let destination = dir.path().join("published.bin");
        fs::write(&source, b"new data")?;
        fs::write(&destination, b"old data")?;

        publish_file(&source, &destination)?;

        assert!(!source.exists());
        assert_eq!(fs::read(destination)?, b"new data");
        Ok(())
    }

    #[test]
    fn exdev_fallback_copies_then_replaces_destination() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("scratch.bin");
        let destination = dir.path().join("published.bin");
        fs::write(&source, b"new data")?;
        fs::write(&destination, b"old data")?;

        // Force the EXDEV branch without requiring two mounted filesystems in CI.
        publish_with_rename(&source, &destination, |_, _| {
            Err(io::Error::new(ErrorKind::CrossesDevices, "simulated EXDEV"))
        })?;

        assert!(!source.exists());
        assert_eq!(fs::read(destination)?, b"new data");
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn exdev_fallback_failure_preserves_existing_destination() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("missing.bin");
        let destination = dir.path().join("published.bin");
        fs::write(&destination, b"old data")?;

        let result = publish_with_rename(&source, &destination, |_, _| {
            Err(io::Error::new(ErrorKind::CrossesDevices, "simulated EXDEV"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&destination)?, b"old data");
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn non_exdev_rename_errors_do_not_trigger_a_copy() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("scratch.bin");
        let destination = dir.path().join("published.bin");
        fs::write(&source, b"new data")?;
        fs::write(&destination, b"old data")?;

        let result = publish_with_rename(&source, &destination, |_, _| {
            Err(io::Error::from(ErrorKind::PermissionDenied))
        });
        assert!(result.is_err());
        assert!(source.exists());
        assert_eq!(fs::read(destination)?, b"old data");
        Ok(())
    }
}
