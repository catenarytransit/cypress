use crate::config::{Config, RegionConfig};
use crate::version::{calculate_file_hash, VersionDoc, VersionManager};
use crate::Args;
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

struct PreparedRegion {
    region: RegionConfig,
    filename: String,
    hash: String,
    args: Args,
    import_start: chrono::DateTime<Utc>,
}

pub async fn run_batch(config_path: PathBuf, args: Args) -> Result<()> {
    let config = Config::load_from_file(config_path)?;
    let version_manager = Arc::new(VersionManager::new(&config.global.es_url).await?);

    info!("Starting batch import for {} regions", config.regions.len());

    // Ensure tmp_dir exists
    std::fs::create_dir_all(&config.global.tmp_dir)?;

    // Channel for pipeline
    // Buffer size 2 means we can have 2 prepared regions waiting while 1 is ingesting.
    // This allows downloading/filtering ahead.
    let (tx, mut rx) = mpsc::channel::<PreparedRegion>(2);

    let config_clone = config.clone();
    let args_clone = args.clone();
    let version_manager_clone = version_manager.clone();

    // Spawn Producer (Download & Filter)
    tokio::spawn(async move {
        // Track if it's the first region to handle --create-index logic
        // Note: This logic assumes strictly sequential processing matching config order.
        let mut is_first_region = true;

        for region in &config_clone.regions {
            let res = prepare_region(
                &region,
                &config_clone,
                &args_clone,
                &version_manager_clone,
                is_first_region,
            )
            .await;
            match res {
                Ok(Some(prepared)) => {
                    // Send to ingest loop
                    if tx.send(prepared).await.is_err() {
                        info!("Receiver dropped, stopping producer.");
                        break;
                    }
                    // Only flip flag if we actually produced a region to ingest
                    // (If we skipped due to version, we don't count it as 'first' for create-index?
                    // Actually, if we skip, we shouldn't trigger create-index later?)
                    // Logic: "Fresh import" usually implies we want to wipe everything.
                    // If the first region is skipped, maybe we shouldn't wipe?
                    // But if user said --create-index, they probably want a fresh start.
                    // If Region 1 is skipped, and Region 2 is processed, if we pass create-index=true to Region 2,
                    // it will delete Region 1's data (if index is shared).
                    // BUT: Current logic `create_index(&es_client, true)` wipes the whole index.
                    // If we skip Region 1, its data remains?
                    // If we wipe on Region 2, we lose Region 1.
                    // So: If `create_index` is requested, we MUST run it on the very first iteration,
                    // OR we force run Region 1 even if version matches.
                    // For now, let's assume if `create_index` is true, we force refresh.
                    // But `args.refresh` handles force.

                    // Actually, if `create_index` is set, `prepare_region` should probably respect it?
                    // Let's handle is_first_logic inside prepare or here.
                    // Be safe: set is_first_region = false after first iteration regardless of skip?
                    // No, if we skip, we don't send anything. The consumer receives the *first sent* item.
                    // That item will have `create_index` set based on what we calculated here.
                    // If we skip R1, and send R2. R2 gets `create_index=true` (if is_first_region is still true).
                    // This wipes index. R1 data lost.
                    // Conclusion: If `create_index` is requested, we probably shouldn't be skipping *any* regions assuming we want a full rebuild.
                    // OR: We only support `create_index` manual usage.
                    // To be safe: we pass `is_first_region` and update it.
                    // If we send a job, we set it to false.

                    is_first_region = false;
                }
                Ok(None) => {
                    info!("Skipped {}", region.name);
                    // If we skip, we DO NOT flip is_first_region?
                    // If R1 skipped, R2 becomes first. It wipes index. R1 lost. Correct behavior?
                    // If R1 is already in index (skipped), and we wipe index for R2, we lose R1. BAD.
                    // FIX: If we skip ANY region, we must ensure we DO NOT wipe index subsequently.
                    // So: `is_first_region` must be set to false after *execution* of the first loop iteration, regardless of outcome?
                    // Or better: If we find a version match, it implies index has data. So we should NOT wipe index.
                    // So if skip happens, we set is_first_region = false.
                    is_first_region = false;
                }
                Err(e) => {
                    error!("Failed to prepare {}: {:?}", region.name, e);
                    // Do not kill pipeline, just skip
                    is_first_region = false;
                }
            }
        }
    });

    // Consumer (Ingest & Version Save)
    while let Some(prepared) = rx.recv().await {
        info!("Starting ingest for {}", prepared.region.name);

        let res = crate::run_single(prepared.args).await;
        if let Err(e) = res {
            error!("Ingest failed for {}: {:?}", prepared.region.name, e);
            continue;
        }

        // Save version
        info!("Saving version for {}...", prepared.region.name);
        let save_res = version_manager
            .save_version(VersionDoc {
                region_name: prepared.region.name.clone(),
                filename: prepared.filename,
                hash: prepared.hash,
                timestamp: prepared.import_start.to_rfc3339(),
            })
            .await;

        if let Err(e) = save_res {
            error!(
                "Failed to save version for {}: {:?}",
                prepared.region.name, e
            );
        } else {
            info!("Region {} complete.", prepared.region.name);
        }
    }

    Ok(())
}

async fn prepare_region(
    region: &RegionConfig,
    config: &Config,
    base_args: &Args,
    version_manager: &VersionManager,
    is_first_region: bool,
) -> Result<Option<PreparedRegion>> {
    // Option::None means skip
    info!("Processing region: {}", region.name);

    let filename = region
        .url
        .split('/')
        .last()
        .unwrap_or("unknown.osm.pbf")
        .to_string();
    let raw_pbf = config.global.tmp_dir.join(&filename);

    // 1. Download
    if !raw_pbf.exists() {
        info!("Downloading {}...", region.name);
        let status = Command::new("curl")
            .args(["-L", "-o", raw_pbf.to_str().unwrap(), &region.url])
            .status()
            .context("Failed to run curl")?;

        if !status.success() {
            warn!("Failed to download {}. Skipping.", region.name);
            return Ok(None);
        }
    } else {
        info!("File {} exists.", filename);
    }

    // 2. Version Check
    let hash = calculate_file_hash(&raw_pbf)?;
    if version_manager
        .is_latest(&region.name, &filename, &hash)
        .await?
        && !base_args.refresh
    // Force refresh overrides version check
    {
        info!(
            "Region {} is up to date (hash match). Skipping.",
            region.name
        );
        return Ok(None);
    }

    // 3. Filter
    let filtered_pbf = config.global.tmp_dir.join(format!(
        "{}-filtered.osm.pbf",
        filename.trim_end_matches(".osm.pbf")
    ));

    let script_dir = std::env::current_dir()?.join("scripts");
    let filter_script = script_dir.join("filter_osm.sh");

    // Check availability logic
    if !filter_script.exists() {
        // Fallback or error?
        // Should likely error as filtering is key
        anyhow::bail!("Filter script not found at {:?}", filter_script);
    }

    info!("Filtering {}...", region.name);
    let status = Command::new(&filter_script)
        .arg(&raw_pbf)
        .arg(&filtered_pbf)
        .status()
        .context("Failed to run filter script")?;

    if !status.success() {
        warn!("Filtering failed for {}. Skipping.", region.name);
        return Ok(None);
    }

    // Admin filter
    let admins_pbf = config.global.tmp_dir.join(format!(
        "{}-admins.osm.pbf",
        filename.trim_end_matches(".osm.pbf")
    ));
    let admin_script = script_dir.join("filter_admins.sh");

    let admin_file_arg = if admin_script.exists() {
        info!("Filtering admins for {}...", region.name);
        let status = Command::new(&admin_script)
            .arg(&raw_pbf)
            .arg(&admins_pbf)
            .status()?;
        if status.success() {
            Some(admins_pbf)
        } else {
            warn!("Admin filtering failed, proceeding without separate admin file.");
            None
        }
    } else {
        None
    };

    // Prepare Args
    let mut args = base_args.clone();
    args.file = Some(filtered_pbf);
    args.admin_file = admin_file_arg;
    args.es_url = config.global.es_url.clone();

    if !is_first_region {
        args.create_index = false;
    }
    // If is_first_region is true, args.create_index remains whatever base_args has.

    Ok(Some(PreparedRegion {
        region: region.clone(),
        filename,
        hash,
        args,
        import_start: Utc::now(),
    }))
}
