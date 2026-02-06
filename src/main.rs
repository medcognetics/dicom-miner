use clap::Parser;
use crossbeam_channel::{bounded, Receiver, Sender};
use dicom::core::dictionary::DataDictionaryEntry;
use dicom::core::DataElement;
use dicom::core::{DataDictionary, VR};
use dicom::dictionary_std::StandardDataDictionary;
use dicom_miner::dicom::{is_dicom_file, open_dicom};
use dicom_miner::parquet::{
    cast_record_to_utf8_schema, create_unified_schema_from_dicoms, dicom_to_record_batch,
};
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use log::{error, info, warn};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rust_search::SearchBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::io::Error;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use thiserror::Error as ThisError;

const CHANNEL_SIZE: usize = 1024;

#[derive(ThisError, Debug)]
enum AggregateError {
    #[error("IO error: {0}")]
    Io(#[from] Error),
    #[error("Core error: {0}")]
    Core(#[from] dicom_miner::error::Error),
    #[error("Other error: {0}")]
    Other(String),
}

/// Find the files to be processed with a progress bar
fn find_dicom_files(dir: &PathBuf) -> impl Iterator<Item = PathBuf> {
    // Set up spinner, iterating may files may take some time
    let spinner = ProgressBar::new_spinner();
    spinner.set_message("Searching for DICOM files");
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.blue} {msg}")
            .unwrap(),
    );

    // Yield from the search
    SearchBuilder::default()
        .location(dir)
        .build()
        .inspect(move |_| spinner.tick())
        .map(PathBuf::from)
        .filter(move |file| is_dicom_file(file, false) && file.is_file())
}

fn sort_by_inode(files: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut files = files;
    let spinner = ProgressBar::new_spinner();
    spinner.set_message("Sorting files by inode");
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.blue} {msg}")
            .unwrap(),
    );
    files.sort_by_key(|f| {
        spinner.tick();
        f.metadata().unwrap().ino()
    });
    spinner.finish_and_clear();
    files
}

/// Get the path to a shard file
#[inline]
fn shard_path(path: &PathBuf, index: usize) -> PathBuf {
    let shard_extension = format!("{:05}.parquet", index);
    path.with_extension("").with_extension(shard_extension)
}

/// Opens a new shard file
#[inline]
fn open_output_shard(path: &PathBuf, index: usize) -> Result<File, dicom_miner::error::Error> {
    let shard_path = shard_path(path, index);
    File::create(shard_path).map_err(|e| dicom_miner::error::Error::Whatever {
        message: format!("Failed to create output file: {}", e),
        source: Some(Box::new(e)),
    })
}

/// Convert DICOM files to RecordBatches and stream to shard writers
fn convert_and_aggregate_dicoms(
    dicom_paths: &[PathBuf],
    output_path: &PathBuf,
    unified_schema: &arrow::datatypes::Schema,
    header_only: bool,
    hash_pixel_data: bool,
    overrides: Option<&HashMap<String, String>>,
    snake_case: bool,
    props: WriterProperties,
    shard_size_mb: usize,
) -> Result<Vec<PathBuf>, dicom_miner::error::Error> {
    let pb = ProgressBar::new(dicom_paths.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec})",
            )
            .unwrap(),
    );
    pb.set_message("Processing DICOM files");

    let (sender, receiver): (
        Sender<arrow::record_batch::RecordBatch>,
        Receiver<arrow::record_batch::RecordBatch>,
    ) = bounded(CHANNEL_SIZE);

    // Receiver thread (shard writer)
    let schema_clone = unified_schema.clone();
    let output_path_clone = output_path.clone();
    let receiver_thread = thread::spawn(move || {
        let mut shard_idx = 0;
        let mut shards = vec![shard_path(&output_path_clone, shard_idx)];
        let shard_file = open_output_shard(&output_path_clone, shard_idx)?;
        let mut writer = ArrowWriter::try_new(
            shard_file,
            Arc::new(schema_clone.clone()),
            Some(props.clone()),
        )
        .map_err(|e| dicom_miner::error::Error::Whatever {
            message: format!("Failed to create Arrow writer: {}", e),
            source: Some(Box::new(e)),
        })?;

        let max_size_bytes = shard_size_mb * 1024 * 1024;
        while let Ok(record) = receiver.recv() {
            // Check if we need a new shard
            let bytes_written = writer.bytes_written() + writer.in_progress_size();
            let need_new_shard = bytes_written > max_size_bytes
                || writer.in_progress_rows() > props.max_row_group_size();

            if need_new_shard {
                writer
                    .close()
                    .map_err(|e| dicom_miner::error::Error::Whatever {
                        message: format!("Failed to close writer: {}", e),
                        source: Some(Box::new(e)),
                    })?;

                shard_idx += 1;
                shards.push(shard_path(&output_path_clone, shard_idx));
                let shard_file = open_output_shard(&output_path_clone, shard_idx)?;
                writer = ArrowWriter::try_new(
                    shard_file,
                    Arc::new(schema_clone.clone()),
                    Some(props.clone()),
                )
                .map_err(|e| dicom_miner::error::Error::Whatever {
                    message: format!("Failed to create Arrow writer: {}", e),
                    source: Some(Box::new(e)),
                })?;
            }

            writer
                .write(&record)
                .map_err(|e| dicom_miner::error::Error::Whatever {
                    message: format!("Failed to write batch: {}", e),
                    source: Some(Box::new(e)),
                })?;
        }

        writer
            .close()
            .map_err(|e| dicom_miner::error::Error::Whatever {
                message: format!("Failed to close writer: {}", e),
                source: Some(Box::new(e)),
            })?;
        Ok(shards)
    });

    // Parallel producer: Convert DICOM -> RecordBatch -> Cast -> Send
    dicom_paths
        .par_iter()
        .progress_with(pb)
        .filter_map(|dicom_path| {
            // Open DICOM
            let mut dicom = match open_dicom(dicom_path, header_only && !hash_pixel_data) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Failed to open {}: {}", dicom_path.display(), e);
                    return None;
                }
            };

            // Apply tag overrides
            if let Some(overrides) = overrides {
                for (tag, value) in overrides {
                    if let Some(dict_entry) = StandardDataDictionary::default().by_name(tag) {
                        let tag = dict_entry.tag();
                        dicom.put(DataElement::new(tag, VR::LO, value.to_string()));
                    }
                }
            }

            // Convert to RecordBatch
            let batch =
                match dicom_to_record_batch(&dicom, header_only, hash_pixel_data, snake_case) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Failed to convert {}: {}", dicom_path.display(), e);
                        return None;
                    }
                };

            // Cast to unified UTF8 schema
            match cast_record_to_utf8_schema(&batch, unified_schema) {
                Ok(casted) => Some(casted),
                Err(e) => {
                    warn!("Failed to cast {}: {}", dicom_path.display(), e);
                    None
                }
            }
        })
        .for_each_with(sender, |s, record| {
            s.send(record).expect("Failed to send record");
        });

    // Wait for writer thread to finish
    receiver_thread.join().unwrap()
}

// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[derive(Parser, Debug)]
#[command(
    author = "Scott Chase Waggener",
    version = "0.1.0",
    about = "Convert DICOM files directly to aggregated Parquet",
    long_about = None
)]
struct Args {
    #[arg(help = "Directory of DICOM files to process")]
    source_dir: PathBuf,

    #[arg(help = "Path to write output to")]
    output_path: PathBuf,

    #[arg(
        long = "header-only",
        help = "Don't include pixel data in the output",
        action = clap::ArgAction::SetTrue
    )]
    header_only: bool,

    #[arg(
        long = "hash",
        help = "Include a hash of the pixel data in the output. Uses xxh3-64 with seed=0.",
        action = clap::ArgAction::SetTrue
    )]
    hash: bool,

    #[arg(short='t', long = "tag", value_parser = parse_key_val::<String, String>, help="Override a DICOM tag with a constant value")]
    tags: Vec<(String, String)>,

    #[arg(
        short = 's',
        long = "snake-case",
        help = "Convert DICOM tag names to snake case",
        action = clap::ArgAction::SetTrue
    )]
    snake_case: bool,

    #[arg(
        short = 'S',
        long = "shard-size",
        help = "Size of each Parquet output shard in MB",
        default_value = "128"
    )]
    shard_size: usize,

    #[arg(
        short = 'c',
        long = "compression-level",
        help = "ZSTD compression level",
        default_value = "6",
        value_parser = clap::value_parser!(i32).range(1..=22)
    )]
    compression_level: i32,
}

fn run(args: Args) -> Result<Vec<PathBuf>, AggregateError> {
    let start = Instant::now();

    info!("Starting DICOM aggregation");
    info!("Source directory: {:?}", args.source_dir);
    info!("Output path: {:?}", args.output_path);
    info!("Header only: {:?}", args.header_only);
    info!("Hash: {:?}", args.hash);
    info!("Tags: {:?}", args.tags);
    info!("Snake case: {:?}", args.snake_case);
    info!("Shard size: {:?}MB", args.shard_size);
    info!("ZSTD compression level: {:?}", args.compression_level);

    // Validate output directory
    if let Some(parent) = args.output_path.parent() {
        if !parent.exists() {
            error!("Output directory does not exist: {:?}", parent);
            std::process::exit(1);
        }
    }

    // Phase 1: Find and sort DICOM files
    info!("Finding DICOM files...");
    let dicom_files: Vec<PathBuf> = find_dicom_files(&args.source_dir).collect();
    let dicom_files = sort_by_inode(dicom_files);
    info!("Found {} DICOM files", dicom_files.len());

    if dicom_files.is_empty() {
        error!("No DICOM files found in source directory");
        std::process::exit(1);
    }

    // Phase 2: Extract unified schema (parallel)
    info!("Extracting unified schema from DICOM headers...");
    let unified_schema =
        create_unified_schema_from_dicoms(&dicom_files, args.snake_case, args.hash)?;

    info!(
        "Unified schema has {} fields",
        unified_schema.fields().len()
    );
    for (i, field) in unified_schema.fields().iter().enumerate() {
        info!(
            "Field {}: {} ({:?}, nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    // Phase 3: Convert and aggregate (streaming)
    info!("Converting DICOM files and writing to Parquet shards...");
    let overrides = HashMap::from_iter(args.tags.into_iter());
    let overrides = if overrides.is_empty() {
        None
    } else {
        Some(&overrides)
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(args.compression_level).unwrap(),
        ))
        .build();

    let shard_paths = convert_and_aggregate_dicoms(
        &dicom_files,
        &args.output_path,
        &unified_schema,
        args.header_only,
        args.hash,
        overrides,
        args.snake_case,
        props,
        args.shard_size,
    )?;

    let elapsed = start.elapsed();
    info!("Processed {} files in {:?}", dicom_files.len(), elapsed);
    info!("Created {} shard(s)", shard_paths.len());
    for (i, shard) in shard_paths.iter().enumerate() {
        info!("  Shard {}: {}", i, shard.display());
    }

    Ok(shard_paths)
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    run(args).unwrap();
}

#[cfg(test)]
mod tests {
    use super::{run, Args};

    #[test]
    fn test_end_to_end_conversion() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();

        // Copy test DICOM files
        let test_files = vec!["pydicom/SC_rgb.dcm", "pydicom/CT_small.dcm"];

        for file in test_files {
            let src = dicom_test_files::path(file).unwrap();
            let dst = temp_input.path().join(file);
            std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
            std::fs::copy(src, dst).unwrap();
        }

        // Run conversion
        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            header_only: true,
            hash: true,
            tags: vec![],
            snake_case: true,
            shard_size: 128,
            compression_level: 6,
        };

        let shards = run(args).unwrap();

        // Verify output
        assert!(shards.len() > 0);
        for shard in shards {
            assert!(shard.exists());
        }
    }
}
