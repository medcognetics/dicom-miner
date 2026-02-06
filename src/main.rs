use arrow::datatypes::{DataType, Field, Schema};
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, Receiver, Sender};
use dicom::core::dictionary::DataDictionaryEntry;
use dicom::core::header::Tag;
use dicom::core::{DataDictionary, DataElement, VR};
use dicom::dictionary_std::StandardDataDictionary;
use dicom_miner::dicom::{is_dicom_file, open_dicom};
use dicom_miner::parquet::{
    cast_record_to_utf8_schema, create_unified_schema_from_dicoms, dicom_to_record_batch,
    snake_case as dicom_tag_to_snake_case,
};
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rust_search::SearchBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use thiserror::Error as ThisError;

const CHANNEL_SIZE: usize = 1024;

#[derive(ThisError, Debug)]
enum AggregateError {
    #[error("Core error: {0}")]
    Core(#[from] dicom_miner::error::Error),
    #[error("Output directory does not exist: {0}")]
    MissingOutputDirectory(PathBuf),
    #[error("No DICOM files found in source directory")]
    NoDicomFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum SortMode {
    Inode,
    Path,
    None,
}

#[derive(Debug)]
struct RunSummary {
    shard_paths: Vec<PathBuf>,
    discovered_files: usize,
    processed_files: usize,
    failed_files: usize,
}

struct ConversionSummary {
    shard_paths: Vec<PathBuf>,
    processed_files: usize,
    failed_files: usize,
}

/// Find the files to be processed
fn find_dicom_files(dir: &Path) -> impl Iterator<Item = PathBuf> {
    SearchBuilder::default()
        .location(dir)
        .build()
        .map(PathBuf::from)
        .filter(move |file| is_dicom_file(file, false) && file.is_file())
}

fn sort_dicom_files(files: Vec<PathBuf>, sort_mode: SortMode) -> Vec<PathBuf> {
    match sort_mode {
        SortMode::Inode => {
            let mut metadata_failures = 0usize;
            let mut keyed_paths = files
                .into_iter()
                .map(|path| {
                    let inode = match path.metadata() {
                        Ok(metadata) => Some(metadata.ino()),
                        Err(_) => {
                            metadata_failures += 1;
                            None
                        }
                    };
                    (inode, path)
                })
                .collect::<Vec<_>>();

            if metadata_failures > 0 {
                warn!(
                    "Failed to read metadata for {} files while inode sorting; falling back to path ordering for those entries",
                    metadata_failures
                );
            }

            keyed_paths.sort_by(|(a_ino, a_path), (b_ino, b_path)| {
                a_ino.cmp(b_ino).then_with(|| a_path.cmp(b_path))
            });
            keyed_paths.into_iter().map(|(_, path)| path).collect()
        }
        SortMode::Path => {
            let mut files = files;
            files.sort();
            files
        }
        SortMode::None => files,
    }
}

/// Get the path to a shard file
#[inline]
fn shard_path(path: &Path, index: usize) -> PathBuf {
    let shard_extension = format!("{:05}.parquet", index);
    path.with_extension("").with_extension(shard_extension)
}

/// Opens a new shard file
#[inline]
fn open_output_shard(path: &Path, index: usize) -> Result<File, dicom_miner::error::Error> {
    let shard_path = shard_path(path, index);
    File::create(shard_path).map_err(|e| dicom_miner::error::Error::Whatever {
        message: format!("Failed to create output file: {}", e),
        source: Some(Box::new(e)),
    })
}

fn resolve_tag_overrides(
    tags: &[(String, String)],
) -> Result<HashMap<Tag, String>, dicom_miner::error::Error> {
    let mut overrides = HashMap::with_capacity(tags.len());
    for (tag_name, value) in tags {
        let tag = StandardDataDictionary
            .by_name(tag_name)
            .ok_or_else(|| dicom_miner::error::Error::TagNotFound {
                tag_name: tag_name.clone(),
            })?
            .tag();
        overrides.insert(tag, value.clone());
    }
    Ok(overrides)
}

fn tag_field_name(tag: Tag, snake_case: bool) -> String {
    let name = StandardDataDictionary
        .by_tag(tag)
        .map_or_else(|| tag.to_string(), |entry| entry.alias().to_string());
    if snake_case {
        dicom_tag_to_snake_case(&name)
    } else {
        name
    }
}

fn add_override_fields_to_schema(
    schema: &Schema,
    overrides: Option<&HashMap<Tag, String>>,
    snake_case: bool,
) -> Schema {
    let Some(overrides) = overrides else {
        return schema.clone();
    };

    let mut existing_names = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<HashSet<_>>();
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();

    for tag in overrides.keys() {
        let field_name = tag_field_name(*tag, snake_case);
        if existing_names.insert(field_name.clone()) {
            fields.push(Field::new(field_name, DataType::Utf8, true));
        }
    }

    Schema::new(fields)
}

fn convert_single_dicom(
    dicom_path: &Path,
    unified_schema: &arrow::datatypes::Schema,
    hash_pixel_data: bool,
    overrides: Option<&HashMap<Tag, String>>,
    snake_case: bool,
) -> Result<arrow::record_batch::RecordBatch, dicom_miner::error::Error> {
    let mut dicom = open_dicom(dicom_path, !hash_pixel_data).map_err(|e| {
        dicom_miner::error::Error::Whatever {
            message: format!("Failed to open DICOM file {}: {}", dicom_path.display(), e),
            source: Some(Box::new(e)),
        }
    })?;

    if let Some(overrides) = overrides {
        for (tag, value) in overrides {
            dicom.put(DataElement::new(*tag, VR::LO, value.clone()));
        }
    }

    let batch = dicom_to_record_batch(&dicom, hash_pixel_data, snake_case)?;
    cast_record_to_utf8_schema(&batch, unified_schema)
}

/// Convert DICOM files to RecordBatches and stream to shard writers
#[allow(clippy::too_many_arguments)]
fn convert_and_aggregate_dicoms(
    dicom_paths: &[PathBuf],
    output_path: &Path,
    unified_schema: &arrow::datatypes::Schema,
    hash_pixel_data: bool,
    overrides: Option<&HashMap<Tag, String>>,
    snake_case: bool,
    strict: bool,
    props: WriterProperties,
    shard_size_mb: usize,
) -> Result<ConversionSummary, dicom_miner::error::Error> {
    let pb = ProgressBar::new(dicom_paths.len() as u64);
    if let Ok(style) = ProgressStyle::default_bar()
        .template(
            "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec})",
        )
    {
        pb.set_style(style);
    }
    pb.set_message("Processing DICOM files");

    let (sender, receiver): (
        Sender<arrow::record_batch::RecordBatch>,
        Receiver<arrow::record_batch::RecordBatch>,
    ) = bounded(CHANNEL_SIZE);

    // Receiver thread (shard writer)
    let schema_clone = Arc::new(unified_schema.clone());
    let output_path_clone = output_path.to_path_buf();
    let props_clone = props.clone();
    let receiver_thread = thread::spawn(move || {
        let mut shard_idx = 0;
        let mut shards = vec![shard_path(&output_path_clone, shard_idx)];
        let shard_file = open_output_shard(&output_path_clone, shard_idx)?;
        let mut writer =
            ArrowWriter::try_new(shard_file, schema_clone.clone(), Some(props_clone.clone()))
                .map_err(|e| dicom_miner::error::Error::Whatever {
                    message: format!("Failed to create Arrow writer: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let max_size_bytes = shard_size_mb.saturating_mul(1024 * 1024);
        while let Ok(record) = receiver.recv() {
            // Check if we need a new shard
            let bytes_written = writer.bytes_written() + writer.in_progress_size();
            let need_new_shard = bytes_written > max_size_bytes
                || writer.in_progress_rows() >= props_clone.max_row_group_size();

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
                    schema_clone.clone(),
                    Some(props_clone.clone()),
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

    let processed_files = AtomicUsize::new(0);
    let failed_files = AtomicUsize::new(0);

    // Parallel producer: Convert DICOM -> RecordBatch -> Cast -> Send.
    // In best-effort mode, per-file failures are logged and skipped.
    let producer_result = dicom_paths.par_iter().progress_with(pb).try_for_each_with(
        sender.clone(),
        |s, dicom_path| match convert_single_dicom(
            dicom_path,
            unified_schema,
            hash_pixel_data,
            overrides,
            snake_case,
        ) {
            Ok(record) => {
                s.send(record)
                    .map_err(|e| dicom_miner::error::Error::Whatever {
                        message: format!("Failed to send record to writer thread: {}", e),
                        source: None,
                    })?;
                processed_files.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                if strict {
                    return Err(e);
                }
                failed_files.fetch_add(1, Ordering::Relaxed);
                warn!("Failed to process {}: {}", dicom_path.display(), e);
                Ok(())
            }
        },
    );
    drop(sender);

    let writer_result =
        receiver_thread
            .join()
            .map_err(|_| dicom_miner::error::Error::Whatever {
                message: "Writer thread panicked".to_string(),
                source: None,
            })?;

    if let Err(e) = &writer_result {
        warn!("Writer thread failed: {}", e);
    }
    if let Err(e) = &producer_result {
        warn!("Producer failed: {}", e);
    }

    producer_result?;
    let shard_paths = writer_result?;

    Ok(ConversionSummary {
        shard_paths,
        processed_files: processed_files.load(Ordering::Relaxed),
        failed_files: failed_files.load(Ordering::Relaxed),
    })
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
        long = "hash",
        help = "Include a hash of the pixel data in the output. Uses xxh3-64 with seed=0.",
        action = clap::ArgAction::SetTrue
    )]
    hash: bool,

    #[arg(short='t', long = "tag", value_parser = parse_key_val::<String, String>, help="Override a DICOM tag with a constant value")]
    tags: Vec<(String, String)>,

    #[arg(
        long = "strict",
        help = "Fail fast if any DICOM file cannot be processed",
        action = clap::ArgAction::SetTrue
    )]
    strict: bool,

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

    #[arg(
        long = "sort",
        value_enum,
        default_value_t = SortMode::Inode,
        help = "Sort discovered files by inode, path, or not at all"
    )]
    sort: SortMode,
}

fn run(args: Args) -> Result<RunSummary, AggregateError> {
    let start = Instant::now();

    info!("Starting DICOM aggregation");
    info!("Source directory: {:?}", args.source_dir);
    info!("Output path: {:?}", args.output_path);
    info!("Hash: {:?}", args.hash);
    info!("Tags: {:?}", args.tags);
    info!("Strict mode: {:?}", args.strict);
    info!("Snake case: {:?}", args.snake_case);
    info!("Sort mode: {:?}", args.sort);
    info!("Shard size: {:?}MB", args.shard_size);
    info!("ZSTD compression level: {:?}", args.compression_level);

    // Validate output directory
    if let Some(parent) = args.output_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(AggregateError::MissingOutputDirectory(parent.to_path_buf()));
        }
    }

    // Phase 1: Find and sort DICOM files
    info!("Finding DICOM files...");
    let dicom_files: Vec<PathBuf> = find_dicom_files(&args.source_dir).collect();
    let dicom_files = sort_dicom_files(dicom_files, args.sort);
    info!("Found {} DICOM files", dicom_files.len());

    if dicom_files.is_empty() {
        return Err(AggregateError::NoDicomFiles);
    }

    let overrides = resolve_tag_overrides(&args.tags)?;
    let overrides = if overrides.is_empty() {
        None
    } else {
        Some(overrides)
    };

    // Phase 2: Extract unified schema (parallel)
    info!("Extracting unified schema from DICOM headers...");
    let unified_schema = add_override_fields_to_schema(
        &create_unified_schema_from_dicoms(&dicom_files, args.snake_case, args.hash, args.strict)?,
        overrides.as_ref(),
        args.snake_case,
    );

    info!(
        "Unified schema has {} fields",
        unified_schema.fields().len()
    );
    for (i, field) in unified_schema.fields().iter().enumerate() {
        debug!(
            "Field {}: {} ({:?}, nullable={})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    // Phase 3: Convert and aggregate (streaming)
    info!("Converting DICOM files and writing to Parquet shards...");
    let overrides_ref = overrides.as_ref();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(args.compression_level).map_err(|e| {
                dicom_miner::error::Error::Whatever {
                    message: format!("Invalid ZSTD compression level: {}", e),
                    source: Some(Box::new(e)),
                }
            })?,
        ))
        .build();

    let conversion_summary = convert_and_aggregate_dicoms(
        &dicom_files,
        &args.output_path,
        &unified_schema,
        args.hash,
        overrides_ref,
        args.snake_case,
        args.strict,
        props,
        args.shard_size,
    )?;

    let elapsed = start.elapsed();
    info!(
        "Processed {} files ({} failed) out of {} discovered files in {:?}",
        conversion_summary.processed_files,
        conversion_summary.failed_files,
        dicom_files.len(),
        elapsed
    );
    info!("Created {} shard(s)", conversion_summary.shard_paths.len());
    for (i, shard) in conversion_summary.shard_paths.iter().enumerate() {
        info!("  Shard {}: {}", i, shard.display());
    }

    Ok(RunSummary {
        shard_paths: conversion_summary.shard_paths,
        discovered_files: dicom_files.len(),
        processed_files: conversion_summary.processed_files,
        failed_files: conversion_summary.failed_files,
    })
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    match run(args) {
        Ok(summary) => {
            info!(
                "Run complete: discovered={}, processed={}, failed={}, shards={}",
                summary.discovered_files,
                summary.processed_files,
                summary.failed_files,
                summary.shard_paths.len()
            );
        }
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run, AggregateError, Args, SortMode};
    use arrow::array::{Array, StringArray};
    use dicom_miner::error::Error as CoreError;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    fn copy_fixture(temp_input: &tempfile::TempDir, fixture: &str) {
        let src = dicom_test_files::path(fixture).unwrap();
        let dst = temp_input.path().join(fixture);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::copy(src, dst).unwrap();
    }

    #[test]
    fn test_end_to_end_conversion() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();

        // Copy test DICOM files
        let test_files = vec!["pydicom/SC_rgb.dcm", "pydicom/CT_small.dcm"];

        for file in test_files {
            copy_fixture(&temp_input, file);
        }

        // Run conversion
        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            hash: true,
            tags: vec![],
            strict: false,
            snake_case: true,
            shard_size: 128,
            compression_level: 6,
            sort: SortMode::Inode,
        };

        let summary = run(args).unwrap();

        // Verify output
        assert!(!summary.shard_paths.is_empty());
        assert_eq!(summary.discovered_files, 2);
        assert_eq!(summary.processed_files, 2);
        assert_eq!(summary.failed_files, 0);
        for shard in summary.shard_paths {
            assert!(shard.exists());
        }
    }

    #[test]
    fn test_invalid_tag_override_fails_fast() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();
        copy_fixture(&temp_input, "pydicom/SC_rgb.dcm");

        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            hash: false,
            tags: vec![("TotallyInvalidTag".to_string(), "value".to_string())],
            strict: false,
            snake_case: false,
            shard_size: 128,
            compression_level: 6,
            sort: SortMode::Inode,
        };

        let result = run(args);
        assert!(matches!(
            result,
            Err(AggregateError::Core(CoreError::TagNotFound { .. }))
        ));
    }

    #[test]
    fn test_best_effort_skips_invalid_dicom() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();
        copy_fixture(&temp_input, "pydicom/SC_rgb.dcm");
        std::fs::write(temp_input.path().join("invalid.dcm"), b"not a dicom").unwrap();

        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            hash: false,
            tags: vec![],
            strict: false,
            snake_case: true,
            shard_size: 128,
            compression_level: 6,
            sort: SortMode::Path,
        };

        let summary = run(args).unwrap();
        assert_eq!(summary.discovered_files, 2);
        assert_eq!(summary.processed_files, 1);
        assert_eq!(summary.failed_files, 1);
        assert!(!summary.shard_paths.is_empty());
    }

    #[test]
    fn test_strict_mode_fails_on_invalid_dicom() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();
        copy_fixture(&temp_input, "pydicom/SC_rgb.dcm");
        std::fs::write(temp_input.path().join("invalid.dcm"), b"not a dicom").unwrap();

        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            hash: false,
            tags: vec![],
            strict: true,
            snake_case: true,
            shard_size: 128,
            compression_level: 6,
            sort: SortMode::None,
        };

        assert!(run(args).is_err());
    }

    #[test]
    fn test_override_field_is_preserved_in_unified_schema() {
        let temp_input = tempfile::tempdir().unwrap();
        let temp_output = tempfile::tempdir().unwrap();
        copy_fixture(&temp_input, "pydicom/SC_rgb.dcm");

        let args = Args {
            source_dir: temp_input.path().to_path_buf(),
            output_path: temp_output.path().join("output.parquet"),
            hash: false,
            tags: vec![("DataSetName".to_string(), "dataset".to_string())],
            strict: true,
            snake_case: false,
            shard_size: 128,
            compression_level: 6,
            sort: SortMode::None,
        };

        let summary = run(args).unwrap();
        assert_eq!(summary.processed_files, 1);
        assert!(!summary.shard_paths.is_empty());

        let mut found_override = false;
        for shard in &summary.shard_paths {
            let file = File::open(shard).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let reader = builder.build().unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let index = batch.schema().index_of("DataSetName").unwrap();
                let values = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..values.len() {
                    assert_eq!(values.value(i), "dataset");
                    found_override = true;
                }
            }
        }

        assert!(found_override, "Expected injected override value in output");
    }
}
