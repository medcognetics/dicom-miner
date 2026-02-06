use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dicom_miner::dicom::open_dicom;
use dicom_miner::parquet::{
    cast_record_to_utf8_schema, create_unified_schema_from_dicoms, dicom_to_record_batch,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum BenchSortMode {
    Inode,
    Path,
    None,
}

impl BenchSortMode {
    fn as_str(self) -> &'static str {
        match self {
            BenchSortMode::Inode => "inode",
            BenchSortMode::Path => "path",
            BenchSortMode::None => "none",
        }
    }
}

fn fixture_paths(multiplier: usize) -> Vec<PathBuf> {
    // Keep the default benchmark corpus schema-homogeneous so repeated runs
    // are stable and don't fail on conflicting VR representations.
    let fixture_files = ["pydicom/SC_rgb.dcm", "pydicom/SC_rgb.dcm"];
    let base = fixture_files
        .iter()
        .map(|file| dicom_test_files::path(file).expect("fixture is missing"))
        .collect::<Vec<_>>();

    let mut paths = Vec::with_capacity(base.len() * multiplier);
    for _ in 0..multiplier {
        paths.extend(base.iter().cloned());
    }
    paths
}

fn sort_paths(mut paths: Vec<PathBuf>, mode: BenchSortMode) -> Vec<PathBuf> {
    match mode {
        BenchSortMode::Inode => {
            paths.sort_by(|a, b| {
                let a_ino = a.metadata().ok().map(|m| m.ino());
                let b_ino = b.metadata().ok().map(|m| m.ino());
                a_ino.cmp(&b_ino).then_with(|| a.cmp(b))
            });
        }
        BenchSortMode::Path => paths.sort(),
        BenchSortMode::None => {}
    }
    paths
}

fn total_input_bytes(paths: &[PathBuf]) -> u64 {
    paths
        .iter()
        .filter_map(|path| path.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn convert_and_cast_all(
    paths: &[PathBuf],
    schema: &arrow::datatypes::Schema,
    hash_pixel_data: bool,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut batches = Vec::with_capacity(paths.len());
    for path in paths {
        let dicom = open_dicom(path, !hash_pixel_data).expect("failed to open fixture");
        let batch = dicom_to_record_batch(&dicom, hash_pixel_data, true)
            .expect("failed to convert fixture");
        let casted =
            cast_record_to_utf8_schema(&batch, schema).expect("failed to cast fixture record");
        batches.push(casted);
    }
    batches
}

fn write_batches(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &arrow::datatypes::Schema,
) -> u64 {
    let output = tempfile::NamedTempFile::new().expect("failed to allocate temp parquet");
    let file = File::create(output.path()).expect("failed to open temp parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(6).expect("invalid zstd level"),
        ))
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
        .expect("failed to create parquet writer");
    for batch in batches {
        writer.write(batch).expect("failed to write record batch");
    }
    writer.close().expect("failed to close parquet writer");
    std::fs::metadata(output.path())
        .expect("failed to stat output parquet")
        .len()
}

fn bench_schema_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_extraction");
    for sort_mode in [
        BenchSortMode::Inode,
        BenchSortMode::Path,
        BenchSortMode::None,
    ] {
        for hash in [false, true] {
            let paths = sort_paths(fixture_paths(6), sort_mode);
            let input_bytes = total_input_bytes(&paths);
            group.throughput(Throughput::Bytes(input_bytes));
            group.bench_with_input(
                BenchmarkId::new(
                    sort_mode.as_str(),
                    if hash { "hash_on" } else { "hash_off" },
                ),
                &paths,
                |b, paths| {
                    b.iter(|| {
                        create_unified_schema_from_dicoms(black_box(paths), true, hash, true)
                            .expect("schema extraction failed")
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_convert_cast(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_cast");
    for hash in [false, true] {
        let paths = sort_paths(fixture_paths(6), BenchSortMode::Inode);
        let input_bytes = total_input_bytes(&paths);
        let schema = create_unified_schema_from_dicoms(&paths, true, hash, true)
            .expect("schema extraction failed");
        group.throughput(Throughput::Bytes(input_bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(if hash { "hash_on" } else { "hash_off" }),
            &paths,
            |b, paths| {
                b.iter(|| {
                    let batches = convert_and_cast_all(black_box(paths), &schema, hash);
                    black_box(batches.len())
                });
            },
        );
    }
    group.finish();
}

fn bench_end_to_end_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end_write");
    for sort_mode in [
        BenchSortMode::Inode,
        BenchSortMode::Path,
        BenchSortMode::None,
    ] {
        for hash in [false, true] {
            let paths = sort_paths(fixture_paths(6), sort_mode);
            let input_bytes = total_input_bytes(&paths);
            group.throughput(Throughput::Bytes(input_bytes));
            group.bench_with_input(
                BenchmarkId::new(
                    sort_mode.as_str(),
                    if hash { "hash_on" } else { "hash_off" },
                ),
                &paths,
                |b, paths| {
                    b.iter(|| {
                        let schema = create_unified_schema_from_dicoms(paths, true, hash, true)
                            .expect("schema extraction failed");
                        let batches = convert_and_cast_all(paths, &schema, hash);
                        let output_bytes = write_batches(&batches, &schema);
                        black_box(output_bytes)
                    });
                },
            );
        }
    }
    group.finish();
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10))
}

criterion_group!(
    name = benches;
    config = benchmark_config();
    targets = bench_schema_extraction, bench_convert_cast, bench_end_to_end_write
);
criterion_main!(benches);
