# DICOM Miner

`dicom-miner` is a Rust CLI for one-pass DICOM to Parquet aggregation, intended for large-scale ingestion workflows.

This repository was ported from https://github.com/TidalPaladin/dicom-structs.

## Usage

Convert DICOM files directly to aggregated Parquet shards:

```text
Usage: dicom-miner [OPTIONS] <SOURCE_DIR> <OUTPUT_PATH>

Arguments:
  <SOURCE_DIR>   Directory of DICOM files to process
  <OUTPUT_PATH>  Base output path (creates shards like output.00000.parquet)

Options:
      --hash                       Include a hash of pixel data in the output (xxh3-64, seed=0)
  -t, --tag <TAG=VALUE>            Override a DICOM tag with a constant value
      --strict                     Fail fast if any DICOM file cannot be processed
  -s, --snake-case                 Convert DICOM tag names to snake case
      --sort <SORT>                Sort discovered files by inode, path, or not at all [default: inode] [possible values: inode, path, none]
  -S, --shard-size <SHARD_SIZE>    Size of each output shard in MB [default: 128]
  -c, --compression-level <LEVEL>  ZSTD compression level [default: 6] [possible values: 1..=22]
  -h, --help                       Print help
  -V, --version                    Print version
```

### Example

```bash
dicom-miner /path/to/dicoms /path/to/output.parquet --snake-case --hash --sort inode
```

This creates sharded output files such as `output.00000.parquet`, `output.00001.parquet`, and so on.

## Notes

- This repository intentionally uses the one-pass aggregation flow as the default and primary workflow.
- Output is metadata-only: `PixelData` is never written. When `--hash` is set, pixel bytes are read only to compute `PixelDataHash`.
- The prior two-pass flow (`dicom-to-parquet` then `collect-parquet`) is deprecated and not included in this repository.

## Benchmarks

Run the Criterion benchmark suite:

```bash
cargo bench --bench pipeline_bench
```

Benchmark reports are written under `target/criterion/` and include baseline-friendly comparisons for schema extraction, conversion/casting, and end-to-end write throughput.
