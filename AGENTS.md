# AGENTS.md

This file provides guidance for coding agents working in this repository.

## Project Overview

`dicom-miner` is a Rust project for converting DICOM (medical imaging) files directly into sharded Parquet output using a one-pass aggregation pipeline.

This repository was ported from `https://github.com/TidalPaladin/dicom-structs` and intentionally keeps only the one-pass workflow.

## Repository Structure

- `src/main.rs` - CLI entrypoint (`dicom-miner`) implementing the end-to-end one-pass pipeline.
- `src/lib.rs` - Library module exports.
- `src/dicom.rs` - DICOM file discovery/opening and pixel hash helpers.
- `src/parquet.rs` - DICOM-to-Arrow/Parquet conversion, schema extraction/merge, casting.
- `src/error.rs` - Shared error types.

## Common Commands

### Build

```bash
cargo build --release
```

### Test

```bash
cargo test
```

### Run CLI

```bash
cargo run --release -- <SOURCE_DIR> <OUTPUT_PATH> [OPTIONS]
```

Example:

```bash
cargo run --release -- /path/to/dicoms /path/to/output.parquet --header-only --snake-case --hash
```

## Data Flow

1. Find DICOM files recursively.
2. Sort file list by inode.
3. Extract schemas from DICOM headers in parallel.
4. Merge to a unified UTF8-compatible schema.
5. Convert each DICOM to a `RecordBatch` in parallel.
6. Cast each batch to the unified schema.
7. Stream batches to a writer thread that emits sharded Parquet files.

Shard naming follows `output.00000.parquet`, `output.00001.parquet`, etc.

## CLI Options

- `--header-only` - Exclude pixel data from output.
- `--hash` - Add `PixelDataHash` / `pixel_data_hash` (xxh3-64, seed=0).
- `-t, --tag <TAG=VALUE>` - Override DICOM tags with constant values (repeatable).
- `-s, --snake-case` - Convert DICOM tag names to snake_case.
- `-S, --shard-size <MB>` - Target shard size in MB (default `128`).
- `-c, --compression-level <1-22>` - ZSTD compression level (default `6`).

## Testing Notes

Tests rely on `dicom-test-files` crate fixtures (for example `pydicom/SC_rgb.dcm`).

When changing schema conversion or DICOM parsing behavior, run full `cargo test` and verify the end-to-end test in `src/main.rs` still produces shard files.
