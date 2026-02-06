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
      --header-only                Don't include pixel data in the output
      --hash                       Include a hash of the pixel data in the output (xxh3-64, seed=0)
  -t, --tag <TAG=VALUE>            Override a DICOM tag with a constant value
  -s, --snake-case                 Convert DICOM tag names to snake case
  -S, --shard-size <SHARD_SIZE>    Size of each output shard in MB [default: 128]
  -c, --compression-level <LEVEL>  ZSTD compression level [default: 6] [possible values: 1..=22]
  -h, --help                       Print help
  -V, --version                    Print version
```

### Example

```bash
dicom-miner /path/to/dicoms /path/to/output.parquet --header-only --snake-case --hash
```

This creates sharded output files such as `output.00000.parquet`, `output.00001.parquet`, and so on.

## Notes

- This repository intentionally uses the one-pass aggregation flow as the default and primary workflow.
- The prior two-pass flow (`dicom-to-parquet` then `collect-parquet`) is deprecated and not included in this repository.
