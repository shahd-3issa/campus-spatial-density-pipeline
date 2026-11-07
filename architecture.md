# Campus Spatial Density & Attendance Pipeline

Synthetic telemetry only. This repository must never ingest real student locations.

## What the pipeline will do

Campus devices emit pings: who (a synthetic id), where (building and zone), and when.
The pipeline will generate those events, reject records that cannot be trusted,
write the accepted set as partitioned Parquet, and record each run in SQLite.

```text
synthetic generator
        |
        v
   raw files            immutable after write
        |
        v
   validator
     |     \
     |      +--> quarantine (row + rejection reason)
     v
   Polars transforms
     |-- duplicate detection
     |-- timestamp-gap analysis
     v
   partitioned Parquet     event_date=/building_id=
     |
     v
   SQLite run metadata     status, seed, counts, retries, backfill window
```

`main.py` will stay a thin entrypoint. Business rules stay in `src/`.

## Review of the original layout

The requested tree is a reasonable single-package layout for a junior portfolio
project. These are the weaknesses to design against while keeping that tree:

| Weakness | Why it matters | Decision for this project |
| --- | --- | --- |
| One orchestrator module tends to absorb every step | Retries, backfill, I/O, and quality rules become untestable | Orchestrator only sequences steps. Each step stays in its own module. |
| No shared schema | Generator, validator, and Parquet can drift apart | `TELEMETRY_SCHEMA` lives in `src/telemetry_generator.py`. The validator will import it. |
| Raw "immutability" is only a folder name | A rerun can overwrite evidence | `write_raw_telemetry` claims `telemetry_<run_id>.parquet` with an exclusive create and leaves an existing file untouched. |
| Retries without an output identity | A retry can append the same partition twice | Each partition write targets a deterministic path and replaces that partition's file for the run, instead of appending blindly. |
| Three dataframe libraries on one hot path | Polars → Pandas → PyArrow copies memory and hides the real cost | Polars processes data. PyArrow is the Parquet layer. Pandas is allowed only at an explicit boundary. |
| Partition grain is unspecified | Partitioning by minute, device, or student creates tiny files | Hive partitions are `event_date` then `building_id`. |
| SQLite treated like a warehouse | SQLite has one writer and weak concurrent access | One pipeline process writes metadata. SQLite stores run state, not telemetry facts. |
| Quarantine without a reason | Invalid rows cannot be explained in a review | Quarantine records keep the original fields plus `rejection_reason` and `run_id`. |
| Reproducibility left implicit | A demo cannot be repeated or defended | Seed, config snapshot, and input file hashes will be stored on the run row. |
| Package name `src` | Imports read as `import src...`, which is awkward outside this repo | Acceptable for this layout. A publishable rename would be `campus_pipeline`. |

## Module boundaries

| Path | Responsibility | Milestone |
| --- | --- | --- |
| `config/settings.py` | Paths, seed, retry policy, quality thresholds, compression, JSON snapshot | 1 |
| `src/telemetry_generator.py` | Deterministic synthetic events and immutable raw output | 2 |
| `src/db_metadata.py` | SQLite run registry (state only, not telemetry facts) | 2 |
| `src/data_validator.py` | Schema checks, duplicates, timestamp gaps, quarantine | 3 |
| Partitioned Parquet writer | `event_date` / `building_id` store | 4 |
| `src/pipeline_orchestrator.py` | Idempotent runs, retries, backfill execution | 5 |
| Benchmark script | Time, peak memory, and bytes on disk for a stated input size | 6 |

`src/data_validator.py` and `src/pipeline_orchestrator.py` are still placeholders.
The registry can store an attempt number and a backfill window. It does not retry or backfill by itself.

## Configuration contract

`load_settings()` reads environment variables and, if present, a `.env` file.
Already-set environment variables win over `.env`. Relative paths resolve
against the repository root, not the shell's current directory.
`load_settings()` does not create directories. `ensure_data_directories()` does,
and it does not delete or rewrite existing files.

| Variable | Default | Constraint |
| --- | --- | --- |
| `CAMPUS_DATA_DIR` | `<root>/data` | Used when a specific path is unset |
| `CAMPUS_RAW_DIR` | `<data>/raw` | |
| `CAMPUS_QUARANTINE_DIR` | `<data>/quarantine` | |
| `CAMPUS_PARTITIONED_STORE_DIR` | `<data>/partitioned_store` | |
| `CAMPUS_METADATA_DB_PATH` | `<data>/pipeline_metadata.sqlite` | |
| `CAMPUS_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `CAMPUS_RANDOM_SEED` | `42` | Integer |
| `CAMPUS_MAX_RETRIES` | `3` | Integer `>= 0` |
| `CAMPUS_RETRY_BACKOFF_SECONDS` | `0.5` | Number `>= 0` |
| `CAMPUS_DEFAULT_RECORD_COUNT` | `10000` | Integer `>= 1` |
| `CAMPUS_PING_INTERVAL_SECONDS` | `60` | Integer `>= 1` |
| `CAMPUS_GAP_THRESHOLD_SECONDS` | `300` | Integer `>= 1` |
| `CAMPUS_PARQUET_COMPRESSION` | `zstd` | `zstd`, `snappy`, `gzip`, `uncompressed` |

No secret is required. If a future integration needs a token, it must come from
the environment and must not be logged.

## Dependency and runtime notes

Checked on this machine: CPython 3.14.3, pip 25.3.

| Package | Pinned | Role |
| --- | --- | --- |
| `polars` | 2.1.0 | Lazy/eager transforms and Parquet scan |
| `pyarrow` | 26.0.1 | Parquet writer used when we need explicit dataset partitioning |
| `pandas` | 3.0.6 | Optional boundary only. Already installed on the base interpreter. |
| `pytest` | 8.4.2 | Tests |

SQLite comes from the Python standard library (`sqlite3`).

Compatibility notes, not yet benchmarked:

- Polars 1.x and 2.x differ in some `write_parquet` / sink APIs. Code will target 2.1 and will not claim 1.x compatibility.
- Pandas 3 uses PyArrow-backed dtypes by default. Passing a Pandas frame through PyArrow and then into Polars can change null handling and timezone storage. The processing path should stay in Polars.
- A global site-packages warning (`Ignoring invalid distribution ~ip`) comes from the base interpreter, not this project. Use a virtual environment at `.venv/` so installs stay isolated.

## Failure handling we will implement later

- Validation failures are data errors. They go to quarantine and are not retried.
- I/O errors around a partition write are transient. The orchestrator retries them `max_retries` times with linear backoff.
- A backfill names an event-date window and rewrites only those partitions.
- A run is reproducible when the same seed, generator version, and config snapshot are used. That claim will be tested, not assumed.

## Telemetry contract

Synthetic columns in `TELEMETRY_SCHEMA`:

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | string | UUID5 of `seed:sequence`. Stable for the same request. |
| `student_id` | string | `STU-#####` |
| `device_id` | string | `DEV-#####`, one device per synthetic student |
| `building_id`, `zone_id` | string | Fictional campus zones |
| `event_type` | string | `enter`, `ping`, or `exit` |
| `timestamp_utc` | datetime, UTC, microseconds | Session clock, not the wall clock of the generator |
| `x_m`, `y_m` | float64 | Meters inside that zone's rectangle |
| `accuracy_m` | float64 | Synthetic horizontal accuracy from 0.5 to 8.0 |

`duplicate_copies` copies early rows onto the tail and keeps `record_count` unchanged. The default is `0`.

## Metadata contract

`pipeline_runs` holds one row per attempt: status (`started`, `succeeded`, `failed`), seed, sorted JSON config snapshot, raw path, SHA-256, record counts, attempt, optional backfill bounds, and error text. Schema version is stored in `schema_meta`. A version other than `1` refuses to open.

## Known limitations after milestone 2

- Records are not validated, quarantined, or written to the partitioned store.
- `main.py` prepares directories and returns. It does not launch a batch.
- Retry settings are still configuration only. `fail_run` records an error; it does not schedule another attempt.
- Storing `backfill_start` and `backfill_end` does not recompute partitions.
- Reproducibility is defined for the same CPython `random.Random` stream and Polars 2.1. It has not been compared across operating systems or Python versions.
- No timing, memory, or file-size benchmark has been run. A 10,000-row default is a batch size, not a performance result.
- The registry is a single-writer SQLite file. WAL lets readers run alongside that writer; it is not a multi-writer queue.
