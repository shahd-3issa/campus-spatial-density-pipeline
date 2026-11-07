"""
pipeline_orchestrator.py
========================
Milestone 4: ties the whole Campus Spatial Density & Attendance Pipeline together.

Flow:
    Generate raw telemetry  ->  Validate (clean + quarantine)
        ->  Store clean data as partitioned Parquet  ->  Log status & timings to SQLite

Design notes (good talking points for a portfolio / interview):
    * The orchestrator only *coordinates*; the real work lives in the earlier modules.
    * Every call into those modules goes through a small "adapter" function
      (see the ADAPTER LAYER section). If a function name or argument differs in
      your Milestone 1-3 code, you only need to edit those few adapters.
    * Transient problems (e.g. a file briefly locked) are retried with exponential
      backoff. Data problems are NOT retried - repeating them would never help.
"""

from __future__ import annotations

import functools
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, TypeVar

import polars as pl

from config import settings
from src import data_validator, db_metadata, telemetry_generator

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Run statuses written to the SQLite registry.
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"


# --------------------------------------------------------------------------- #
# Small helpers: custom error, retry decorator, stage timer
# --------------------------------------------------------------------------- #
class PipelineError(Exception):
    """Raised when a pipeline stage fails in a way we cannot recover from."""


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[BaseException], ...] = (OSError,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry a function when it raises one of `exceptions`.

    The wait time grows each attempt (1s, 2s, 4s, ...) - this is called
    "exponential backoff" and is a standard pattern for flaky I/O.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            wait = delay_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_attempts, exc,
                        )
                        raise
                    logger.warning(
                        "%s failed (attempt %d/%d): %s. Retrying in %.1fs...",
                        func.__name__, attempt, max_attempts, exc, wait,
                    )
                    time.sleep(wait)
                    wait *= backoff
            raise PipelineError("unreachable")  # keeps type checkers happy

        return wrapper

    return decorator


@contextmanager
def timed_stage(timings: Dict[str, float], stage: str) -> Iterator[None]:
    """Measure how long a block takes and store it in `timings[stage]` (seconds)."""
    start = time.perf_counter()
    try:
        yield
    finally:
        timings[stage] = time.perf_counter() - start
        logger.info("Stage '%s' finished in %.3fs", stage, timings[stage])


# --------------------------------------------------------------------------- #
# ADAPTER LAYER
# Thin wrappers around your Milestone 1-3 code. If your real function names or
# arguments are different, edit ONLY this section.
# --------------------------------------------------------------------------- #
@retry(max_attempts=3, delay_seconds=1.0)
def _generate_raw_data(seed: int) -> Path:
    """Milestone 1: generate telemetry and return the path of the raw Parquet output."""
    return Path(telemetry_generator.generate_telemetry(seed=seed))


def _validate_raw_data(raw_path: Path) -> Any:
    """Milestone 3: validate raw data. Returns a ValidationResult with `.clean_df`."""
    return data_validator.validate_telemetry(raw_path)


def _registry_start(seed: int) -> int:
    """Milestone 2: create the registry (if needed) and open a new run. Returns run_id."""
    db_metadata.init_db()
    return db_metadata.log_run_start(seed=seed)


def _registry_finish(
    run_id: int,
    status: str,
    records_raw: int,
    records_clean: int,
    records_quarantined: int,
    duration_seconds: float,
    error_message: Optional[str] = None,
) -> None:
    """Milestone 2: close the run with its final status, counts and benchmark time.

    Tip: if your `runs` table has no duration / error columns yet, add
    `duration_seconds REAL` and `error_message TEXT` to it.
    """
    db_metadata.log_run_end(
        run_id=run_id,
        status=status,
        records_raw=records_raw,
        records_clean=records_clean,
        records_quarantined=records_quarantined,
        duration_seconds=duration_seconds,
        error_message=error_message,
    )


# --------------------------------------------------------------------------- #
# Pipeline summary (what main.py prints at the end)
# --------------------------------------------------------------------------- #
@dataclass
class PipelineSummary:
    """Everything we want to report about one pipeline run."""

    seed: int
    status: str = STATUS_FAILED
    run_id: Optional[int] = None
    records_raw: int = 0
    records_clean: int = 0
    records_quarantined: int = 0
    partitions_written: int = 0
    partition_columns: Tuple[str, ...] = ()
    store_dir: Optional[Path] = None
    timings: Dict[str, float] = field(default_factory=dict)
    total_seconds: float = 0.0
    error_message: Optional[str] = None


# --------------------------------------------------------------------------- #
# Partitioned storage
# --------------------------------------------------------------------------- #
def _count_rows(parquet_path: Path) -> int:
    """Count rows in a Parquet file (or a folder of Parquet files) without loading it all."""
    source = parquet_path / "*.parquet" if parquet_path.is_dir() else parquet_path
    return int(pl.scan_parquet(source).select(pl.len()).collect().item())


def _add_partition_columns(df: pl.DataFrame) -> Tuple[pl.DataFrame, list[str]]:
    """Derive year / month / day from `timestamp` (and include `zone` if present)."""
    if df.schema["timestamp"] == pl.Utf8:  # be forgiving if timestamps are strings
        df = df.with_columns(pl.col("timestamp").str.to_datetime())

    df = df.with_columns(
        year=pl.col("timestamp").dt.year(),
        month=pl.col("timestamp").dt.month(),
        day=pl.col("timestamp").dt.day(),
    )

    partition_cols = ["year", "month", "day"]
    if "zone" in df.columns:
        partition_cols.append("zone")
    return df, partition_cols


@retry(max_attempts=3, delay_seconds=1.0)
def write_partitioned_parquet(clean_df: pl.DataFrame, store_dir: Path) -> Tuple[int, list[str]]:
    """Write `clean_df` to `store_dir` as Hive-style partitioned Parquet.

    Resulting layout:
        data/partitioned_store/year=2026/month=11/day=7/zone=Library/part-0.parquet

    Re-running with the same data overwrites the same files, so a run is idempotent.
    Read it back with:
        pl.scan_parquet("data/partitioned_store/**/*.parquet", hive_partitioning=True)

    Returns (number_of_partition_files_written, partition_columns_used).
    """
    if clean_df.is_empty():
        logger.warning("clean_df is empty - nothing to store.")
        return 0, []

    df, partition_cols = _add_partition_columns(clean_df)
    store_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0
    # partition_by splits the DataFrame into one smaller DataFrame per unique key combo.
    for keys, part in df.partition_by(partition_cols, as_dict=True, include_key=False).items():
        folder = store_dir / "/".join(f"{col}={val}" for col, val in zip(partition_cols, keys))
        folder.mkdir(parents=True, exist_ok=True)
        part.write_parquet(folder / "part-0.parquet", compression="zstd")
        files_written += 1

    logger.info(
        "Wrote %d partition file(s) to %s (partitioned by %s)",
        files_written, store_dir, partition_cols,
    )
    return files_written, partition_cols


# --------------------------------------------------------------------------- #
# The orchestrator
# --------------------------------------------------------------------------- #
class PipelineOrchestrator:
    """Runs the full pipeline once: Generate -> Validate -> Store -> Log."""

    def __init__(self, seed: Optional[int] = None, store_dir: Optional[Path] = None) -> None:
        # Fall back to values from config/settings.py when they exist.
        self.seed: int = seed if seed is not None else int(getattr(settings, "RANDOM_SEED", 42))
        self.store_dir: Path = Path(
            store_dir
            or getattr(settings, "PARTITIONED_STORE_DIR", Path("data") / "partitioned_store")
        )

    def run(self) -> PipelineSummary:
        """Execute the pipeline. Never raises - failures are captured in the summary."""
        summary = PipelineSummary(seed=self.seed, store_dir=self.store_dir)
        pipeline_start = time.perf_counter()
        logger.info("=== Pipeline run starting (seed=%d) ===", self.seed)

        try:
            summary.run_id = _registry_start(self.seed)

            # Step 1: generate raw telemetry
            with timed_stage(summary.timings, "generate"):
                raw_path = _generate_raw_data(self.seed)
                summary.records_raw = _count_rows(raw_path)

            # Step 2: validate (clean rows returned, bad rows quarantined by the validator)
            with timed_stage(summary.timings, "validate"):
                result = _validate_raw_data(raw_path)
                clean_df: pl.DataFrame = result.clean_df
                summary.records_clean = clean_df.height
                summary.records_quarantined = summary.records_raw - summary.records_clean

            # Step 3: store clean data as partitioned Parquet
            with timed_stage(summary.timings, "store"):
                summary.partitions_written, cols = write_partitioned_parquet(
                    clean_df, self.store_dir
                )
                summary.partition_columns = tuple(cols)

            summary.status = STATUS_SUCCESS

        except Exception as exc:  # noqa: BLE001 - we want to log ANY failure, then report it
            summary.status = STATUS_FAILED
            summary.error_message = f"{type(exc).__name__}: {exc}"
            logger.exception("Pipeline failed")

        finally:
            summary.total_seconds = time.perf_counter() - pipeline_start
            self._log_to_registry(summary)

        logger.info("=== Pipeline run finished: %s ===", summary.status)
        return summary

    @staticmethod
    def _log_to_registry(summary: PipelineSummary) -> None:
        """Record the outcome in SQLite. A logging problem must not hide the real result."""
        if summary.run_id is None:
            logger.warning("No run_id available - skipping registry update.")
            return
        try:
            _registry_finish(
                run_id=summary.run_id,
                status=summary.status,
                records_raw=summary.records_raw,
                records_clean=summary.records_clean,
                records_quarantined=summary.records_quarantined,
                duration_seconds=summary.total_seconds,
                error_message=summary.error_message,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Could not write final status to the metadata registry")
