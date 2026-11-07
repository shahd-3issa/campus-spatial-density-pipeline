"""
src/data_validator.py
=====================
Milestone 3: Validation and Quarantine

What this module does (in plain English):
    1. Loads the raw telemetry Parquet files created by telemetry_generator.py
    2. Checks every row against our rules (no missing values, correct ID
       formats, coordinates inside the campus zone, valid timestamps)
    3. Finds duplicate records
    4. Looks for suspicious gaps in the timestamps of each device
    5. Saves every bad row into data/quarantine/ with a "rejection_reason"
    6. Returns the clean rows so the next pipeline step can use them

IMPORTANT: Check the CONFIG section below and make sure the column names
and coordinate limits match what your telemetry_generator.py produces!
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# CONFIG: change these values to match your project
# ---------------------------------------------------------------------------

# Where the raw data lives and where bad data should go
RAW_DATA_PATH = Path("data/raw")          # a folder of .parquet files OR one file
QUARANTINE_DIR = Path("data/quarantine")

# Column names (must match the Parquet files from telemetry_generator.py)
COL_STUDENT = "student_id"
COL_DEVICE = "device_id"
COL_X = "x_m"                # x coordinate in meters
COL_Y = "y_m"                # y coordinate in meters
COL_TIME = "timestamp"

REQUIRED_COLUMNS: list[str] = [COL_STUDENT, COL_DEVICE, COL_X, COL_Y, COL_TIME]

# ID formats: STU-##### and DEV-##### (exactly 5 digits)
STUDENT_ID_PATTERN = r"^STU-\d{5}$"
DEVICE_ID_PATTERN = r"^DEV-\d{5}$"

# The campus zone boundaries in meters (coordinates outside are invalid)
X_MIN, X_MAX = 0.0, 1000.0
Y_MIN, Y_MAX = 0.0, 1000.0

# If one device goes silent for longer than this, we report a "gap"
MAX_GAP_SECONDS = 300  # 5 minutes

# Columns that together identify "the same reading". If two rows share all of
# these, the second one is a duplicate.
DUPLICATE_KEY: list[str] = [COL_STUDENT, COL_DEVICE, COL_TIME]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Everything the validator hands back, bundled in one object."""

    clean_df: pl.DataFrame        # good rows -> go to the next pipeline step
    quarantine_df: pl.DataFrame   # bad rows + a "rejection_reason" column
    gap_report: pl.DataFrame      # timestamp gaps found (informational)
    quarantine_path: Path | None  # where the quarantine file was saved
    total_rows: int               # how many rows we started with
    clean_rows: int               # how many survived
    quarantined_rows: int         # how many were rejected


# ---------------------------------------------------------------------------
# Step 1: Load the data
# ---------------------------------------------------------------------------

def load_raw_telemetry(path: Path = RAW_DATA_PATH) -> pl.DataFrame:
    """Read raw telemetry from one Parquet file or a folder of Parquet files."""
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.glob("*.parquet"))
    else:
        raise FileNotFoundError(f"Raw data path does not exist: {path}")

    if not files:
        raise FileNotFoundError(f"No .parquet files found in: {path}")

    logger.info("Loading %d Parquet file(s) from %s", len(files), path)

    # Read each file and stack them on top of each other into one big table
    frames = [pl.read_parquet(f) for f in files]
    return pl.concat(frames, how="vertical_relaxed")


# ---------------------------------------------------------------------------
# Step 2: Schema validation
# ---------------------------------------------------------------------------

def check_required_columns(df: pl.DataFrame) -> None:
    """Stop immediately if a required column is missing.

    If a whole column is missing we cannot validate row by row, so this is
    a "hard fail" (an exception) rather than a quarantine.
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Raw data is missing required column(s): {missing}")


def ensure_timestamp_type(df: pl.DataFrame) -> pl.DataFrame:
    """Make sure the timestamp column is a real datetime.

    If timestamps were saved as text, we convert them. Anything that cannot be
    converted becomes null, and the validation rules below will then reject it.
    """
    if df.schema[COL_TIME] != pl.Datetime:
        df = df.with_columns(
            pl.col(COL_TIME).cast(pl.Datetime, strict=False)
        )
    return df


def build_rejection_reasons() -> pl.Expr:
    """Create ONE column that lists every rule a row broke.

    How it works: each rule gives back a short text if the row breaks it,
    otherwise null. We put all those results in a list, remove the nulls,
    and join what is left with "; ". An empty text "" means the row is clean.
    """

    def rule(broken: pl.Expr, message: str) -> pl.Expr:
        # fill_null(False) -> if we can't evaluate the rule (value is null),
        # don't flag it here; the "missing value" rules handle nulls.
        return pl.when(broken.fill_null(False)).then(pl.lit(message)).otherwise(None)

    reasons = [
        # --- missing values ---
        rule(pl.col(COL_STUDENT).is_null(), "missing student_id"),
        rule(pl.col(COL_DEVICE).is_null(), "missing device_id"),
        rule(pl.col(COL_X).is_null(), "missing x coordinate"),
        rule(pl.col(COL_Y).is_null(), "missing y coordinate"),
        rule(pl.col(COL_TIME).is_null(), "missing or unparseable timestamp"),
        # --- bad ID formats (only checked when the ID exists) ---
        rule(
            ~pl.col(COL_STUDENT).cast(pl.Utf8).str.contains(STUDENT_ID_PATTERN),
            "invalid student_id format",
        ),
        rule(
            ~pl.col(COL_DEVICE).cast(pl.Utf8).str.contains(DEVICE_ID_PATTERN),
            "invalid device_id format",
        ),
        # --- coordinates outside the campus zone ---
        rule(
            (pl.col(COL_X) < X_MIN) | (pl.col(COL_X) > X_MAX),
            f"x coordinate out of range [{X_MIN}, {X_MAX}]",
        ),
        rule(
            (pl.col(COL_Y) < Y_MIN) | (pl.col(COL_Y) > Y_MAX),
            f"y coordinate out of range [{Y_MIN}, {Y_MAX}]",
        ),
        # --- NaN / infinite numbers sneak past null checks, so catch them ---
        rule(
            pl.col(COL_X).is_nan() | pl.col(COL_X).is_infinite()
            | pl.col(COL_Y).is_nan() | pl.col(COL_Y).is_infinite(),
            "coordinate is NaN or infinite",
        ),
    ]

    return pl.concat_list(reasons).list.drop_nulls().list.join("; ")


def split_valid_and_invalid(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split the data into (valid_rows, invalid_rows).

    invalid_rows gets a new "rejection_reason" column explaining why.
    """
    df = df.with_columns(build_rejection_reasons().alias("rejection_reason"))

    invalid = df.filter(pl.col("rejection_reason") != "")
    valid = df.filter(pl.col("rejection_reason") == "").drop("rejection_reason")
    return valid, invalid


# ---------------------------------------------------------------------------
# Step 3: Duplicates and timestamp gaps
# ---------------------------------------------------------------------------

def split_duplicates(valid_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Separate unique rows from duplicate rows.

    We KEEP the first time we see a reading and quarantine every repeat
    of it. Example: if the same reading appears 3 times, 1 is kept and
    2 are quarantined.
    """
    # is_first_distinct() is True for the first occurrence of each key
    is_repeat = ~pl.struct(DUPLICATE_KEY).is_first_distinct()

    duplicates = valid_df.filter(is_repeat).with_columns(
        pl.lit("duplicate record (same student, device and timestamp)")
        .alias("rejection_reason")
    )
    unique = valid_df.filter(~is_repeat)
    return unique, duplicates


def find_timestamp_gaps(
    df: pl.DataFrame, max_gap_seconds: int = MAX_GAP_SECONDS
) -> pl.DataFrame:
    """Find places where a device stopped sending data for too long.

    Gaps are NOT bad rows (the rows around them are fine), so we don't
    quarantine anything here. We just return a report so you can see
    where data might be missing.
    """
    if df.is_empty():
        return pl.DataFrame(
            schema={
                COL_DEVICE: pl.Utf8,
                "gap_start": pl.Datetime,
                "gap_end": pl.Datetime,
                "gap_seconds": pl.Int64,
            }
        )

    return (
        df.sort([COL_DEVICE, COL_TIME])
        .with_columns(
            # The timestamp of the previous reading from the SAME device
            pl.col(COL_TIME).shift(1).over(COL_DEVICE).alias("gap_start")
        )
        .with_columns(
            (pl.col(COL_TIME) - pl.col("gap_start"))
            .dt.total_seconds()
            .alias("gap_seconds")
        )
        .filter(pl.col("gap_seconds") > max_gap_seconds)
        .select(
            pl.col(COL_DEVICE),
            pl.col("gap_start"),
            pl.col(COL_TIME).alias("gap_end"),
            pl.col("gap_seconds"),
        )
    )


# ---------------------------------------------------------------------------
# Step 4: Save quarantined rows
# ---------------------------------------------------------------------------

def save_quarantine(
    quarantine_df: pl.DataFrame, quarantine_dir: Path = QUARANTINE_DIR
) -> Path | None:
    """Write the rejected rows to a Parquet file inside data/quarantine/."""
    if quarantine_df.is_empty():
        logger.info("Nothing to quarantine - all rows were clean!")
        return None

    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # A timestamp in the file name means we never overwrite an older file
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = quarantine_dir / f"quarantine_{stamp}.parquet"

    quarantine_df.write_parquet(out_path)
    logger.info("Saved %d quarantined rows to %s", quarantine_df.height, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def validate_telemetry(
    raw_path: Path = RAW_DATA_PATH,
    quarantine_dir: Path = QUARANTINE_DIR,
) -> ValidationResult:
    """Run the full Milestone 3 validation pipeline.

    Order of operations:
        load -> check columns -> row rules -> duplicates -> gaps -> save
    """
    # 1. Load
    raw_df = load_raw_telemetry(raw_path)
    total_rows = raw_df.height

    # 2. Schema checks
    check_required_columns(raw_df)
    raw_df = ensure_timestamp_type(raw_df)

    # 3. Row-level rules (missing values, formats, out-of-range coordinates)
    valid_df, invalid_df = split_valid_and_invalid(raw_df)

    # 4. Duplicates (only among rows that passed the rules above, so the
    #    row we keep is always a valid one)
    clean_df, duplicates_df = split_duplicates(valid_df)

    # 5. Timestamp gaps (reported, not quarantined)
    gap_report = find_timestamp_gaps(clean_df)
    if not gap_report.is_empty():
        logger.warning(
            "Found %d timestamp gap(s) longer than %d seconds",
            gap_report.height,
            MAX_GAP_SECONDS,
        )

    # 6. Combine all rejected rows and save them
    quarantine_df = pl.concat([invalid_df, duplicates_df], how="vertical_relaxed")
    quarantine_path = save_quarantine(quarantine_df, quarantine_dir)

    result = ValidationResult(
        clean_df=clean_df,
        quarantine_df=quarantine_df,
        gap_report=gap_report,
        quarantine_path=quarantine_path,
        total_rows=total_rows,
        clean_rows=clean_df.height,
        quarantined_rows=quarantine_df.height,
    )

    logger.info(
        "Validation done: %d total | %d clean | %d quarantined",
        result.total_rows,
        result.clean_rows,
        result.quarantined_rows,
    )
    return result


if __name__ == "__main__":
    # Quick manual test: python -m src.data_validator
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    outcome = validate_telemetry()
    print(f"Total rows:       {outcome.total_rows}")
    print(f"Clean rows:       {outcome.clean_rows}")
    print(f"Quarantined rows: {outcome.quarantined_rows}")
    print(f"Timestamp gaps:   {outcome.gap_report.height}")
    if outcome.quarantined_rows:
        print(outcome.quarantine_df["rejection_reason"].value_counts())