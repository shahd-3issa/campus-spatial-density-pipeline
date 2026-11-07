"""Deterministic synthetic campus telemetry.

Events describe movement through a fictional meter grid. Identifiers are
synthetic (``STU-`` / ``DEV-``). This module never reads real student locations.

The same ``GenerationRequest`` produces the same rows on the same CPython
version because it uses ``random.Random`` with an explicit seed. Raw Parquet
files are claimed with an exclusive create and are not replaced after a
successful write.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

import polars as pl

from config.settings import Settings

logger = logging.getLogger(__name__)

_EVENT_NAMESPACE = uuid.UUID("8f3a2c1e-7b14-4d6a-9c21-0a5e6b7c8d9f")
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COMPRESSIONS = ("zstd", "snappy", "gzip", "uncompressed", "lz4", "brotli")
_ANCHOR = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)

TELEMETRY_SCHEMA = pl.Schema(
    {
        "event_id": pl.String,
        "student_id": pl.String,
        "device_id": pl.String,
        "building_id": pl.String,
        "zone_id": pl.String,
        "event_type": pl.String,
        "timestamp_utc": pl.Datetime(time_unit="us", time_zone="UTC"),
        "x_m": pl.Float64,
        "y_m": pl.Float64,
        "accuracy_m": pl.Float64,
    }
)


@dataclass(frozen=True, slots=True)
class Zone:
    """Axis-aligned zone on a fictional campus, in meters."""

    building_id: str
    zone_id: str
    x_min: float
    y_min: float
    x_max: float
    y_max: float


# A made-up north quad. Coordinates are local meters, not latitude/longitude.
ZONES: tuple[Zone, ...] = (
    Zone("BLD-LIB", "ENTRANCE", 10.0, 10.0, 30.0, 24.0),
    Zone("BLD-LIB", "STACKS", 30.0, 10.0, 90.0, 70.0),
    Zone("BLD-SCI", "LAB", 120.0, 20.0, 180.0, 80.0),
    Zone("BLD-SCI", "LECTURE", 180.0, 20.0, 230.0, 70.0),
    Zone("BLD-UNI", "FOOD", 40.0, 100.0, 110.0, 150.0),
    Zone("BLD-UNI", "HALL", 110.0, 100.0, 170.0, 160.0),
    Zone("BLD-RES", "LOBBY", 200.0, 110.0, 240.0, 140.0),
    Zone("BLD-RES", "FLOOR-2", 240.0, 110.0, 300.0, 170.0),
)
_ZONE_INDEX = {(zone.building_id, zone.zone_id): zone for zone in ZONES}


@dataclass(frozen=True, slots=True)
class GenerationRequest:
    """Inputs that fully define one synthetic batch."""

    record_count: int
    seed: int = 42
    ping_interval_seconds: int = 60
    start_time: datetime = _ANCHOR
    student_count: int = 50
    duplicate_copies: int = 0
    dwell_seconds: tuple[int, int] = (300, 2400)
    break_seconds: tuple[int, int] = (120, 900)
    jitter_seconds: int = 5
    arrival_spread_seconds: int = 1800

    def __post_init__(self) -> None:
        if self.record_count < 1:
            raise ValueError("record_count must be >= 1")
        if self.student_count < 1:
            raise ValueError("student_count must be >= 1")
        if self.ping_interval_seconds < 1:
            raise ValueError("ping_interval_seconds must be >= 1")
        if self.duplicate_copies < 0:
            raise ValueError("duplicate_copies must be >= 0")
        if self.duplicate_copies >= self.record_count:
            raise ValueError("duplicate_copies must be smaller than record_count")
        if self.jitter_seconds < 0:
            raise ValueError("jitter_seconds must be >= 0")
        if self.arrival_spread_seconds < 0:
            raise ValueError("arrival_spread_seconds must be >= 0")
        if self.start_time.tzinfo is None or self.start_time.utcoffset() is None:
            raise ValueError("start_time must be timezone-aware")
        _require_range("dwell_seconds", self.dwell_seconds)
        _require_range("break_seconds", self.break_seconds)

    @classmethod
    def from_settings(cls, settings: Settings) -> GenerationRequest:
        """Build a request from the operational defaults in ``Settings``."""
        return cls(
            record_count=settings.default_record_count,
            seed=settings.random_seed,
            ping_interval_seconds=settings.ping_interval_seconds,
        )


@dataclass(frozen=True, slots=True)
class RawWrite:
    """Result of an exclusive raw-file write."""

    path: Path
    records: int
    sha256: str


def zone_for(building_id: str, zone_id: str) -> Zone:
    """Return the fictional zone rectangle for a telemetry row."""
    try:
        return _ZONE_INDEX[(building_id, zone_id)]
    except KeyError as exc:
        raise KeyError(f"unknown zone {building_id}/{zone_id}") from exc


def generate_telemetry(request: GenerationRequest) -> pl.DataFrame:
    """Generate ``request.record_count`` synthetic telemetry rows.

    Each student walks one or more sessions: ``enter``, interval ``ping``
    events, then ``exit``. A later session begins after a break, which can be
    longer than the ping interval. ``duplicate_copies`` copies the earliest
    rows onto the tail so a downstream duplicate check has a known signal.
    """
    logger.info(
        "Generating telemetry records=%s seed=%s students=%s",
        request.record_count,
        request.seed,
        request.student_count,
    )
    rng = Random(request.seed)
    clocks = [
        request.start_time + timedelta(seconds=rng.randrange(request.arrival_spread_seconds + 1))
        for _ in range(request.student_count)
    ]
    rows: list[dict[str, object]] = []
    sequence = 0

    while len(rows) < request.record_count:
        student_index = rng.randrange(request.student_count)
        zone = ZONES[rng.randrange(len(ZONES))]
        dwell = rng.randint(request.dwell_seconds[0], request.dwell_seconds[1])
        session_start = clocks[student_index]
        session_end = session_start + timedelta(seconds=dwell)
        timestamp = session_start

        sequence = _append_event(
            rows,
            request=request,
            rng=rng,
            sequence=sequence,
            student_index=student_index,
            zone=zone,
            event_type="enter",
            timestamp=timestamp,
        )
        next_timestamp = timestamp + timedelta(seconds=_step_seconds(rng, request))
        while next_timestamp < session_end and len(rows) < request.record_count:
            sequence = _append_event(
                rows,
                request=request,
                rng=rng,
                sequence=sequence,
                student_index=student_index,
                zone=zone,
                event_type="ping",
                timestamp=next_timestamp,
            )
            next_timestamp += timedelta(seconds=_step_seconds(rng, request))
        if len(rows) < request.record_count:
            sequence = _append_event(
                rows,
                request=request,
                rng=rng,
                sequence=sequence,
                student_index=student_index,
                zone=zone,
                event_type="exit",
                timestamp=session_end,
            )
        clocks[student_index] = session_end + timedelta(
            seconds=rng.randint(request.break_seconds[0], request.break_seconds[1])
        )

    if request.duplicate_copies:
        for offset in range(request.duplicate_copies):
            rows[-1 - offset] = dict(rows[offset])
        logger.info("Applied duplicate_copies=%s", request.duplicate_copies)

    frame = pl.DataFrame(rows, schema=_FRAME_SCHEMA)
    frame = frame.sort(["timestamp_utc", "sequence"]).drop("sequence")
    if frame.schema != TELEMETRY_SCHEMA:
        raise RuntimeError(f"generator produced unexpected schema: {frame.schema}")
    logger.info(
        "Generated telemetry records=%s distinct_students=%s",
        frame.height,
        frame.get_column("student_id").n_unique(),
    )
    return frame


def write_raw_telemetry(
    frame: pl.DataFrame,
    raw_dir: Path,
    run_id: str,
    *,
    compression: str = "zstd",
) -> RawWrite:
    """Write ``frame`` to ``raw_dir/telemetry_<run_id>.parquet``.

    The destination is created with ``O_EXCL``. If that file already exists,
    ``FileExistsError`` is raised and the existing bytes are left in place.
    A failed write deletes the partial file so a later attempt can claim it.
    """
    if _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError(f"run_id must be a file-safe token, got {run_id!r}")
    if compression not in _COMPRESSIONS:
        allowed = ", ".join(_COMPRESSIONS)
        raise ValueError(f"compression must be one of {allowed}, got {compression!r}")
    if frame.schema != TELEMETRY_SCHEMA:
        raise ValueError(f"frame schema does not match telemetry schema: {frame.schema}")

    raw_dir.mkdir(parents=True, exist_ok=True)
    destination = raw_dir / f"telemetry_{run_id}.parquet"
    try:
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        logger.error("Refusing to overwrite raw telemetry at %s", destination)
        raise
    os.close(descriptor)

    logger.info(
        "Writing raw telemetry path=%s rows=%s compression=%s",
        destination,
        frame.height,
        compression,
    )
    try:
        frame.write_parquet(destination, compression=compression)
    except Exception:
        logger.exception("Raw write failed for %s", destination)
        destination.unlink(missing_ok=True)
        raise

    digest = sha256_file(destination)
    logger.info("Raw telemetry sealed path=%s sha256=%s", destination, digest)
    return RawWrite(path=destination, records=frame.height, sha256=digest)


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of ``path``."""
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65_536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


_FRAME_SCHEMA = pl.Schema({**dict(TELEMETRY_SCHEMA.items()), "sequence": pl.Int64})


def _require_range(name: str, bounds: tuple[int, int]) -> None:
    low, high = bounds
    if low < 1 or low > high:
        raise ValueError(f"{name} must be an inclusive range of integers >= 1")


def _step_seconds(rng: Random, request: GenerationRequest) -> int:
    if request.jitter_seconds == 0:
        return request.ping_interval_seconds
    jitter = rng.randint(-request.jitter_seconds, request.jitter_seconds)
    return max(1, request.ping_interval_seconds + jitter)


def _append_event(
    rows: list[dict[str, object]],
    *,
    request: GenerationRequest,
    rng: Random,
    sequence: int,
    student_index: int,
    zone: Zone,
    event_type: str,
    timestamp: datetime,
) -> int:
    width = zone.x_max - zone.x_min
    height = zone.y_max - zone.y_min
    rows.append(
        {
            "event_id": str(uuid.uuid5(_EVENT_NAMESPACE, f"{request.seed}:{sequence}")),
            "student_id": f"STU-{student_index:05d}",
            "device_id": f"DEV-{student_index:05d}",
            "building_id": zone.building_id,
            "zone_id": zone.zone_id,
            "event_type": event_type,
            "timestamp_utc": timestamp.astimezone(timezone.utc),
            "x_m": zone.x_min + rng.random() * width,
            "y_m": zone.y_min + rng.random() * height,
            "accuracy_m": 0.5 + rng.random() * 7.5,
            "sequence": sequence,
        }
    )
    return sequence + 1
