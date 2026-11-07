"""Tests for deterministic synthetic telemetry and immutable raw writes."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from src.telemetry_generator import (
    TELEMETRY_SCHEMA,
    GenerationRequest,
    generate_telemetry,
    sha256_file,
    write_raw_telemetry,
    zone_for,
)

_ANCHOR = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)


def _session_request(**overrides: object) -> GenerationRequest:
    values: dict[str, object] = {
        "record_count": 4,
        "seed": 1,
        "student_count": 1,
        "ping_interval_seconds": 60,
        "start_time": _ANCHOR,
        "dwell_seconds": (180, 180),
        "break_seconds": (600, 600),
        "jitter_seconds": 0,
        "arrival_spread_seconds": 0,
    }
    values.update(overrides)
    return GenerationRequest(**values)  # type: ignore[arg-type]


def test_session_shape_is_deterministic() -> None:
    frame = generate_telemetry(_session_request())

    assert frame.schema == TELEMETRY_SCHEMA
    assert frame.height == 4
    assert frame.get_column("event_type").to_list() == ["enter", "ping", "ping", "exit"]
    timestamps = frame.get_column("timestamp_utc").to_list()
    assert timestamps == [
        _ANCHOR,
        _ANCHOR + timedelta(seconds=60),
        _ANCHOR + timedelta(seconds=120),
        _ANCHOR + timedelta(seconds=180),
    ]
    assert frame.get_column("student_id").unique().to_list() == ["STU-00000"]
    assert frame.get_column("device_id").unique().to_list() == ["DEV-00000"]
    assert generate_telemetry(_session_request()).equals(frame)


def test_break_between_sessions_is_explicit() -> None:
    frame = generate_telemetry(_session_request(record_count=6))
    timestamps = frame.get_column("timestamp_utc").to_list()

    assert timestamps[3] == _ANCHOR + timedelta(seconds=180)
    assert timestamps[4] == _ANCHOR + timedelta(seconds=780)
    assert timestamps[4] - timestamps[3] == timedelta(seconds=600)
    assert frame.get_column("event_type").to_list()[:5] == ["enter", "ping", "ping", "exit", "enter"]


def test_coordinates_stay_inside_the_fictional_zone() -> None:
    frame = generate_telemetry(GenerationRequest(record_count=200, seed=42, student_count=8))

    for building_id, zone_id, x_m, y_m in frame.select(
        "building_id", "zone_id", "x_m", "y_m"
    ).iter_rows():
        zone = zone_for(building_id, zone_id)
        assert zone.x_min <= x_m <= zone.x_max
        assert zone.y_min <= y_m <= zone.y_max
    assert frame.get_column("event_type").is_in(["enter", "ping", "exit"]).all()
    assert frame.null_count().sum_horizontal()[0] == 0


def test_same_seed_matches_and_a_different_seed_does_not() -> None:
    left = generate_telemetry(GenerationRequest(record_count=300, seed=7, student_count=12))
    right = generate_telemetry(GenerationRequest(record_count=300, seed=7, student_count=12))
    other = generate_telemetry(GenerationRequest(record_count=300, seed=8, student_count=12))

    assert left.equals(right)
    assert not left.equals(other)


def test_duplicate_copies_keep_the_row_count(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    frame = generate_telemetry(_session_request(record_count=5, duplicate_copies=2))
    counts = frame.get_column("event_id").value_counts()
    duplicated = counts.filter(pl.col("count") == 2)

    assert frame.height == 5
    assert duplicated.height == 2
    assert "Applied duplicate_copies=2" in caplog.text


def test_invalid_requests_are_rejected() -> None:
    with pytest.raises(ValueError, match="record_count"):
        GenerationRequest(record_count=0)
    with pytest.raises(ValueError, match="start_time"):
        GenerationRequest(record_count=1, start_time=datetime(2026, 3, 2, 8, 0))
    with pytest.raises(ValueError, match="duplicate_copies"):
        GenerationRequest(record_count=2, duplicate_copies=2)


def test_raw_write_is_immutable_and_round_trips(tmp_path) -> None:
    frame = generate_telemetry(_session_request())
    raw_dir = tmp_path / "raw"
    written = write_raw_telemetry(frame, raw_dir, "run-001", compression="zstd")

    assert written.records == 4
    assert written.sha256 == sha256_file(written.path)
    assert pl.read_parquet(written.path).equals(frame)
    sealed = written.path.read_bytes()

    with pytest.raises(FileExistsError):
        write_raw_telemetry(frame, raw_dir, "run-001")
    assert written.path.read_bytes() == sealed


def test_raw_write_rejects_path_traversal_and_bad_frames(tmp_path) -> None:
    frame = generate_telemetry(_session_request())
    with pytest.raises(ValueError, match="run_id"):
        write_raw_telemetry(frame, tmp_path, "../escape")
    with pytest.raises(ValueError, match="schema"):
        write_raw_telemetry(frame.drop("accuracy_m"), tmp_path, "run-002")
    assert not (tmp_path / "telemetry_run-002.parquet").exists()
