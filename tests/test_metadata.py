"""Tests for the SQLite pipeline run registry."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

import pytest

from config.settings import Settings
from src.db_metadata import (
    MetadataSchemaError,
    MetadataStore,
    RunAlreadyExists,
    RunNotFound,
    RunStateError,
    RunStatus,
)
from src.telemetry_generator import GenerationRequest, generate_telemetry, write_raw_telemetry


def _settings(tmp_path) -> Settings:
    return Settings(
        project_root=tmp_path,
        raw_dir=tmp_path / "raw",
        quarantine_dir=tmp_path / "quarantine",
        partitioned_store_dir=tmp_path / "partitioned_store",
        metadata_db_path=tmp_path / "meta" / "pipeline.sqlite",
        log_level="INFO",
        random_seed=42,
        max_retries=3,
        retry_backoff_seconds=0.5,
        default_record_count=20,
        ping_interval_seconds=60,
        gap_threshold_seconds=300,
        parquet_compression="zstd",
    )


def test_run_lifecycle_and_persistence(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    database = tmp_path / "meta" / "pipeline.sqlite"
    with MetadataStore(database) as store:
        started = store.start_run(
            "run-001",
            seed=42,
            config_snapshot={"random_seed": 42, "default_record_count": 20},
            attempt=1,
            backfill_start="2026-03-02",
            backfill_end="2026-03-03",
        )
        assert started.status is RunStatus.STARTED
        assert started.backfill_start == "2026-03-02"
        completed = store.complete_run(
            "run-001",
            records_written=20,
            records_quarantined=1,
            raw_path=tmp_path / "raw" / "telemetry_run-001.parquet",
            raw_sha256="abc",
        )
        assert completed.status is RunStatus.SUCCEEDED
        assert completed.records_written == 20
        assert completed.finished_at is not None
        assert completed.finished_at.tzinfo is not None

    with MetadataStore(database) as store:
        loaded = store.get_run("run-001")
        assert loaded is not None
        assert loaded.config_snapshot == {"default_record_count": 20, "random_seed": 42}
        assert loaded.raw_sha256 == "abc"
        assert store.list_runs(RunStatus.SUCCEEDED)[0].run_id == "run-001"
        assert store.get_run("missing") is None

    assert "Started run_id=run-001" in caplog.text
    assert "Completed run_id=run-001" in caplog.text


def test_duplicate_start_and_illegal_transitions(tmp_path) -> None:
    with MetadataStore(tmp_path / "pipeline.sqlite") as store:
        store.start_run("run-001", seed=1, config_snapshot={"random_seed": 1})
        with pytest.raises(RunAlreadyExists):
            store.start_run("run-001", seed=1, config_snapshot={"random_seed": 1})
        store.fail_run("run-001", "disk full")
        with pytest.raises(RunStateError):
            store.complete_run("run-001", records_written=1)
        with pytest.raises(RunNotFound):
            store.fail_run("missing", "nope")
        failed = store.get_run("run-001")
        assert failed is not None
        assert failed.status is RunStatus.FAILED
        assert failed.error_message == "disk full"


def test_invalid_registry_inputs(tmp_path) -> None:
    with MetadataStore(tmp_path / "pipeline.sqlite") as store:
        with pytest.raises(ValueError, match="backfill"):
            store.start_run("run-001", seed=1, config_snapshot={}, backfill_start="2026-03-02")
        with pytest.raises(ValueError, match="attempt"):
            store.start_run("run-002", seed=1, config_snapshot={}, attempt=0)
        store.start_run("run-003", seed=1, config_snapshot={})
        with pytest.raises(ValueError, match="error_message"):
            store.fail_run("run-003", "   ")


def test_closed_store_and_unknown_schema_version(tmp_path) -> None:
    store = MetadataStore(tmp_path / "pipeline.sqlite")
    with pytest.raises(Exception, match="closed"):
        store.get_run("run-001")

    database = tmp_path / "future.sqlite"
    with MetadataStore(database):
        pass
    connection = sqlite3.connect(database)
    connection.execute("UPDATE schema_meta SET value = '99' WHERE key = 'version'")
    connection.commit()
    connection.close()
    with pytest.raises(MetadataSchemaError, match="99"):
        with MetadataStore(database):
            pass


def test_generate_write_and_register(tmp_path) -> None:
    settings = _settings(tmp_path)
    request = GenerationRequest.from_settings(settings)
    request = GenerationRequest(
        record_count=request.record_count,
        seed=request.seed,
        ping_interval_seconds=request.ping_interval_seconds,
        student_count=4,
        jitter_seconds=0,
        arrival_spread_seconds=0,
    )
    frame = generate_telemetry(request)
    written = write_raw_telemetry(
        frame,
        settings.raw_dir,
        "batch-20260302",
        compression=settings.parquet_compression,
    )

    with MetadataStore(settings.metadata_db_path) as store:
        store.start_run("batch-20260302", seed=settings.random_seed, config_snapshot=settings.snapshot())
        record = store.complete_run(
            "batch-20260302",
            records_written=written.records,
            raw_path=written.path,
            raw_sha256=written.sha256,
        )

    assert record.status is RunStatus.SUCCEEDED
    assert record.records_written == frame.height == 20
    assert record.config_snapshot["parquet_compression"] == "zstd"
    assert record.raw_sha256 == written.sha256
    assert datetime.now(timezone.utc) >= record.started_at
