"""Tests for environment-backed pipeline configuration."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from config.settings import Settings, configure_logging, load_settings

_ENV_KEYS = (
    "CAMPUS_DATA_DIR",
    "CAMPUS_RAW_DIR",
    "CAMPUS_QUARANTINE_DIR",
    "CAMPUS_PARTITIONED_STORE_DIR",
    "CAMPUS_METADATA_DB_PATH",
    "CAMPUS_LOG_LEVEL",
    "CAMPUS_RANDOM_SEED",
    "CAMPUS_MAX_RETRIES",
    "CAMPUS_RETRY_BACKOFF_SECONDS",
    "CAMPUS_DEFAULT_RECORD_COUNT",
    "CAMPUS_PING_INTERVAL_SECONDS",
    "CAMPUS_GAP_THRESHOLD_SECONDS",
    "CAMPUS_PARQUET_COMPRESSION",
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove pipeline variables so defaults are observable."""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_defaults_are_stable_and_do_not_create_directories(
    clean_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "pipeline-data"
    monkeypatch.setenv("CAMPUS_DATA_DIR", str(data_dir))

    settings = load_settings(env_file=tmp_path / "missing.env")

    assert settings.random_seed == 42
    assert settings.log_level == "INFO"
    assert settings.max_retries == 3
    assert settings.retry_backoff_seconds == 0.5
    assert settings.default_record_count == 10_000
    assert settings.ping_interval_seconds == 60
    assert settings.gap_threshold_seconds == 300
    assert settings.parquet_compression == "zstd"
    assert settings.raw_dir == data_dir / "raw"
    assert settings.quarantine_dir == data_dir / "quarantine"
    assert settings.partitioned_store_dir == data_dir / "partitioned_store"
    assert settings.metadata_db_path == data_dir / "pipeline_metadata.sqlite"
    assert not data_dir.exists()
    assert not (tmp_path / "missing.env").exists()


def test_environment_overrides_and_relative_paths_use_project_root(
    clean_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("CAMPUS_DATA_DIR", "custom-data")
    monkeypatch.setenv("CAMPUS_RAW_DIR", str(tmp_path / "incoming"))
    monkeypatch.setenv("CAMPUS_LOG_LEVEL", "debug")
    monkeypatch.setenv("CAMPUS_RANDOM_SEED", "7")
    monkeypatch.setenv("CAMPUS_MAX_RETRIES", "0")
    monkeypatch.setenv("CAMPUS_RETRY_BACKOFF_SECONDS", "1.5")
    monkeypatch.setenv("CAMPUS_DEFAULT_RECORD_COUNT", "25")
    monkeypatch.setenv("CAMPUS_PING_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("CAMPUS_GAP_THRESHOLD_SECONDS", "90")
    monkeypatch.setenv("CAMPUS_PARQUET_COMPRESSION", "SNAPPY")

    settings = load_settings(env_file=tmp_path / "missing.env")

    assert settings.raw_dir == tmp_path / "incoming"
    assert settings.quarantine_dir == settings.project_root / "custom-data" / "quarantine"
    assert settings.log_level == "DEBUG"
    assert settings.random_seed == 7
    assert settings.max_retries == 0
    assert settings.retry_backoff_seconds == 1.5
    assert settings.default_record_count == 25
    assert settings.ping_interval_seconds == 30
    assert settings.gap_threshold_seconds == 90
    assert settings.parquet_compression == "snappy"


def test_dotenv_does_not_override_existing_environment(
    clean_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# local overrides",
                "export CAMPUS_RANDOM_SEED=99",
                "CAMPUS_LOG_LEVEL='WARNING'",
                "CAMPUS_MAX_RETRIES=4",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CAMPUS_RANDOM_SEED", "3")

    settings = load_settings(env_file=env_file)

    assert settings.random_seed == 3
    assert settings.log_level == "WARNING"
    assert settings.max_retries == 4


def test_invalid_values_are_rejected(clean_env: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CAMPUS_MAX_RETRIES", "-1")
    with pytest.raises(ValueError, match="CAMPUS_MAX_RETRIES"):
        load_settings(env_file=tmp_path / "missing.env")

    monkeypatch.setenv("CAMPUS_MAX_RETRIES", "1")
    monkeypatch.setenv("CAMPUS_LOG_LEVEL", "VERBOSE")
    with pytest.raises(ValueError, match="CAMPUS_LOG_LEVEL"):
        load_settings(env_file=tmp_path / "missing.env")

    monkeypatch.setenv("CAMPUS_LOG_LEVEL", "INFO")
    monkeypatch.setenv("CAMPUS_PARQUET_COMPRESSION", "brotli")
    with pytest.raises(ValueError, match="CAMPUS_PARQUET_COMPRESSION"):
        load_settings(env_file=tmp_path / "missing.env")


def test_malformed_dotenv_line_raises(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("NOT_AN_ASSIGNMENT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="KEY=VALUE"):
        load_settings(env_file=env_file)


def test_ensure_data_directories_creates_only_directories(tmp_path: Path) -> None:
    settings = Settings(
        project_root=tmp_path,
        raw_dir=tmp_path / "raw",
        quarantine_dir=tmp_path / "quarantine",
        partitioned_store_dir=tmp_path / "partitioned_store",
        metadata_db_path=tmp_path / "meta" / "pipeline.sqlite",
        log_level="INFO",
        random_seed=1,
        max_retries=1,
        retry_backoff_seconds=0.0,
        default_record_count=1,
        ping_interval_seconds=60,
        gap_threshold_seconds=300,
        parquet_compression="zstd",
    )
    marker = settings.raw_dir / "already_there.parquet"
    settings.raw_dir.mkdir()
    marker.write_bytes(b"keep")

    settings.ensure_data_directories()

    assert settings.quarantine_dir.is_dir()
    assert settings.partitioned_store_dir.is_dir()
    assert settings.metadata_db_path.parent.is_dir()
    assert marker.read_bytes() == b"keep"
    assert not settings.metadata_db_path.exists()


def test_configure_logging_sets_root_level(clean_env: None, tmp_path: Path) -> None:
    settings = load_settings(env_file=tmp_path / "missing.env")
    configure_logging(settings)
    assert logging.getLogger().level == logging.INFO
