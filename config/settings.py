"""Environment-backed configuration for the campus telemetry pipeline.

Operational settings are read from process environment variables. A local
``.env`` file is optional and never overrides variables that are already set.
This module does not create directories or open databases on import.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_PARQUET_CODECS = ("zstd", "snappy", "gzip", "uncompressed")


def project_root() -> Path:
    """Return the repository root (the parent of ``config/``)."""
    return Path(__file__).resolve().parents[1]


def _load_dotenv(path: Path) -> None:
    """Load ``KEY=VALUE`` lines into the environment without overriding it.

    Existing process environment variables win. Blank lines and ``#`` comments
    are ignored. Values wrapped in a matching single or double quote pair are
    unwrapped. This is intentionally small so the project does not depend on
    ``python-dotenv``.
    """
    if not path.is_file():
        return

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            raise ValueError(f"{path}:{line_number} is not a KEY=VALUE entry")

        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"{path}:{line_number} is missing a variable name")

        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _optional_env(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


def _env_path(name: str, default: Path, *, root: Path) -> Path:
    raw = _optional_env(name)
    if raw is None:
        return default
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = root / path
    return path


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = _optional_env(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = _optional_env(name)
    if raw is None:
        value = default
    else:
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_choice(name: str, default: str, allowed: tuple[str, ...], *, lower: bool) -> str:
    raw = _optional_env(name)
    value = default if raw is None else raw
    value = value.lower() if lower else value.upper()
    if value not in allowed:
        joined = ", ".join(allowed)
        observed = value if raw is None else raw
        raise ValueError(f"{name} must be one of {joined}, got {observed!r}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Resolved pipeline settings.

    Paths are absolute. Relative environment values are resolved against the
    repository root so a run does not depend on the process working directory.
    """

    project_root: Path
    raw_dir: Path
    quarantine_dir: Path
    partitioned_store_dir: Path
    metadata_db_path: Path
    log_level: str
    random_seed: int
    max_retries: int
    retry_backoff_seconds: float
    default_record_count: int
    ping_interval_seconds: int
    gap_threshold_seconds: int
    parquet_compression: str

    def snapshot(self) -> dict[str, str | int | float]:
        """Return a JSON-safe copy of the settings that affect a run.

        The snapshot is what the metadata registry stores. It contains paths
        and tuning values, never process environment secrets.
        """
        return {
            "project_root": str(self.project_root),
            "raw_dir": str(self.raw_dir),
            "quarantine_dir": str(self.quarantine_dir),
            "partitioned_store_dir": str(self.partitioned_store_dir),
            "metadata_db_path": str(self.metadata_db_path),
            "log_level": self.log_level,
            "random_seed": self.random_seed,
            "max_retries": self.max_retries,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "default_record_count": self.default_record_count,
            "ping_interval_seconds": self.ping_interval_seconds,
            "gap_threshold_seconds": self.gap_threshold_seconds,
            "parquet_compression": self.parquet_compression,
        }

    def ensure_data_directories(self) -> None:
        """Create pipeline data directories if they are missing.

        Raw files already present are left untouched. This method only creates
        directories; it does not write telemetry.
        """
        for path in (
            self.raw_dir,
            self.quarantine_dir,
            self.partitioned_store_dir,
            self.metadata_db_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def load_settings(*, env_file: Path | None = None) -> Settings:
    """Load settings from the environment and an optional ``.env`` file.

    Parameters
    ----------
    env_file:
        Dotenv file to read. When omitted, ``<project_root>/.env`` is used if
        that file exists. Variables already exported in the process environment
        are never replaced.
    """
    root = project_root()
    _load_dotenv(root / ".env" if env_file is None else env_file)

    data_dir = _env_path("CAMPUS_DATA_DIR", root / "data", root=root)
    log_level = _env_choice("CAMPUS_LOG_LEVEL", "INFO", _LOG_LEVELS, lower=False)
    compression = _env_choice("CAMPUS_PARQUET_COMPRESSION", "zstd", _PARQUET_CODECS, lower=True)

    return Settings(
        project_root=root,
        raw_dir=_env_path("CAMPUS_RAW_DIR", data_dir / "raw", root=root),
        quarantine_dir=_env_path("CAMPUS_QUARANTINE_DIR", data_dir / "quarantine", root=root),
        partitioned_store_dir=_env_path(
            "CAMPUS_PARTITIONED_STORE_DIR",
            data_dir / "partitioned_store",
            root=root,
        ),
        metadata_db_path=_env_path(
            "CAMPUS_METADATA_DB_PATH",
            data_dir / "pipeline_metadata.sqlite",
            root=root,
        ),
        log_level=log_level,
        random_seed=_env_int("CAMPUS_RANDOM_SEED", 42),
        max_retries=_env_int("CAMPUS_MAX_RETRIES", 3, minimum=0),
        retry_backoff_seconds=_env_float("CAMPUS_RETRY_BACKOFF_SECONDS", 0.5, minimum=0.0),
        default_record_count=_env_int("CAMPUS_DEFAULT_RECORD_COUNT", 10_000, minimum=1),
        ping_interval_seconds=_env_int("CAMPUS_PING_INTERVAL_SECONDS", 60, minimum=1),
        gap_threshold_seconds=_env_int("CAMPUS_GAP_THRESHOLD_SECONDS", 300, minimum=1),
        parquet_compression=compression,
    )


def configure_logging(settings: Settings) -> None:
    """Configure process logging for the application entrypoint.

    Call this from ``main`` only. Library modules must use
    ``logging.getLogger(__name__)`` and must not configure handlers themselves.
    """
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
