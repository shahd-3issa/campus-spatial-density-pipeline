"""SQLite registry for pipeline runs.

The registry stores run state, a configuration snapshot, and optional raw-file
evidence. It does not store telemetry rows. One process should write the
database. Schema version 1 has no further migrations.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = "1"
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    seed INTEGER NOT NULL,
    config_snapshot TEXT NOT NULL,
    raw_path TEXT,
    raw_sha256 TEXT,
    records_written INTEGER,
    records_quarantined INTEGER,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    backfill_start TEXT,
    backfill_end TEXT,
    error_message TEXT
);
"""


class RunStatus(StrEnum):
    """Lifecycle of one pipeline attempt."""

    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class MetadataError(Exception):
    """Base error for the run registry."""


class MetadataSchemaError(MetadataError):
    """The database schema version is not supported."""


class RunAlreadyExists(MetadataError):
    """A run id was inserted twice."""


class RunNotFound(MetadataError):
    """No row exists for the requested run id."""


class RunStateError(MetadataError):
    """The run is not in the state the transition requires."""


@dataclass(frozen=True, slots=True)
class RunRecord:
    """One row from ``pipeline_runs``."""

    run_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    seed: int
    config_snapshot: dict[str, object]
    raw_path: str | None
    raw_sha256: str | None
    records_written: int | None
    records_quarantined: int | None
    attempt: int
    backfill_start: str | None
    backfill_end: str | None
    error_message: str | None


class MetadataStore:
    """Open, migrate, and update the SQLite run registry."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._connection: sqlite3.Connection | None = None

    def __enter__(self) -> MetadataStore:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        logger.info("Opened metadata database path=%s journal_mode=%s", self.db_path, journal_mode)
        self._connection = connection
        try:
            self._migrate()
        except Exception:
            self.close()
            raise
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: object) -> None:
        connection = self._connection
        try:
            if connection is not None and exc_type is not None:
                connection.rollback()
        finally:
            self.close()

    def close(self) -> None:
        """Close the database connection if it is open."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Closed metadata database path=%s", self.db_path)

    def start_run(
        self,
        run_id: str,
        *,
        seed: int,
        config_snapshot: Mapping[str, object],
        attempt: int = 1,
        backfill_start: str | None = None,
        backfill_end: str | None = None,
    ) -> RunRecord:
        """Insert a run in the ``started`` state.

        ``config_snapshot`` is stored as sorted JSON. Values must already be
        JSON-serializable. Backfill bounds are stored together or not at all;
        this method does not execute a backfill.
        """
        _require_run_id(run_id)
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        _require_backfill_pair(backfill_start, backfill_end)
        payload = json.dumps(dict(config_snapshot), sort_keys=True, separators=(",", ":"))
        started_at = _utc_now()
        connection = self._require_connection()
        try:
            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, status, started_at, seed, config_snapshot, attempt,
                    backfill_start, backfill_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    RunStatus.STARTED.value,
                    started_at,
                    seed,
                    payload,
                    attempt,
                    backfill_start,
                    backfill_end,
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            logger.error("Run id already exists: %s", run_id)
            raise RunAlreadyExists(run_id) from exc
        logger.info("Started run_id=%s seed=%s attempt=%s", run_id, seed, attempt)
        logger.debug("Run config snapshot keys=%s", sorted(dict(config_snapshot)))
        record = self.get_run(run_id)
        if record is None:
            raise RuntimeError(f"run {run_id} disappeared after insert")
        return record

    def complete_run(
        self,
        run_id: str,
        *,
        records_written: int,
        raw_path: Path | None = None,
        raw_sha256: str | None = None,
        records_quarantined: int = 0,
    ) -> RunRecord:
        """Mark a started run as succeeded and store output evidence."""
        if records_written < 0:
            raise ValueError("records_written must be >= 0")
        if records_quarantined < 0:
            raise ValueError("records_quarantined must be >= 0")
        finished_at = _utc_now()
        self._transition(
            run_id,
            """
            UPDATE pipeline_runs
            SET status = ?, finished_at = ?, records_written = ?,
                records_quarantined = ?, raw_path = ?, raw_sha256 = ?
            WHERE run_id = ? AND status = ?
            """,
            (
                RunStatus.SUCCEEDED.value,
                finished_at,
                records_written,
                records_quarantined,
                None if raw_path is None else str(raw_path),
                raw_sha256,
                run_id,
                RunStatus.STARTED.value,
            ),
        )
        logger.info(
            "Completed run_id=%s records_written=%s records_quarantined=%s",
            run_id,
            records_written,
            records_quarantined,
        )
        return self._require_run(run_id)

    def fail_run(self, run_id: str, error_message: str) -> RunRecord:
        """Mark a started run as failed."""
        if not error_message.strip():
            raise ValueError("error_message must not be empty")
        self._transition(
            run_id,
            """
            UPDATE pipeline_runs
            SET status = ?, finished_at = ?, error_message = ?
            WHERE run_id = ? AND status = ?
            """,
            (
                RunStatus.FAILED.value,
                _utc_now(),
                error_message,
                run_id,
                RunStatus.STARTED.value,
            ),
        )
        logger.error("Failed run_id=%s error=%s", run_id, error_message)
        return self._require_run(run_id)

    def get_run(self, run_id: str) -> RunRecord | None:
        """Return one run, or ``None`` when the id is unknown."""
        row = self._require_connection().execute(
            "SELECT * FROM pipeline_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return _record_from_row(row)

    def list_runs(self, status: RunStatus | None = None) -> list[RunRecord]:
        """Return runs ordered by start time, optionally filtered by status."""
        connection = self._require_connection()
        if status is None:
            rows = connection.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at, run_id"
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT * FROM pipeline_runs WHERE status = ? ORDER BY started_at, run_id",
                (status.value,),
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def _migrate(self) -> None:
        connection = self._require_connection()
        connection.executescript(_SCHEMA_SQL)
        row = connection.execute("SELECT value FROM schema_meta WHERE key = 'version'").fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('version', ?)",
                (_SCHEMA_VERSION,),
            )
            connection.commit()
            logger.info("Initialized metadata schema version=%s", _SCHEMA_VERSION)
            return
        if row["value"] != _SCHEMA_VERSION:
            raise MetadataSchemaError(
                f"unsupported metadata schema version {row['value']}; expected {_SCHEMA_VERSION}"
            )

    def _transition(self, run_id: str, sql: str, parameters: tuple[object, ...]) -> None:
        connection = self._require_connection()
        existing = self.get_run(run_id)
        if existing is None:
            raise RunNotFound(run_id)
        if existing.status is not RunStatus.STARTED:
            raise RunStateError(f"run {run_id} is {existing.status.value}, expected started")
        changes_before = connection.total_changes
        connection.execute(sql, parameters)
        if connection.total_changes - changes_before != 1:
            connection.rollback()
            raise RunStateError(f"run {run_id} was not updated")
        connection.commit()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise MetadataError("metadata store is closed")
        return self._connection

    def _require_run(self, run_id: str) -> RunRecord:
        record = self.get_run(run_id)
        if record is None:
            raise RunNotFound(run_id)
        return record


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _require_run_id(run_id: str) -> None:
    if not run_id.strip():
        raise ValueError("run_id must not be empty")


def _require_backfill_pair(start: str | None, end: str | None) -> None:
    if (start is None) != (end is None):
        raise ValueError("backfill_start and backfill_end must both be set or both be omitted")
    if start is not None and (not start.strip() or not end or not end.strip()):
        raise ValueError("backfill bounds must be non-empty")


def _record_from_row(row: sqlite3.Row) -> RunRecord:
    snapshot = json.loads(row["config_snapshot"])
    if not isinstance(snapshot, dict):
        raise MetadataError(f"config snapshot for {row['run_id']} is not an object")
    return RunRecord(
        run_id=row["run_id"],
        status=RunStatus(row["status"]),
        started_at=datetime.fromisoformat(row["started_at"]),
        finished_at=None if row["finished_at"] is None else datetime.fromisoformat(row["finished_at"]),
        seed=row["seed"],
        config_snapshot=snapshot,
        raw_path=row["raw_path"],
        raw_sha256=row["raw_sha256"],
        records_written=row["records_written"],
        records_quarantined=row["records_quarantined"],
        attempt=row["attempt"],
        backfill_start=row["backfill_start"],
        backfill_end=row["backfill_end"],
        error_message=row["error_message"],
    )
