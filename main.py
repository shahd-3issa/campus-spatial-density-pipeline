"""
main.py
=======
Entry point for the Campus Spatial Density & Attendance Pipeline.

Usage:
    python main.py              # run with the default seed from config/settings.py
    python main.py --seed 123   # run with a specific seed (same seed -> same data)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from config import settings
from src.pipeline_orchestrator import STATUS_SUCCESS, PipelineOrchestrator, PipelineSummary

LOG_FILE = Path("logs") / "pipeline.log"


def setup_logging() -> None:
    """Send logs to the console AND to logs/pipeline.log."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE)],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Campus Spatial Density pipeline.")
    parser.add_argument(
        "--seed",
        type=int,
        default=int(getattr(settings, "RANDOM_SEED", 42)),
        help="Random seed for reproducible synthetic data (default: from settings, else 42).",
    )
    return parser.parse_args()


def print_report(summary: PipelineSummary) -> None:
    """Print a human-friendly summary of the run."""
    total = summary.records_raw or 1  # avoid dividing by zero
    line = "=" * 58
    icon = "✅" if summary.status == STATUS_SUCCESS else "❌"

    print(f"\n{line}")
    print("  CAMPUS SPATIAL DENSITY & ATTENDANCE PIPELINE - REPORT")
    print(line)
    print(f"  Status            : {icon} {summary.status}")
    print(f"  Run ID            : {summary.run_id}")
    print(f"  Seed              : {summary.seed}")
    print("-" * 58)
    print(f"  Raw records       : {summary.records_raw:,}")
    print(f"  Clean records     : {summary.records_clean:,} ({summary.records_clean / total:.1%})")
    print(f"  Quarantined       : {summary.records_quarantined:,} "
          f"({summary.records_quarantined / total:.1%})")
    print("-" * 58)
    print(f"  Partitioned by    : {', '.join(summary.partition_columns) or 'n/a'}")
    print(f"  Partition files   : {summary.partitions_written}")
    print(f"  Store location    : {summary.store_dir}")
    print("-" * 58)
    print("  Timings (seconds)")
    for stage, seconds in summary.timings.items():
        print(f"    {stage:<15}: {seconds:8.3f}")
    print(f"    {'TOTAL':<15}: {summary.total_seconds:8.3f}")
    if summary.error_message:
        print("-" * 58)
        print(f"  Error             : {summary.error_message}")
    print(f"{line}\n")


def main() -> int:
    """Run the pipeline and return a process exit code (0 = success, 1 = failure)."""
    setup_logging()
    args = parse_args()

    summary = PipelineOrchestrator(seed=args.seed).run()
    print_report(summary)

    return 0 if summary.status == STATUS_SUCCESS else 1


if __name__ == "__main__":
    sys.exit(main())
