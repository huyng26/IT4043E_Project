"""Placeholder for profiling frozen source input before injecting faults."""

from pathlib import Path


def profile_snapshot(snapshot_path: Path, *, catalog_path: Path) -> dict[str, object]:
    """TODO: report natural defects, units, periods, duplicates and known coverage.

    Record snapshot/catalog checksums. Separate malformed deliveries from quality
    flags and unknown cadence. Read original input without rewriting it or making
    API requests. Return a serializable report for the evaluator/dashboard.
    """
    raise NotImplementedError("Frozen-source quality profiling is not implemented")
