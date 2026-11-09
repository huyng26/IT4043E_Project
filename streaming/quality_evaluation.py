"""Placeholder for evaluator-only baseline and corrupted-run comparisons.

This module may read expected answers. Never import it into quality rules or
executor processing, or pass its manifest into a worker.
"""

from pathlib import Path


def evaluate_run(
    *,
    manifest_path: Path,
    baseline_results_path: Path,
    experiment_results_path: Path,
    output_directory: Path,
) -> dict[str, object]:
    """TODO: save reproducible quality metrics and before/after examples.

    Verify matching snapshot/rule versions and completed input cutoffs. Measure
    terminal delivery accounting, repair correctness, detection precision/recall,
    valid-control false positives, lost coverage and final logical invariance.
    Separate natural defects, destructive faults, group scenarios and transport
    attempts. Do not use unrecoverable baseline values as repaired measurements.
    """
    raise NotImplementedError("Dirty-data experiment evaluation is not implemented")
