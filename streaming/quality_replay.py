"""Placeholder for a bounded deterministic fault injector and replay producer.

Future CLI: load config/quality.example.json explicitly, freeze/check input,
build replay payloads plus a separate evaluator manifest, then optionally publish.
No source polling, topic creation or publication happens at import time.
"""

from collections.abc import Mapping
from pathlib import Path


def build_replay(
    snapshot_path: Path,
    output_directory: Path,
    *,
    config: Mapping[str, object],
) -> dict[str, object]:
    """TODO: write deterministic wrappers and evaluator-only manifest separately.

    Record checksums, versions, seed, stable selection, mutations, intentional
    duplicates/omissions and release order. Keep original input immutable.
    Reserve group scenarios before row mutations. Do not leak original values,
    expected outcomes or fault labels into processor-facing wrappers.
    """
    raise NotImplementedError("Deterministic dirty-data generation is not implemented")


def publish_replay(replay_path: Path, *, bootstrap_servers: str) -> None:
    """TODO: publish prebuilt wrappers with KafkaManager and record broker ACKs.

    Use only quality_replay_raw. Preserve delivery IDs/bytes on retry, record
    actual transport attempts separately and honor the stored release schedule.
    Provision the experiment topic separately from this producer.
    """
    raise NotImplementedError("Experiment Kafka publication is not implemented")


def main() -> int:
    """TODO: add explicit build/publish commands after their implementations."""
    raise NotImplementedError("The quality replay CLI is a placeholder")


if __name__ == "__main__":
    raise SystemExit(main())
