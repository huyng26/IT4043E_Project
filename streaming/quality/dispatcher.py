"""Placeholder for an experiment-only executor partition adapter.

Do not add the replay topic to streaming.processing.TOPICS. Its current consumer
expects unwrapped live events and writes to the live serving database.
"""

from collections.abc import Iterable, Mapping


def process_replay_partition(
    records: Iterable[Mapping[str, object]],
    *,
    mongo_uri: str,
    experiment_database: str,
    run_id: str,
    rule_version: str,
) -> None:
    """TODO: validate/decode wrappers and project through shared quality rules.

    Require explicit experiment storage; reject live/collector database targets
    and mixed run IDs. Preserve outer and inner bytes when decoding fails. Map
    original_topic to source validation and retain transport/delivery lineage.
    Use database.MongoDB rather than introduce another persistence boundary.
    Persist retry-safe decisions before summaries; propagate failed writes so
    Spark retries. This worker must never read the evaluation manifest.
    """
    raise NotImplementedError("Isolated experiment dispatch is not implemented")
