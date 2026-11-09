"""Placeholder for quality collection validators/indexes, not a new DB client.

Keep database.MongoDB as the repository's sole persistence boundary. Later add
decision writes and bounded quality reads there, and integrate these definitions
into database.schema initialization/migrations after the contracts are settled.
"""


def quality_schema_definitions() -> dict[str, object]:
    """TODO: define quality_decisions and quality_summaries validators/indexes.

    Decision uniqueness uses run/delivery/rule for experiments and Kafka
    topic/partition/offset/rule for live input. Summary keys include dataset/run,
    scope/time window and rule version. Retain stream_rejections for raw failures.
    Define deterministic upserts/summary recomputation before enabling writes.
    """
    raise NotImplementedError("Quality collection schemas are not implemented")
