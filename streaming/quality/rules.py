"""Placeholder for pure quality rules shared by local, speed and batch paths.

Extend streaming.measurements deliberately; do not create a second incompatible
observation identity, revision ordering or hourly aggregation policy.
"""


def evaluate_payload(
    payload: bytes, *, source_topic: str, rule_version: str
) -> dict[str, object]:
    """TODO: parse, repair representation and classify one received payload.

    Retain bytes/received hashes and record derived normalization/repair actions.
    Reuse existing unit, source-flag and identity helpers after defining repair
    semantics. Never consult a manifest or source snapshot for expected values.
    Preserve identifiable invalid revisions for selection before usability
    filtering. Cross-record revision selection/aggregation belongs to the caller.
    Return quarantine decisions for malformed input; let infrastructure errors
    propagate. Suspected statistical anomalies remain warnings.
    """
    raise NotImplementedError("Shared versioned quality evaluation is not implemented")
