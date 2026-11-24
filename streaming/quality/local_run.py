"""Local, manifest-free processing of a snapshot or a replay into saved decisions.

This is what a consumer sees: received bytes go through the shared rules and every
delivery gets exactly one terminal decision. Nothing here imports or reads an
evaluator manifest, so expected answers cannot leak into processing. The saved
results files are the only input the evaluator needs besides the manifest.

Results document::

    {"schema_version": 1, "kind": "baseline" | "experiment", "rule_version": ...,
     "input": {"sha256": ..., "deliveries": n}, "run_id": ... | null,
     "decisions": [{"key": <record number | delivery id>, "decision": {...}}]}
"""

import json
from pathlib import Path

from streaming.quality.contracts import (
    decode_wrapper_payload,
    sha256_hex,
    validate_decision,
    validate_wrapper,
)
from streaming.quality.rules import evaluate_payload

RESULTS_SCHEMA_VERSION = 1
SOURCE_TOPIC = "air_quality_raw"
RESULT_KINDS = ("baseline", "experiment")


def read_snapshot_lines(path: Path) -> tuple[str, list[tuple[int, bytes]]]:
    """Return the snapshot digest and its non-blank lines as ``(line_number, bytes)``.

    Line numbers are 1-based positions in the file and stay stable when blank lines
    are skipped. A trailing carriage return is not part of a delivery.
    """
    data = Path(path).read_bytes()
    lines = []
    for number, line in enumerate(data.split(b"\n"), 1):
        raw = line.rstrip(b"\r")
        if raw.strip():
            lines.append((number, raw))
    return sha256_hex(data), lines


def process_snapshot(snapshot_path: Path, *, rule_version: str) -> dict[str, object]:
    """Decide every snapshot line; the key is its record number."""
    digest, lines = read_snapshot_lines(snapshot_path)
    decisions = [
        {
            "key": number,
            "decision": evaluate_payload(raw, source_topic=SOURCE_TOPIC, rule_version=rule_version),
        }
        for number, raw in lines
    ]
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "kind": "baseline",
        "rule_version": rule_version,
        "input": {"sha256": digest, "deliveries": len(lines)},
        "run_id": None,
        "decisions": decisions,
    }


def process_replay(replay_path: Path, *, rule_version: str) -> dict[str, object]:
    """Decide every wrapper; the key is its delivery ID.

    A wrapper that fails its transport contract raises: that is a producer defect,
    not a data-quality outcome, and must not be hidden as a quarantine.
    """
    data = Path(replay_path).read_bytes()
    decisions, run_ids = [], set()
    for line_number, line in enumerate(data.split(b"\n"), 1):
        if not line.strip():
            continue
        try:
            wrapper = validate_wrapper(json.loads(line.decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError(f"replay line {line_number} is not a valid wrapper: {error}") from error
        run_ids.add(wrapper["run_id"])
        decisions.append(
            {
                "key": wrapper["delivery_id"],
                "decision": evaluate_payload(
                    decode_wrapper_payload(wrapper),
                    source_topic=str(wrapper["original_topic"]),
                    rule_version=rule_version,
                ),
            }
        )
    if len(run_ids) > 1:
        raise ValueError("replay mixes several run IDs")
    keys = [row["key"] for row in decisions]
    if len(set(keys)) != len(keys):
        raise ValueError("replay contains duplicate delivery IDs")
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "kind": "experiment",
        "rule_version": rule_version,
        "input": {"sha256": sha256_hex(data), "deliveries": len(decisions)},
        "run_id": next(iter(run_ids), None),
        "decisions": decisions,
    }


def save_results(results: dict[str, object], path: Path) -> None:
    """Write results as canonical JSON; an existing file is never overwritten."""
    text = json.dumps(results, indent=1, sort_keys=True, allow_nan=False) + "\n"
    with Path(path).open("x", encoding="utf-8") as handle:
        handle.write(text)


def load_results(path: Path, *, kind: str) -> dict[str, object]:
    """Read and validate a results document of the expected kind."""
    try:
        results = json.loads(Path(path).read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError(f"{path} is not valid JSON: {error}") from error
    if not isinstance(results, dict) or results.get("schema_version") != RESULTS_SCHEMA_VERSION:
        raise ValueError(f"{path} is not a schema-{RESULTS_SCHEMA_VERSION} results document")
    if results.get("kind") != kind:
        raise ValueError(f"{path} holds {results.get('kind')!r} results, expected {kind!r}")
    rows = results.get("decisions")
    if not isinstance(rows, list):
        raise ValueError(f"{path}: decisions must be a list")
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"key", "decision"}:
            raise ValueError(f"{path}: malformed decision row")
        if row["key"] in seen:
            raise ValueError(f"{path}: duplicate key {row['key']!r}")
        seen.add(row["key"])
        validate_decision(row["decision"])
    return results
