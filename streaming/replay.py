"""Run a bounded, in-memory JSONL replay: python3 -m streaming.replay FILE."""

import argparse
import json
from pathlib import Path

from streaming.measurements import (
    current_measurements,
    hourly_metrics,
    parse_measurement,
)


def reject_constant(value: str) -> None:
    raise ValueError(f"nonstandard JSON number: {value}")


def replay(lines) -> dict:
    measurements = []
    rejected = []
    records = 0
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        records += 1
        try:
            raw = json.loads(line, parse_constant=reject_constant)
            measurements.append(parse_measurement(raw))
        except (ValueError, OverflowError) as error:
            rejected.append({"line": line_number, "reason": str(error), "raw_line": line.rstrip("\n")})
    current = current_measurements(measurements)
    # Preserve each distinct payload revision; repeat deliveries do not add rows.
    revisions = {}
    for measurement in measurements:
        key = measurement.event_id, measurement.revision_hash
        previous = revisions.get(key)
        if previous is None or (measurement.collected_at, measurement.ingested_at) > (
            previous.collected_at, previous.ingested_at
        ):
            revisions[key] = measurement
    return {
        "mode": "local_replay",
        "summary": {
            "input_records": records,
            "parsed_records": len(measurements),
            "rejected_records": len(rejected),
            "unique_revisions": len(revisions),
            "duplicate_deliveries": len(measurements) - len(revisions),
            "current_observations": len(current),
            "usable_observations": sum(not item.quality_flags for item in current),
        },
        "hourly_metrics": [item.as_dict() for item in hourly_metrics(current)],
        "current_measurements": [item.as_dict() for item in current],
        "measurement_revisions": [revisions[key].as_dict() for key in sorted(revisions)],
        "rejected_records": rejected,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Small internal measurement JSONL file")
    parser.add_argument(
        "--strict", action="store_true",
        help="Exit with status 1 if any records are rejected; still print the report",
    )
    args = parser.parse_args()
    try:
        with args.input.open(encoding="utf-8") as lines:
            result = replay(lines)
    except OSError as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2, allow_nan=False))
    return 1 if args.strict and result["summary"]["rejected_records"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
