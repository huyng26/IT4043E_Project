"""Re-derive ``air_quality_raw`` envelopes from captured OpenAQ responses.

Envelopes staged before commit 5bf84c9 carry a bogus ``source_flags`` entry for
OpenAQ's ``flagInfo {"hasFlags": false}``, so every row looks source-flagged. The
original responses are kept in ``raw_responses``; this command runs today's
``adapt_measurement`` over them again and writes the envelopes to a new JSONL file::

    python -m collectors.regenerate --output data/outputs/real/regenerated.jsonl

It is **read-only** towards MongoDB: the outbox, Kafka and the serving database are
not touched, so nothing is republished. Output is deterministic: responses in
capture order (``collected_at``, then ID), rows in page order, and ``ingested_at``
set to the response's ``collected_at`` (the original used the staging clock, which
is not recorded). Identities (``event_id``) match the outbox; revision hashes
differ wherever the flags changed. A report with counts and the file's SHA-256 is
written next to the output.

``--repair-outbox`` instead fixes the stale rows in the collector outbox itself,
which is what the live Kafka/Spark path will publish. Only rows that are still
unpublished are changed, each envelope is recomputed by ``adapt_measurement``
from its own stored ``provider_payload`` with its original ``collected_at``,
``ingested_at`` and ``raw_response_id``, and a row is skipped if its identity
would change or it was published or modified meanwhile. Stop the crawler and back
up the outbox first; ``--dry-run`` reports what would change::

    python -m collectors.regenerate --repair-outbox --dry-run
    python -m collectors.regenerate --repair-outbox
"""

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path

from collectors.openaq import adapt_measurement
from models import canonical_json, digest, timestamp, utc_time

UNFLAGGED = {"hasFlags": False}
STALE_FLAG = "openaq:flagInfo:" + digest(UNFLAGGED)
REPAIR_NAME = "openaq-hasflags-false-5bf84c9"


def _reject_constant(value):
    raise ValueError(f"nonstandard JSON number: {value}")


def _rejected(payload, row, error, collected, **identity):
    """Same shape as ``collectors.storage.rejected_envelope`` with a fixed clock."""
    return {
        "schema_version": 1,
        "source": row["source"],
        **identity,
        "status": "rejected",
        "error": str(error),
        "collected_at": timestamp(collected),
        "ingested_at": timestamp(collected),
        "provider_payload": payload,
        "raw_response_id": row["_id"],
    }


def regenerate_response(row):
    """Return the envelopes the current collector stages for one captured page.

    Mirrors ``collectors.service.recover_pending``/``stage_measurements``: a page
    that is not a results list becomes one rejected envelope, and a row the adapter
    refuses becomes a rejected envelope with the provider payload kept.
    """
    sensor = row["context"]["sensor"]
    station = f"openaq:{sensor['location_id']}"
    collected = utc_time(
        row.get("collected_at_iso") or timestamp(row["collected_at"]), "collected_at"
    )
    try:
        page = json.loads(row["body"], parse_constant=_reject_constant)
    except (TypeError, ValueError):
        page = row["body"]
    if not isinstance(page, dict) or not isinstance(page.get("results"), list):
        return [
            _rejected(page, row, ValueError("invalid OpenAQ response"), collected, station_id=station)
        ]
    envelopes = []
    for payload in page["results"]:
        try:
            envelope = adapt_measurement(payload, sensor, collected, ingested_at=collected)
        except ValueError as error:
            envelope = _rejected(
                payload, row, error, collected, station_id=station, sensor_id=str(sensor["sensor_id"])
            )
        envelope["raw_response_id"] = row["_id"]
        envelopes.append(envelope)
    return envelopes


def regenerate(rows, handle, *, pollutants=None):
    """Write regenerated envelopes as JSONL to ``handle``; return a count report."""
    digest = hashlib.sha256()
    counts = Counter()
    by_pollutant = Counter()
    for row in rows:
        counts["responses"] += 1
        for envelope in regenerate_response(row):
            pollutant = envelope.get("pollutant")
            if pollutants and pollutant not in pollutants:
                continue
            line = (canonical_json(envelope) + "\n").encode("utf-8")
            handle.write(line)
            digest.update(line)
            counts["envelopes"] += 1
            if envelope.get("status") == "rejected":
                counts["rejected"] += 1
                continue
            by_pollutant[pollutant] += 1
            if envelope["source_flags"]:
                counts["source_flagged"] += 1
            if envelope["provider_payload"].get("flagInfo") == UNFLAGGED:
                counts["flaginfo_has_flags_false"] += 1
    return {
        **{key: counts[key] for key in ("responses", "envelopes", "rejected", "source_flagged", "flaginfo_has_flags_false")},
        "by_pollutant": dict(by_pollutant.most_common()),
        "sha256": digest.hexdigest(),
    }


def repaired_envelope(envelope):
    """Recompute one stale outbox envelope with today's adapter, or return None.

    Keeps the original timestamps and response lineage. Returns None when the
    envelope cannot be re-adapted or its identity would change, so such rows are
    reported instead of rewritten.
    """
    station = envelope.get("station_id", "")
    if not isinstance(station, str) or not station.startswith("openaq:"):
        return None
    sensor = {"location_id": station.removeprefix("openaq:"), "sensor_id": envelope.get("sensor_id")}
    try:
        fixed = adapt_measurement(
            envelope["provider_payload"],
            sensor,
            utc_time(envelope["collected_at"], "collected_at"),
            ingested_at=utc_time(envelope["ingested_at"], "ingested_at"),
        )
    except (KeyError, TypeError, ValueError):
        return None
    for field in ("collected_at", "ingested_at"):
        fixed[field] = envelope[field]  # keep the exact original text
    if "raw_response_id" in envelope:
        fixed["raw_response_id"] = envelope["raw_response_id"]
    if fixed["event_id"] != envelope.get("event_id"):
        return None
    return fixed


def repair_outbox(repository, *, dry_run=False, batch_size=1000):
    """Replace stale flagged envelopes in unpublished outbox rows; return counts."""
    counts = Counter()
    batch = []

    def flush():
        if batch and not dry_run:
            counts["replaced"] += repository.replace_unpublished_envelopes(batch, repair=REPAIR_NAME)
        batch.clear()

    for row in repository.iter_outbox_with_flag("air_quality_raw", STALE_FLAG):
        counts["stale_rows"] += 1
        fixed = repaired_envelope(row["envelope"])
        if fixed is None:
            counts["skipped_not_repairable"] += 1
            continue
        if fixed["source_flags"]:
            counts["still_flagged"] += 1
        counts["would_replace"] += 1
        batch.append((row["_id"], row["revision_hash"], fixed))
        if len(batch) >= batch_size:
            flush()
    flush()
    keys = ("stale_rows", "would_replace", "replaced", "still_flagged", "skipped_not_repairable")
    return {**{key: counts[key] for key in keys}, "dry_run": dry_run}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, help="new JSONL file (never overwritten)")
    parser.add_argument(
        "--repair-outbox",
        action="store_true",
        help="fix stale unpublished rows in the collector outbox instead of writing a file",
    )
    parser.add_argument("--dry-run", action="store_true", help="with --repair-outbox: count only")
    parser.add_argument("--pollutant", action="append", help="keep only this pollutant (repeatable)")
    parser.add_argument("--limit-responses", type=int, default=0, help="0 means every captured page")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"))
    parser.add_argument(
        "--database",
        default=os.getenv(
            "MONGO_COLLECTOR_DATABASE",
            os.getenv("MONGO_DATABASE", "urban_air_quality") + "_collector",
        ),
        help="collector state database holding raw_responses",
    )
    args = parser.parse_args(argv)
    if args.limit_responses < 0:
        parser.error("--limit-responses must be nonnegative")
    from database import MongoDB

    if args.repair_outbox:
        if args.output or args.pollutant or args.limit_responses:
            parser.error("--repair-outbox does not take --output, --pollutant or --limit-responses")
        with MongoDB(args.mongo_uri, args.database) as store:
            report = repair_outbox(store, dry_run=args.dry_run)
        report["source_database"] = args.database
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    if args.output is None:
        parser.error("--output is required unless --repair-outbox is given")

    report_path = args.output.with_name(args.output.name + ".report.json")
    if report_path.exists():
        print(f"error: refusing to overwrite {report_path}", file=sys.stderr)
        return 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with MongoDB(args.mongo_uri, args.database) as source, args.output.open("xb") as handle:
            report = regenerate(
                source.iter_raw_responses("openaq", args.limit_responses),
                handle,
                pollutants=set(args.pollutant or ()),
            )
    except FileExistsError:
        print(f"error: refusing to overwrite {args.output}", file=sys.stderr)
        return 2
    report.update(
        source_database=args.database,
        output=str(args.output),
        pollutant_filter=sorted(args.pollutant or []),
        limit_responses=args.limit_responses,
        ingested_at_policy="collected_at of the captured response",
    )
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
