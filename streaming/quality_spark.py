"""Experiment-only Spark job: quality_replay_raw -> shared rules -> experiment MongoDB.

Separate from ``streaming.spark`` on purpose: it subscribes only to the replay
topic, keeps its own checkpoint and writes only to a dedicated experiment
database, so the live consumer and serving database never see a wrapper::

    python -m streaming.quality_spark --run-id <run_id> \\
        --database urban_air_quality_quality --available-now

One job processes one run. Wrappers of other runs on the topic are skipped and
counted. Each run should use a new checkpoint directory (the default derives it
from the run ID).
"""

import argparse
import os
from collections import Counter
from functools import partial

from streaming.quality.contracts import REPLAY_TOPIC
from streaming.quality.dispatcher import OUTCOMES, process_replay_partition
from streaming.quality.rules import RULE_VERSION, SUPPORTED_RULE_VERSIONS
from streaming.spark import KAFKA_PACKAGE

LIVE_CHECKPOINT_DEFAULTS = ("data/checkpoints/spark-mongo", "/checkpoints/mongo")


def _partition_counts(records, **options):
    """mapPartitions adapter: yield one outcome-count dict per partition."""
    yield process_replay_partition(records, **options)


def write_batch(batch, batch_id, *, uri, database, run_id, rule_version, query_name):
    from pyspark.sql import functions as F

    from database import MongoDB

    batch.persist()
    try:
        per_partition = batch.rdd.mapPartitions(
            partial(
                _partition_counts,
                mongo_uri=uri,
                experiment_database=database,
                run_id=run_id,
                rule_version=rule_version,
            )
        ).collect()
        counts = Counter({outcome: 0 for outcome in OUTCOMES})
        for partition in per_partition:
            counts.update(partition)
        offsets = [
            row.asDict()
            for row in batch.groupBy("topic", "partition")
            .agg(
                F.min("offset").alias("first_offset"),
                F.max("offset").alias("last_offset"),
            )
            .collect()
        ]
        with MongoDB(uri, database) as repository:
            repository.complete_quality_batch(query_name, batch_id, offsets, dict(counts))
    finally:
        batch.unpersist()


def parser():
    result = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    result.add_argument("--run-id", required=True, help="run ID printed by quality_replay build")
    result.add_argument(
        "--database",
        default=os.getenv("QUALITY_MONGO_DATABASE"),
        help="dedicated experiment database (default: QUALITY_MONGO_DATABASE)",
    )
    result.add_argument("--rule-version", default=RULE_VERSION)
    result.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    result.add_argument(
        "--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    )
    result.add_argument(
        "--checkpoint-root",
        default=os.getenv("QUALITY_CHECKPOINT_ROOT", "data/checkpoints/quality"),
        help="the checkpoint is <root>/<run-id> unless --checkpoint is given",
    )
    result.add_argument("--checkpoint")
    result.add_argument("--topic", default=REPLAY_TOPIC)
    result.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[2]"))
    result.add_argument("--trigger-seconds", type=int, default=5)
    result.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    result.add_argument("--starting-offsets", default="earliest")
    result.add_argument(
        "--available-now", action="store_true", help="drain available input and exit"
    )
    return result


def validate_args(args, environ=None):
    """Refuse settings that could touch live data; return the checkpoint path."""
    from database.quality_schema import check_experiment_database

    check_experiment_database(args.database, environ)
    if args.rule_version not in SUPPORTED_RULE_VERSIONS:
        raise ValueError(f"unsupported rule version: {args.rule_version}")
    if not args.topic.startswith(REPLAY_TOPIC):
        raise ValueError(f"the quality job only reads {REPLAY_TOPIC}* topics")
    if not args.run_id.strip() or "/" in args.run_id:
        raise ValueError("run_id must be a nonempty name without '/'")
    if args.trigger_seconds < 1 or args.max_offsets_per_trigger < 1:
        raise ValueError("trigger-seconds and max-offsets-per-trigger must be positive")
    checkpoint = args.checkpoint or os.path.join(args.checkpoint_root, args.run_id)
    live = {os.path.normpath(p) for p in LIVE_CHECKPOINT_DEFAULTS}
    environ = os.environ if environ is None else environ
    if environ.get("STREAMING_CHECKPOINT_LOCATION"):
        live.add(os.path.normpath(environ["STREAMING_CHECKPOINT_LOCATION"]))
    if os.path.normpath(checkpoint) in live:
        raise ValueError("the quality job must not share the live Spark checkpoint")
    return checkpoint


def main(argv=None):
    from dotenv import load_dotenv

    load_dotenv()
    args = parser().parse_args(argv)
    checkpoint = validate_args(args)

    from pyspark.sql import SparkSession

    from database import MongoDB

    with MongoDB(args.mongo_uri, args.database) as repository:
        repository.initialize_quality()
    query_name = f"quality-{args.run_id}"
    builder = (
        SparkSession.builder.appName(query_name)
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.speculation", "false")
    )
    package = os.getenv("SPARK_KAFKA_PACKAGE", KAFKA_PACKAGE)
    if package:
        builder = builder.config("spark.jars.packages", package)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        writer = (
            events.select("topic", "partition", "offset", "timestamp", "key", "value")
            .writeStream.queryName(query_name)
            .option("checkpointLocation", checkpoint)
            .foreachBatch(
                partial(
                    write_batch,
                    uri=args.mongo_uri,
                    database=args.database,
                    run_id=args.run_id,
                    rule_version=args.rule_version,
                    query_name=query_name,
                )
            )
        )
        writer = (
            writer.trigger(availableNow=True)
            if args.available_now
            else writer.trigger(processingTime=f"{args.trigger_seconds} seconds")
        )
        query = writer.start()
        query.awaitTermination()
        return 0
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
