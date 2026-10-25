"""Kafka → Spark Structured Streaming → MongoDB serving projections."""

import argparse
import os
from functools import partial

from streaming.processing import TOPICS, process_partition

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def write_batch(batch, batch_id, *, uri, database, query_name):
    from pyspark.sql import functions as F

    from database import MongoDB

    batch.persist()
    try:
        batch.foreachPartition(partial(process_partition, uri=uri, database=database))
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
            repository.complete_stream_batch(query_name, batch_id, offsets)
    finally:
        batch.unpersist()


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    result.add_argument(
        "--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    )
    result.add_argument(
        "--database", default=os.getenv("MONGO_DATABASE", "urban_air_quality")
    )
    result.add_argument(
        "--checkpoint",
        default=os.getenv(
            "STREAMING_CHECKPOINT_LOCATION", "data/checkpoints/spark-mongo"
        ),
    )
    result.add_argument("--query-name", default="kafka-mongo")
    result.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[2]"))
    result.add_argument("--trigger-seconds", type=int, default=30)
    result.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    result.add_argument(
        "--starting-offsets",
        default="earliest",
        help="earliest, latest, or Kafka's per-topic/partition JSON offset map",
    )
    result.add_argument(
        "--available-now", action="store_true", help="Drain available input and exit"
    )
    return result


def main(argv=None):
    # Load the environment before argparse captures its defaults.
    from dotenv import load_dotenv

    load_dotenv()
    args = parser().parse_args(argv)
    if args.trigger_seconds < 1 or args.max_offsets_per_trigger < 1:
        raise ValueError("trigger-seconds and max-offsets-per-trigger must be positive")
    if args.database == os.getenv(
        "MONGO_COLLECTOR_DATABASE",
        os.getenv("MONGO_DATABASE", "urban_air_quality") + "_collector",
    ):
        raise ValueError(
            "Spark must write to the serving database, not collector state"
        )

    from pyspark.sql import SparkSession

    from database import MongoDB

    with MongoDB(args.mongo_uri, args.database) as repository:
        repository.initialize()
    builder = SparkSession.builder.appName(args.query_name).master(args.master)
    builder = builder.config("spark.sql.session.timeZone", "UTC").config(
        "spark.speculation", "false"
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
            .option("subscribe", ",".join(TOPICS))
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        writer = (
            events.select("topic", "partition", "offset", "timestamp", "key", "value")
            .writeStream.queryName(args.query_name)
            .option("checkpointLocation", args.checkpoint)
            .foreachBatch(
                partial(
                    write_batch,
                    uri=args.mongo_uri,
                    database=args.database,
                    query_name=args.query_name,
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
