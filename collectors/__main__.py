"""Global discovery, durable near-realtime crawling, and bounded reads."""

import argparse
import json
import logging
import os
import signal
import threading
from datetime import datetime, timedelta

from collectors.scheduler import Crawler, Lease
from collectors.service import recover_pending
from collectors.settings import load_settings
from collectors.storage import Store, now
from database.migrations import migrate_envelopes
from streaming.measurements import canonical_json, digest, timestamp, utc_time

LOGGER = logging.getLogger("collectors")


def parser():
    result = argparse.ArgumentParser(
        description="Collect every OpenAQ location globally"
    )
    result.add_argument("--config")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db")
    commands.add_parser("status")
    commands.add_parser("recover")
    commands.add_parser(
        "migrate",
        help="Project existing envelopes into schema v2 without deleting raw data",
    )
    discovery = commands.add_parser(
        "discover", help="Resume the complete global catalog"
    )
    discovery.add_argument(
        "--max-pages", type=int, default=0, help="0 means finish the catalog"
    )
    discovery.add_argument("--refresh", action="store_true")
    for name in ("run", "poll"):
        command = commands.add_parser(name)
        command.add_argument(
            "--source", choices=("all", "openaq", "weather"), default="all"
        )
        if name == "run":
            command.add_argument("--publish", action="store_true")
        else:
            command.add_argument("--max-jobs", type=int, default=10)
    backfill = commands.add_parser(
        "backfill", help="Queue bounded history for every catalog sensor"
    )
    backfill.add_argument("--start")
    backfill.add_argument("--end")
    publish = commands.add_parser("publish")
    publish.add_argument("--limit", type=int, default=1000)
    cities = commands.add_parser("cities")
    cities.add_argument("--country")
    cities.add_argument("--after")
    cities.add_argument("--limit", type=int, default=100)
    history = commands.add_parser("history")
    history.add_argument("--station", required=True)
    history.add_argument("--parameter")
    history.add_argument("--start", required=True)
    history.add_argument("--end", required=True)
    history.add_argument("--cursor")
    history.add_argument("--limit", type=int, default=100)
    return result


def create_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092").split(
            ","
        ),
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda value: canonical_json(value).encode(),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
        max_block_ms=10000,
    )


def publish_once(store, limit=1000):
    store.ensure_job("kafka:publisher", "internal", "publisher", {})
    job = store.claim_job("internal", os.urandom(16).hex(), kinds=["publisher"])
    if job is None:
        return 0
    producer = None
    with Lease(store.mongodb, job, 120) as lease:
        try:
            producer = create_producer()
            count = store.publish(producer, limit)
            lease.check()
            store.finish_job(job, now())
            return count
        except Exception as error:
            store.finish_job(job, now(), error=str(error))
            raise
        finally:
            if producer is not None:
                producer.close(timeout=5)


def run(crawler, sources, publish):
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: crawler.stop.set())

    def publisher():
        while not crawler.stop.is_set():
            try:
                count = publish_once(crawler.store)
                LOGGER.info("Kafka acknowledged %s records", count)
                if count < 1000:
                    crawler.stop.wait(5)
            except Exception:
                LOGGER.exception("Kafka unavailable; pending records retained")
                crawler.stop.wait(10)

    thread = threading.Thread(target=publisher, daemon=True) if publish else None
    if thread:
        thread.start()
    try:
        crawler.run(sources)
    finally:
        crawler.stop.set()
        if thread:
            thread.join(timeout=40)


def enqueue_backfill(store, settings, start, end):
    if not start < end <= now():
        raise ValueError("require start < end <= current UTC time")
    count = 0
    for sensor in store.iter_sensors():
        identity = "openaq:backfill:" + digest(
            [sensor["_id"], timestamp(start), timestamp(end)]
        )
        store.ensure_job(
            identity,
            "openaq",
            "backfill",
            {
                "sensor_id": sensor["source_id"],
                "location_id": sensor["location_id"],
                "start": start,
                "end": end,
            },
            priority=5,
        )
        count += 1
    return {"queued_sensor_jobs": count, "start": start, "end": end}


def main(argv=None):
    args = parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    store = None
    try:
        config = load_settings(args.config)
        store = Store()
        store.initialize()
        crawler = Crawler(store, config)
        output = None
        failed = False
        if args.command == "init-db":
            output = {
                "database": store.mongodb.db.name,
                "schema_version": 2,
                "status": "ready",
            }
        elif args.command == "status":
            output = store.status()
        elif args.command == "recover":
            output = {"recovered_responses": recover_pending(store)}
        elif args.command == "migrate":
            output = migrate_envelopes(store.mongodb)
        elif args.command == "discover":
            if args.max_pages < 0:
                raise ValueError("max-pages must be nonnegative")
            crawler.bootstrap(["openaq"])
            if args.refresh:
                store.request_job("openaq:catalog", priority=40)
            completed = 0
            while not args.max_pages or completed < args.max_pages:
                result = crawler.tick("openaq", kinds=["catalog"])
                if result is None:
                    break
                if result["status"] == "failed":
                    failed = True
                    break
                completed += 1
            output = {"processed_pages": completed, **store.status()}
        elif args.command in ("run", "poll"):
            sources = (
                ["openaq", "open-meteo"]
                if args.source == "all"
                else ["open-meteo" if args.source == "weather" else "openaq"]
            )
            if "openaq" in sources and not os.getenv("OPENAQ_API_KEY", "").strip():
                raise ValueError("set OPENAQ_API_KEY in .env")
            crawler.bootstrap(sources)
            if args.command == "run":
                run(crawler, sources, args.publish)
                output = {"status": "stopped"}
            else:
                if args.max_jobs < 1:
                    raise ValueError("max-jobs must be positive")
                output = []
                for _ in range(args.max_jobs):
                    results = [crawler.tick(source) for source in sources]
                    output.extend(result for result in results if result is not None)
                    if all(result is None for result in results):
                        break
                failed = any(result["status"] == "failed" for result in output)
        elif args.command == "backfill":
            end = (
                utc_time(args.end, "end")
                if args.end
                else now().replace(minute=0, second=0, microsecond=0)
            )
            start = (
                utc_time(args.start, "start")
                if args.start
                else end - timedelta(days=config["openaq"]["history_days"])
            )
            output = enqueue_backfill(store, config["openaq"], start, end)
        elif args.command == "publish":
            if not 1 <= args.limit <= 1000:
                raise ValueError("limit must be 1..1000")
            output = {"published": publish_once(store, args.limit)}
        elif args.command == "cities":
            output = store.list_cities(args.country, after=args.after, limit=args.limit)
        elif args.command == "history":
            output = store.get_measurements(
                args.station,
                utc_time(args.start, "start"),
                utc_time(args.end, "end"),
                parameter=args.parameter,
                cursor=args.cursor,
                limit=args.limit,
            )
        print(
            json.dumps(
                output,
                ensure_ascii=False,
                indent=2,
                default=lambda value: (
                    timestamp(value) if isinstance(value, datetime) else str(value)
                ),
            )
        )
        return 1 if failed else 0
    except Exception as error:
        LOGGER.error("%s", error)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
