"""Persistent quota-aware near-realtime collection across the global API catalog."""

import logging
import math
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from collectors.catalog import is_last_page, register_station, sync_catalog
from collectors.http import HTTPClient
from collectors.openaq import OpenAQ
from collectors.service import (
    collect_sensor,
    recover_pending,
    stage_latest,
    stage_weather,
)
from collectors.storage import now
from collectors.weather import fetch_weather
from streaming.measurements import timestamp

LOGGER = logging.getLogger(__name__)


class Lease:
    def __init__(self, repository, job, seconds):
        self.repository, self.job, self.seconds = repository, job, seconds
        self.stop, self.lost = threading.Event(), threading.Event()

    def __enter__(self):
        def renew():
            while not self.stop.wait(self.seconds / 3):
                try:
                    if not self.repository.renew_job(
                        self.job["_id"], self.job["owner"], self.seconds
                    ):
                        self.lost.set()
                        return
                except Exception:
                    self.lost.set()
                    LOGGER.exception("Cannot renew job lease %s", self.job["_id"])
                    return

        self.thread = threading.Thread(target=renew, daemon=True)
        self.thread.start()
        return self

    def check(self):
        if self.lost.is_set():
            raise RuntimeError("job lease was lost")

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=5)


class Crawler:
    def __init__(self, store, config, http_factory=None):
        self.store, self.config = store, config
        self.stop = threading.Event()
        self.store.stop_event = self.stop
        self.http_factory = http_factory or self._http
        self._turn, self._turn_lock = 0, threading.Lock()

    def _http(self, source):
        key = os.getenv("OPENAQ_API_KEY", "").strip() if source == "openaq" else None
        if source == "openaq" and not key:
            raise ValueError(
                "set OPENAQ_API_KEY in .env before global OpenAQ collection"
            )
        return HTTPClient(source, self.store, key)

    def bootstrap(self, sources):
        if "openaq" in sources:
            self.store.set_job_priority("openaq", "station", 5)
            self.store.ensure_job(
                "openaq:catalog", "openaq", "catalog", {}, priority=40
            )
        if "open-meteo" in sources and self.config["weather"]["enabled"]:
            for location in self.config["weather"].get("locations", []):
                self.store.save_manual_weather_location(location)
                self.store.ensure_job(
                    f"weather:{location['location_id']}",
                    "open-meteo",
                    "weather",
                    {"location_id": location["location_id"]},
                )

    def _sensor(self, client, job):
        settings, checkpoint = self.config["openaq"], dict(job["checkpoint"])
        sensor = job["payload"]
        progress_key = f"openaq:live:{sensor['sensor_id']}"
        if job["kind"] == "backfill":
            progress_key = job["_id"]
            cursor = checkpoint.get("cursor", sensor["start"])
            target = sensor["end"]
        elif checkpoint:
            cursor, target = checkpoint["cursor"], checkpoint["target"]
        else:
            target = now().replace(microsecond=0)
            previous = self.store.progress(progress_key)
            cursor = (
                (
                    min(previous["completed_until"], target)
                    - timedelta(hours=settings["lookback_hours"])
                )
                if previous
                else target - timedelta(hours=settings["bootstrap_hours"])
            )
        end = min(target, cursor + timedelta(hours=settings["window_hours"]))
        if cursor < end:
            collect_sensor(
                client,
                self.store,
                sensor,
                cursor,
                end,
                settings["window_hours"],
                progress_key,
            )
        if end < target:
            return now(), {"cursor": end, "target": target}
        if job["kind"] == "backfill":
            # A completed bounded job stays dormant; it can still be inspected and retried.
            return now() + timedelta(days=36500), {
                "cursor": target,
                "target": target,
                "complete": True,
            }
        return now() + timedelta(seconds=settings["reconcile_interval_seconds"]), {}

    def _latest(self, client, job):
        parameter = job["payload"]["parameter"]
        checkpoint = job["checkpoint"]
        scan_min = checkpoint.get("datetime_min") or timestamp(
            now() - timedelta(hours=self.config["openaq"]["latest_lookback_hours"])
        )
        page_number = (
            checkpoint.get("next_page", 1) if checkpoint.get("datetime_min") else 1
        )
        for response in client.pages(
            f"parameters/{parameter['id']}/latest",
            start_page=page_number,
            previous_fingerprint=checkpoint.get("last_page_hash")
            if checkpoint.get("datetime_min")
            else None,
            context={"kind": "openaq_latest", "parameter": parameter},
            datetime_min=scan_min,
        ):
            stage_latest(self.store, response, parameter)
            if is_last_page(response, client.page_size):
                return now() + timedelta(
                    seconds=self.config["openaq"]["latest_interval_seconds"]
                ), {}
            from streaming.measurements import digest

            return now(), {
                "next_page": page_number + 1,
                "last_page_hash": digest(response.payload["results"]),
                "datetime_min": scan_min,
            }
        raise ValueError("latest scan returned no page")

    def _weather(self, http, job):
        location = self.store.get_weather_location(job["payload"]["location_id"])
        if location is None:
            raise ValueError("weather location no longer exists")
        settings = dict(self.config["weather"])
        previous = self.store.progress(job["_id"])
        if previous:
            gap = math.ceil(
                (now() - previous["completed_until"]).total_seconds() / 86400
            )
            settings["past_days"] = max(settings["past_days"], gap + 1)
            if settings["past_days"] > 92:
                raise ValueError(
                    "weather gap exceeds API history; historical archive backfill required"
                )
        response = fetch_weather(http, location, settings)
        envelopes = stage_weather(self.store, response, location, settings["model"])
        self.store.complete(
            job["_id"],
            response.collected_at,
            records=len(envelopes),
            model=settings["model"],
        )
        return now() + timedelta(seconds=settings["poll_seconds"]), {}

    def execute(self, job, http):
        if job["kind"] == "weather":
            return self._weather(http, job)
        client = OpenAQ(http, self.config["openaq"]["page_size"])
        if job["kind"] == "catalog":
            checkpoint, finished = sync_catalog(client, self.store, self.config, job)
            delay = self.config["openaq"]["catalog_interval_seconds"] if finished else 0
            return now() + timedelta(seconds=delay), checkpoint
        if job["kind"] == "latest":
            return self._latest(client, job)
        if job["kind"] == "station":
            register_station(
                self.store,
                client.one(f"locations/{job['payload']['location_id']}"),
                self.config,
            )
            return now() + timedelta(
                seconds=self.config["openaq"]["catalog_interval_seconds"]
            ), {}
        if job["kind"] in ("sensor", "backfill"):
            return self._sensor(client, job)
        raise ValueError(f"unknown job kind: {job['kind']}")

    def tick(self, source, kinds=None):
        if kinds is None and source == "openaq":
            with self._turn_lock:
                self._turn += 1
                history_turn = self._turn % 10 == 0
            if history_turn:
                result = self.tick(source, kinds=["backfill"])
                if result is not None:
                    return result
        job = self.store.claim_job(
            source, uuid.uuid4().hex, self.config["scheduler"]["lease_seconds"], kinds
        )
        if job is None:
            return None
        http = None
        with Lease(
            self.store.mongodb, job, self.config["scheduler"]["lease_seconds"]
        ) as lease:
            try:
                recover_pending(self.store, source)
                http = self.http_factory(source)
                next_run, checkpoint = self.execute(job, http)
                lease.check()
                if not self.store.finish_job(job, next_run, checkpoint):
                    raise RuntimeError("lease expired before job completion")
                return {"job": job["_id"], "status": "ok"}
            except InterruptedError:
                self.store.release_job(job)
                return None
            except Exception as error:
                delay = min(
                    3600,
                    self.config["scheduler"]["retry_seconds"]
                    * 2 ** min(job["failures"], 6),
                )
                if getattr(error, "status_code", None) in (404, 410):
                    delay = 86400
                self.store.finish_job(
                    job, now() + timedelta(seconds=delay), error=str(error)
                )
                LOGGER.exception(
                    "Crawl job %s failed; durable retry scheduled", job["_id"]
                )
                return {"job": job["_id"], "status": "failed", "error": str(error)}
            finally:
                if http is not None:
                    http.close()

    def run(self, sources):
        self.bootstrap(sources)

        def worker(source, slot):
            turn = 0
            while not self.stop.is_set():
                try:
                    turn += 1
                    preferred = None
                    if source == "openaq":
                        if slot == 0:
                            preferred = (
                                ["sensor", "backfill"]
                                if turn % 5 == 0
                                else ["catalog"]
                                if turn % 2 == 0
                                else ["latest"]
                            )
                        else:
                            preferred = (
                                ["backfill", "station"]
                                if turn % 10 == 0
                                else ["sensor"]
                            )
                    result = self.tick(source, kinds=preferred)
                    if result is None and preferred is not None:
                        result = self.tick(source)
                    if result is None:
                        self.stop.wait(self.config["scheduler"]["idle_seconds"])
                except InterruptedError:
                    return
                except Exception:
                    LOGGER.exception(
                        "%s worker lost a connection; leases will recover unfinished work",
                        source,
                    )
                    self.stop.wait(self.config["scheduler"]["retry_seconds"])

        workers = [
            (
                source,
                self.config["openaq" if source == "openaq" else "weather"][
                    "worker_count"
                ],
            )
            for source in sources
            if source != "open-meteo" or self.config["weather"]["enabled"]
        ]
        with ThreadPoolExecutor(
            max_workers=max(1, sum(count for _, count in workers))
        ) as executor:
            futures = [
                executor.submit(worker, source, slot)
                for source, count in workers
                for slot in range(count)
            ]
            for future in futures:
                future.result()
