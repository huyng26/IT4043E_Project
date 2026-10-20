"""OpenAQ v3 discovery, source coverage, and lossless measurement adaptation."""

import math
from collections import Counter
from datetime import timedelta

from collectors.storage import now
from streaming.measurements import digest, parse_measurement, timestamp, utc_time

BASE_URL = "https://api.openaq.org/v3"


class OpenAQ:
    def __init__(self, http, page_size=1000):
        self.http, self.page_size = http, page_size

    def pages(
        self, path, *, context=None, start_page=1, previous_fingerprint=None, **params
    ):
        previous = previous_fingerprint
        for number in range(start_page, 10001):
            request_params = {**params, "limit": self.page_size, "page": number}
            if context is None:
                response = self.http.get(f"{BASE_URL}/{path}", request_params)
            else:
                response = self.http.get(
                    f"{BASE_URL}/{path}", request_params, context=context
                )
            payload = response.payload
            if not isinstance(payload, dict) or not isinstance(
                payload.get("results"), list
            ):
                raise ValueError(
                    "OpenAQ response must contain results array; response captured"
                )
            meta = payload.get("meta") or {}
            if meta.get("page", number) != number:
                raise ValueError(
                    "OpenAQ returned the wrong page; refusing to advance progress"
                )
            rows = payload["results"]
            fingerprint = digest(rows)
            if rows and fingerprint == previous:
                raise ValueError("OpenAQ repeated a page; refusing to advance progress")
            previous = fingerprint
            found = meta.get("found")
            # 'found' can be '>1000', not an exact count.
            limit = meta.get("limit", self.page_size)
            if type(limit) is not int or limit < 1:
                raise ValueError("invalid OpenAQ page limit")
            if not rows:
                if type(found) is int and (number - 1) * limit < found:
                    raise ValueError(
                        "OpenAQ ended pagination before the reported total"
                    )
                yield response
                return
            if len(rows) < limit:
                if type(found) is int and number * limit < found:
                    raise ValueError("incomplete OpenAQ page")
            yield response
            if (type(found) is int and number * limit >= found) or len(rows) < limit:
                return
        raise ValueError(
            "OpenAQ pagination exceeded 10000 pages; narrow the time window"
        )

    def one(self, path):
        pages = self.pages(path)
        first = next(pages)
        rows = first.payload["results"]
        if len(rows) != 1:
            raise ValueError(f"OpenAQ {path}: expected one result")
        return rows[0]

    def measurements(self, sensor_id, start, end, context=None):
        return self.pages(
            f"sensors/{sensor_id}/measurements",
            context=context,
            datetime_from=timestamp(start),
            datetime_to=timestamp(end),
        )


def adapt_measurement(payload, sensor, collected_at, ingested_at=None):
    if not isinstance(payload, dict):
        raise ValueError("measurement must be an object")
    parameter = payload.get("parameter")
    if (
        not isinstance(parameter, dict)
        or not isinstance(parameter.get("name"), str)
        or not parameter["name"]
    ):
        raise ValueError("OpenAQ measurement parameter is required")
    period = payload.get("period")
    if not isinstance(period, dict):
        raise ValueError("OpenAQ measurement period is required")
    # Preserve any inline flags conservatively; coverage/summary are not quality flags.
    source_flags = []
    for field in ("flags", "flagInfo"):
        if payload.get(field):
            source_flags.append(f"openaq:{field}:{digest(payload[field])}")
    try:
        envelope = {
            "schema_version": 1,
            "source": "openaq",
            "station_id": f"openaq:{sensor['location_id']}",
            "sensor_id": str(sensor["sensor_id"]),
            "pollutant": parameter["name"],
            "value": payload.get("value"),
            "unit": parameter["units"],
            "period_start": period["datetimeFrom"]["utc"],
            "period_end": period["datetimeTo"]["utc"],
            "source_flags": source_flags,
            "provider_payload": payload,
            "collected_at": timestamp(collected_at),
            "ingested_at": timestamp(ingested_at or now()),
        }
        if "value" not in payload:
            raise ValueError("OpenAQ value field is missing")
        if envelope["pollutant"] == "pm25":
            measurement = parse_measurement(envelope)
            envelope.update(
                event_id=measurement.event_id, revision_hash=measurement.revision_hash
            )
        else:
            value = envelope["value"]
            if value is not None and (
                type(value) not in (int, float) or not math.isfinite(value)
            ):
                raise ValueError("measurement value must be finite or null")
            start = utc_time(envelope["period_start"], "period_start")
            end = utc_time(envelope["period_end"], "period_end")
            if (
                not start
                < end
                <= collected_at
                <= utc_time(envelope["ingested_at"], "ingested_at")
            ):
                raise ValueError("require start < end <= collected_at <= ingested_at")
            if not isinstance(envelope["unit"], str) or not envelope["unit"]:
                raise ValueError("measurement unit is required")
            identity = {
                field: envelope[field]
                for field in ("source", "station_id", "sensor_id", "pollutant")
            }
            identity.update(period_start=timestamp(start), period_end=timestamp(end))
            envelope.update(
                event_id=digest(identity),
                revision_hash=digest(
                    {
                        **identity,
                        **{
                            field: envelope[field]
                            for field in (
                                "value",
                                "unit",
                                "source_flags",
                                "provider_payload",
                            )
                        },
                    }
                ),
            )
    except (KeyError, TypeError) as error:
        raise ValueError(f"incomplete OpenAQ measurement: {error}") from error
    return envelope


def coverage_report(envelopes, start, end, rejected=0):
    """Count distinct supported source hours, never equate repeated polls with coverage."""
    current = {}
    periods, flags = Counter(), Counter()
    for envelope in envelopes:
        measurement = parse_measurement(envelope)
        previous = current.get(measurement.event_id)
        if previous is None or measurement.order > previous.order:
            current[measurement.event_id] = measurement
    observed, usable = set(), set()
    for measurement in current.values():
        periods[
            str((measurement.period_end - measurement.period_start).total_seconds())
        ] += 1
        flags.update(measurement.quality_flags)
        if (
            start <= measurement.period_start
            and measurement.period_end <= end
            and "unsupported_reporting_period" not in measurement.quality_flags
        ):
            observed.add(measurement.period_start)
            if not measurement.quality_flags:
                usable.add(measurement.period_start)
    expected = int((end - start).total_seconds() // 3600)
    return {
        "sample_start": timestamp(start),
        "sample_end": timestamp(end),
        "expected_hourly_periods": expected,
        "observed_hourly_periods": len(observed),
        "usable_hourly_periods": len(usable),
        "missing_hourly_periods": expected - len(observed),
        "usable_coverage_percent": 100 * len(usable) / expected if expected else None,
        "reporting_period_seconds": dict(periods),
        "quality_flags": dict(flags),
        "rejected_records": rejected,
        "missing_hours": [
            timestamp(start + timedelta(hours=i))
            for i in range(expected)
            if start + timedelta(hours=i) not in observed
        ],
    }
