"""Bounded HTTP retries, provider budgets, and capture before adaptation."""

import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import requests

from collectors.storage import now, wait
from streaming.measurements import digest
from streaming.replay import reject_constant


@dataclass
class Response:
    source: str
    url: str
    params: dict
    payload: object
    body: str
    headers: dict
    collected_at: datetime
    request_id: str
    context: dict | None = None


class SourceHTTPError(RuntimeError):
    def __init__(self, source, status_code, hint=""):
        self.source, self.status_code = source, status_code
        super().__init__(f"{source}: HTTP {status_code}{hint}")


def reset_seconds(value):
    """Handle delta seconds, Unix reset time, or HTTP-date Retry-After."""
    try:
        number = float(value)
        if number > 1_000_000_000:
            return max(0, number - now().timestamp())
        return max(0, number)
    except (ValueError, TypeError):
        try:
            return max(
                0,
                (parsedate_to_datetime(value).astimezone(UTC) - now()).total_seconds(),
            )
        except (ValueError, TypeError, OverflowError):
            return 60


class HTTPClient:
    def __init__(self, source, store, api_key=None, attempts=5, session=None):
        self.source, self.store, self.attempts = source, store, attempts
        self.session = session or requests.Session()
        self.headers = {"User-Agent": "urban-air-quality/0.1 (academic collector)"}
        if api_key:
            self.headers["X-API-Key"] = api_key
        # Same key shares one budget without storing credentials in MongoDB.
        key_hash = hashlib.sha256((api_key or "public").encode()).hexdigest()
        self.scope = f"{source}:{key_hash}"

    def get(self, url, params, context=None, request_units=1):
        for attempt in range(self.attempts):
            self.store.reserve_request(
                self.scope,
                minute_limit=60,
                hour_limit=2000 if self.source == "openaq" else 5000,
                day_limit=10000 if self.source == "open-meteo" else None,
                units=request_units,
            )
            try:
                result = self.session.get(
                    url, params=params, headers=self.headers, timeout=(5, 30)
                )
            except requests.RequestException:
                if attempt + 1 == self.attempts:
                    raise RuntimeError(
                        f"{self.source}: network request failed after retries"
                    ) from None
                wait(2**attempt + random.random(), self.store.stop_event)
                continue
            remaining = result.headers.get("x-ratelimit-remaining")
            if remaining is not None and reset_seconds(remaining) == 0:
                self.store.defer_requests(
                    self.scope,
                    reset_seconds(result.headers.get("x-ratelimit-reset", "60")),
                )
            if result.status_code == 429:
                delays = [60]
                for header in ("Retry-After", "x-ratelimit-reset"):
                    if header in result.headers:
                        delays.append(reset_seconds(result.headers[header]))
                self.store.defer_requests(self.scope, max(delays))
            if (
                result.status_code in (408, 429, 500, 502, 503, 504)
                and attempt + 1 < self.attempts
            ):
                wait(2**attempt + random.random(), self.store.stop_event)
                continue
            if result.status_code != 200:
                hint = (
                    "; check OPENAQ_API_KEY" if result.status_code in (401, 403) else ""
                )
                raise SourceHTTPError(self.source, result.status_code, hint)
            response = Response(
                self.source,
                url,
                dict(params),
                None,
                result.text,
                {
                    k.lower(): v
                    for k, v in result.headers.items()
                    if k.lower() in ("date", "content-type", "retry-after")
                    or k.lower().startswith("x-ratelimit-")
                },
                now(),
                str(uuid.uuid4()),
                context,
            )
            self.store.capture(response)
            try:
                response.payload = json.loads(
                    result.text, parse_constant=reject_constant
                )
                digest(response.payload)
            except ValueError:
                raise ValueError(
                    f"{self.source}: invalid JSON; original response was captured"
                ) from None
            return response
        raise RuntimeError(f"{self.source}: retries exhausted")

    def close(self):
        self.session.close()
