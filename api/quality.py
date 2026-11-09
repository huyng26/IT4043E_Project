"""Placeholder for the future GET /api/v1/quality/summary route.

The FastAPI application/dependencies are not implemented. Keep imports free of
optional web dependencies until the serving milestone. This file registers no
route and queries no database.
"""


def create_quality_router(repository: object) -> object:
    """TODO: create a FastAPI router backed by bounded MongoDB quality reads.

    Require explicit live versus experiment/run scope; return rule version,
    terminal counts, repair/flag attributes, coverage and rejection reasons.
    Validate station/time filters, enforce bounded queries and preserve nulls.
    Read prepared summaries; do not trigger replay, training or Spark in a request.
    Register this router only once an API application exists.
    """
    raise NotImplementedError("The quality summary API is not implemented")
