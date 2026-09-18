"""Identity environment for the process-bigraph event stream (observability plan D4a).

Every task viva-api dispatches -- a Nextflow Batch task, a chain-dispatch
container job, an MNP node -- gets a handful of ``PBG_*`` environment variables
that tell the simulation engine WHO it is running for, so the events it emits
carry the campaign's identity without the engine knowing anything about
viva-api, AWS Batch or Kubernetes. Until an engine that reads them runs
(process-bigraph >= 1.9, ``process_bigraph.events``), they are inert.

The identity is modelled OpenTelemetry-style, deliberately without an OTel SDK:

* ``trace_id`` -- one per campaign. Derived DETERMINISTICALLY from the run's
  ``HpcRun.correlation_id`` (first 32 hex chars of its SHA-256), because that
  column is ``{sim_id}_{commit}_{random}``, not W3C hex, and because a
  deterministic derivation lets the API recompute it from the existing indexed
  column with no lookup table and no ordering constraint between "submit the
  job" and "insert the row" (the handler submits first).
* ``span_id`` of the campaign span -- likewise derived (``campaign:`` +
  correlation id), for the same reason.
* ``PBG_TRACEPARENT`` -- the full W3C ``traceparent`` form,
  ``00-<trace_id>-<campaign_span_id>-01``, so an OTel-aware consumer can read it
  unchanged; the engine's parser also accepts the short ``<trace>-<span>`` form.
* ``PBG_TRACE_BAGGAGE`` -- W3C ``baggage``-style ``key=value,key=value`` (NOT
  JSON: the Nextflow renderer emits every value as a docker ``--env K=V``
  argument, and :func:`viva_api.common.dispatch_validation.validate_task_env`
  documents why quotes, whitespace, ``$`` and backslashes cannot appear there).
  Values are percent-encoded outside ``[A-Za-z0-9._~-]``.
* ``PBG_EVENT_TAGS`` -- same format, opaque to the engine, copied verbatim into
  every event's ``tags`` (backend, seed, generation, ...). This is where AWS/K8s
  identifiers belong; the core event schema carries none.
* ``PBG_EVENT_SINKS`` -- ``stdout`` always (CloudWatch keeps it), plus the
  run's S3 events prefix when ``Settings.events_enabled`` and a work bucket are
  configured. The S3 sink itself is a v2ecoli plugin; the engine ships only
  stdout/file sinks.
* ``PBG_EVENT_FLUSH_S`` / ``PBG_EVENT_HEARTBEAT_S`` -- cadence knobs from settings.

All of it rides in the same ``task_env`` mechanism the request's own env uses
(``resolve_task_env`` -> ``task_env_as_batch_list`` / Nextflow ``container_env``),
merged UNDER the request's values so a caller can still override for a
diagnostic; ``PBG_`` is a reserved prefix at the API boundary so a request
cannot set them by accident.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import quote

__all__ = [
    "PBG_EVENT_FLUSH_S",
    "PBG_EVENT_HEARTBEAT_S",
    "PBG_EVENT_SINKS",
    "PBG_EVENT_TAGS",
    "PBG_TRACEPARENT",
    "PBG_TRACE_BAGGAGE",
    "baggage",
    "campaign_span_id",
    "events_env",
    "events_s3_prefix",
    "trace_id_from_correlation",
    "traceparent",
    "with_events_env",
]

PBG_TRACEPARENT = "PBG_TRACEPARENT"
PBG_TRACE_BAGGAGE = "PBG_TRACE_BAGGAGE"
PBG_EVENT_TAGS = "PBG_EVENT_TAGS"
PBG_EVENT_SINKS = "PBG_EVENT_SINKS"
PBG_EVENT_FLUSH_S = "PBG_EVENT_FLUSH_S"
PBG_EVENT_HEARTBEAT_S = "PBG_EVENT_HEARTBEAT_S"

_TRACE_ID_HEX = 32
_SPAN_ID_HEX = 16
_BAGGAGE_SAFE = "._~-"
_TRACEPARENT_RE = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$")


def trace_id_from_correlation(correlation_id: str) -> str:
    """The campaign's ``trace_id``: first 32 hex chars of SHA-256(correlation_id)."""
    return hashlib.sha256(correlation_id.encode("utf-8")).hexdigest()[:_TRACE_ID_HEX]


def campaign_span_id(correlation_id: str) -> str:
    """The campaign span's ``span_id``, derived so submit and insert need no handshake."""
    return hashlib.sha256(f"campaign:{correlation_id}".encode()).hexdigest()[:_SPAN_ID_HEX]


def traceparent(trace_id: str, span_id: str) -> str:
    """The W3C ``traceparent`` header form (version 00, sampled)."""
    return f"00-{trace_id}-{span_id}-01"


def parse_traceparent(value: str) -> tuple[str, str] | None:
    """``(trace_id, span_id)`` from a W3C traceparent, or ``None`` if malformed."""
    match = _TRACEPARENT_RE.match(value.strip().lower())
    if match is None:
        return None
    return match.group(1), match.group(2)


def baggage(fields: dict[str, Any]) -> str:
    """W3C ``baggage``-style ``key=value,key=value``; ``None`` values are skipped.

    Percent-encodes everything outside ``[A-Za-z0-9._~-]`` so the result is safe
    for a docker ``--env K=V`` argument and for ``validate_task_env``'s forbidden
    set (no whitespace, quotes, ``$`` or backslashes can survive the encoding).
    """
    parts = [f"{key}={quote(str(value), safe=_BAGGAGE_SAFE)}" for key, value in fields.items() if value is not None]
    return ",".join(parts)


def _str_setting(settings: Any, name: str) -> str | None:
    """A settings attribute only if it is a real string (tests use MagicMock settings)."""
    value = getattr(settings, name, None)
    return value if isinstance(value, str) else None


def _int_setting(settings: Any, name: str, default: int) -> int:
    value = getattr(settings, name, None)
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def events_s3_prefix(settings: Any, experiment_id: str) -> str | None:
    """Where a run's ``events.jsonl`` objects go, or ``None`` when S3 events are off.

    ``Settings.events_s3_prefix`` is a template with ``{experiment_id}``; empty means
    "derive from the work bucket": ``s3://<s3_work_bucket>/<s3_work_prefix>/<experiment_id>/events/``.
    No bucket configured means no S3 sink (stdout only) -- never a half-formed URI.
    """
    enabled = getattr(settings, "events_enabled", True)
    if enabled is False:
        return None
    template = _str_setting(settings, "events_s3_prefix")
    if template:
        return template.format(experiment_id=experiment_id).rstrip("/") + "/"
    bucket = _str_setting(settings, "s3_work_bucket")
    if not bucket:
        return None
    work_prefix = (_str_setting(settings, "s3_work_prefix") or "nextflow/work").strip("/")
    return f"s3://{bucket}/{work_prefix}/{experiment_id}/events/"


def events_env(
    *,
    correlation_id: str | None,
    experiment_id: str,
    backend: str,
    settings: Any,
    sim_id: int | str | None = None,
    tags: dict[str, Any] | None = None,
) -> dict[str, str]:
    """The ``PBG_*`` identity env for one dispatch. Empty when events are disabled.

    ``correlation_id`` may be ``None`` on service-level entry points reachable
    without the API handler; the run's ``experiment_id`` then seeds the trace so
    every task of that run still shares one id.
    """
    if getattr(settings, "events_enabled", True) is False:
        return {}
    seed = correlation_id or experiment_id
    trace_id = trace_id_from_correlation(seed)
    span_id = campaign_span_id(seed)
    sinks = ["stdout"]
    prefix = events_s3_prefix(settings, experiment_id)
    if prefix:
        sinks.append(prefix)
    env = {
        PBG_TRACEPARENT: traceparent(trace_id, span_id),
        PBG_TRACE_BAGGAGE: baggage({"sim_id": sim_id, "experiment_id": experiment_id}),
        PBG_EVENT_TAGS: baggage({"backend": backend, **(tags or {})}),
        PBG_EVENT_SINKS: ",".join(sinks),
        PBG_EVENT_FLUSH_S: str(_int_setting(settings, "events_flush_seconds", 60)),
        PBG_EVENT_HEARTBEAT_S: str(_int_setting(settings, "events_heartbeat_seconds", 30)),
    }
    return env


def with_events_env(task_env: dict[str, str] | None, **kwargs: Any) -> dict[str, str]:
    """``events_env(**kwargs)`` merged UNDER ``task_env`` (the request's values win)."""
    merged = events_env(**kwargs)
    merged.update(task_env or {})
    return merged
