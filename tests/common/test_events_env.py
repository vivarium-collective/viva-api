"""The PBG_* identity env every dispatched task gets (observability plan D4a)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from viva_api.common.dispatch_validation import DispatchValidationError, validate_task_env
from viva_api.common.events_env import (
    PBG_EVENT_SINKS,
    PBG_EVENT_TAGS,
    PBG_TRACE_BAGGAGE,
    PBG_TRACEPARENT,
    baggage,
    campaign_span_id,
    events_env,
    events_s3_prefix,
    parse_traceparent,
    trace_id_from_correlation,
    traceparent,
    with_events_env,
)


def _settings(**overrides: object) -> SimpleNamespace:
    base = {
        "events_enabled": True,
        "events_s3_prefix": "",
        "events_flush_seconds": 60,
        "events_heartbeat_seconds": 30,
        "s3_work_bucket": "mybucket",
        "s3_work_prefix": "nextflow/work",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_trace_id_is_deterministic_and_w3c_shaped() -> None:
    """The row is inserted AFTER the job is submitted, so the ids a task gets and
    the ids the row records must agree without a handshake: derived, not minted."""
    cid = "946_c233c7a_ab12cd3"
    assert trace_id_from_correlation(cid) == trace_id_from_correlation(cid)
    assert len(trace_id_from_correlation(cid)) == 32
    assert len(campaign_span_id(cid)) == 16
    assert trace_id_from_correlation(cid) != trace_id_from_correlation("other")
    assert parse_traceparent(traceparent(trace_id_from_correlation(cid), campaign_span_id(cid))) == (
        trace_id_from_correlation(cid),
        campaign_span_id(cid),
    )


def test_baggage_is_env_safe() -> None:
    """The Nextflow renderer emits every value as a docker ``--env K=V``; the
    forbidden set validate_task_env documents cannot survive the encoding."""
    encoded = baggage({"experiment_id": 'sim 1 "x" $y\\z', "sim_id": 7, "skip": None})
    assert " " not in encoded and '"' not in encoded and "$" not in encoded and "\\" not in encoded
    assert encoded.startswith("experiment_id=sim%201%20") and encoded.endswith(",sim_id=7")
    assert "skip" not in encoded


def test_events_env_carries_identity_sinks_and_cadence() -> None:
    env = events_env(
        correlation_id="946_c233c7a_ab12cd3",
        experiment_id="sim193-run3-pilot",
        sim_id=946,
        backend="nextflow",
        tags={"phase": "lineage"},
        settings=_settings(),
    )
    assert env[PBG_TRACEPARENT] == traceparent(
        trace_id_from_correlation("946_c233c7a_ab12cd3"), campaign_span_id("946_c233c7a_ab12cd3")
    )
    assert env[PBG_TRACE_BAGGAGE] == "sim_id=946,experiment_id=sim193-run3-pilot"
    assert env[PBG_EVENT_TAGS] == "backend=nextflow,phase=lineage"
    assert env[PBG_EVENT_SINKS] == "stdout,s3://mybucket/nextflow/work/sim193-run3-pilot/events/"
    assert env["PBG_EVENT_FLUSH_S"] == "60"
    assert env["PBG_EVENT_HEARTBEAT_S"] == "30"
    # Every value passes the same check a request's task_env must pass: these ride
    # the same --env rendering.
    for key, value in env.items():
        validate_task_env({key.replace("PBG_", "X_"): value})


def test_events_env_is_empty_when_disabled_and_stdout_only_without_a_bucket() -> None:
    assert (
        events_env(correlation_id="c", experiment_id="e", backend="chain", settings=_settings(events_enabled=False))
        == {}
    )
    env = events_env(correlation_id="c", experiment_id="e", backend="chain", settings=_settings(s3_work_bucket=""))
    assert env[PBG_EVENT_SINKS] == "stdout"


def test_events_s3_prefix_template_wins_over_the_derived_default() -> None:
    settings = _settings(events_s3_prefix="s3://other/events/{experiment_id}")
    assert events_s3_prefix(settings, "exp1") == "s3://other/events/exp1/"


def test_a_missing_correlation_id_still_gives_the_run_one_trace() -> None:
    """Service-level entry points may run without the handler's correlation id."""
    a = events_env(correlation_id=None, experiment_id="exp1", backend="mnp", settings=_settings())
    b = events_env(correlation_id=None, experiment_id="exp1", backend="mnp", settings=_settings())
    assert a[PBG_TRACEPARENT] == b[PBG_TRACEPARENT]


def test_with_events_env_puts_the_request_under_its_own_values() -> None:
    merged = with_events_env(
        {"V2ECOLI_SKIP_CACHE_VERIFY": "1"},
        correlation_id="c",
        experiment_id="e",
        backend="nextflow",
        settings=_settings(),
    )
    assert merged["V2ECOLI_SKIP_CACHE_VERIFY"] == "1"
    assert PBG_TRACEPARENT in merged


def test_settings_doubles_without_real_strings_fall_back_safely() -> None:
    """Tests build settings as MagicMocks; a non-string attribute must not leak
    into a URI or a cadence value."""
    from unittest.mock import MagicMock

    env = events_env(correlation_id="c", experiment_id="e", backend="nextflow", settings=MagicMock())
    assert env[PBG_EVENT_SINKS] == "stdout"
    assert env["PBG_EVENT_FLUSH_S"] == "60"


def test_a_request_cannot_set_pbg_env_itself() -> None:
    with pytest.raises(DispatchValidationError, match="PBG_"):
        validate_task_env({"PBG_TRACEPARENT": "00-abc-def-01"})
