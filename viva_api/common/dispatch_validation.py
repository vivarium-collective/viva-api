"""Request-shape checks for the dispatch payloads, done at the API boundary.

These are the checks answerable from the REQUEST ALONE -- no database, no
cluster, no settings. That is what lets them run before anything is written.

Why it matters that they run early (viva-api#455): ``run_simulation_workflow``
inserts a parca-dataset row and a simulation row *before* it submits, so a
dispatch-time raise leaves two rows describing a run that never started. A
malformed ``nextflow_dispatch`` is an ordinary typo, not an infrastructure
failure, so it must not cost durable state.

Server-side facts stay OUT of here. ``_awsbatch_nf_params``' missing-settings
check is a deployment error, not a caller error: it deserves its 500 and cannot
be answered from the request anyway.
"""

from __future__ import annotations

import re
from typing import Any


class DispatchValidationError(ValueError):
    """A dispatch request the caller can fix. Maps to 400, never 500.

    A subclass of ValueError so the service layer can keep raising and catching
    plain ValueError, while the router can tell "your request is wrong" apart
    from "something broke" -- a distinction a blanket ValueError -> 400 would
    lose, since not every ValueError raised down there is the caller's fault.
    """


def validate_nextflow_dispatch(nf_dispatch: Any) -> None:
    """Check a ``nextflow_dispatch`` block, or raise DispatchValidationError."""
    if not isinstance(nf_dispatch, dict):
        raise DispatchValidationError(f"nextflow_dispatch must be an object, got {type(nf_dispatch).__name__}")

    if not nf_dispatch.get("composite_id"):
        raise DispatchValidationError(
            "nextflow_dispatch.composite_id is required: the registered composite to compile "
            "into a Nextflow workflow (e.g. 'v2ecoli.composites.workflow_nf.workflow_nf' -- "
            "note the doubled tail; this generator has no bare-module alias)."
        )

    if nf_dispatch.get("task_env") is not None:
        validate_task_env(nf_dispatch["task_env"], where="nextflow_dispatch.task_env")

    if nf_dispatch.get("resume") and not nf_dispatch.get("resume_from"):
        raise DispatchValidationError(
            "nextflow_dispatch.resume needs resume_from: the experiment_id of the run whose "
            "work dir and session cache to continue. Each dispatch gets its own work dir "
            "(viva-api#452), so a resume without one would find no session -- and Nextflow "
            "does not treat that as an error, it warns and silently re-runs the whole "
            "campaign at full cost."
        )


# --- task_env: a per-dispatch environment passthrough for every simulation TASK ---
#
# Why (sms-ecoli#166, 2026-09-09): v2ecoli#758 changed a file in
# ``cache_version.INPUT_FILES`` and thereby re-keyed every ParCa cache in S3; the
# caches' biology was unchanged, so the team chose v2ecoli's documented escape
# hatch (``V2ECOLI_SKIP_CACHE_VERIFY=1``, ``v2ecoli/core.py``, warns once per
# process) over a rebuild cascade. Nothing in this API could set an env var on a
# Batch task from a request, so this is the hole it fills -- generically, not
# for that one variable.
#
# It rides in the request as ``task_env: {NAME: value}``, either at the top of the
# simulation config (``extra_params.task_env``) or inside a dispatch block
# (``nextflow_dispatch.task_env``, ``multi_node_dispatch.task_env``,
# ``mbp_dispatch.task_env``); a block's entries win over the top-level ones.

_TASK_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The Nextflow renderer refuses these in a value (they would end the Groovy
# string or split the docker `--env` argument); the same rule applies on every
# path so a request that works on one works on all.
_TASK_ENV_VALUE_FORBIDDEN = frozenset(" \t\n\"'$\\")
TASK_ENV_MAX_ENTRIES = 32

#: Exact names the service sets itself on at least one path; a request must not
#: be able to shadow them silently.
TASK_ENV_RESERVED_NAMES: frozenset[str] = frozenset({
    "PYTHONPATH",
    "V2E_ROOT",
    "V2E_REQUIRE_CLEAN_CHAIN",
    "LINEAGE_DEBUG_DIVISION",
    "PBG_RUNNER_ENV",
    "PATH",
    "HOME",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
})
#: Prefixes the service or the executors own outright (Batch/Ray/container
#: entrypoint contracts, AWS SDK, Nextflow).
# ``PBG_``: the service sets the process-bigraph identity/event env itself
# (``viva_api.common.events_env``: PBG_TRACEPARENT, PBG_TRACE_BAGGAGE,
# PBG_EVENT_SINKS, ...); a request must not shadow a run's identity.
TASK_ENV_RESERVED_PREFIXES: tuple[str, ...] = ("AWS_", "RAY_", "CONTAINER_", "NXF_", "NEXTFLOW_", "PBG_")


def validate_task_env(task_env: Any, *, where: str = "task_env") -> dict[str, str]:
    """Check a ``task_env`` mapping and return it as a plain ``dict[str, str]``.

    Rejects, never coerces: a non-object, a non-string value, a name that is not
    a valid environment-variable name, a value the Nextflow renderer could not
    emit, more than :data:`TASK_ENV_MAX_ENTRIES` entries, or a name the service
    sets itself (:data:`TASK_ENV_RESERVED_NAMES` / :data:`TASK_ENV_RESERVED_PREFIXES`).
    ``where`` names the request field in the message.
    """
    if task_env is None:
        return {}
    if not isinstance(task_env, dict):
        raise DispatchValidationError(f"{where} must be an object of NAME: value, got {type(task_env).__name__}")
    if len(task_env) > TASK_ENV_MAX_ENTRIES:
        raise DispatchValidationError(
            f"{where} has {len(task_env)} entries; at most {TASK_ENV_MAX_ENTRIES} are allowed"
        )
    out: dict[str, str] = {}
    for key, value in task_env.items():
        name = str(key)
        if not _TASK_ENV_NAME.match(name):
            raise DispatchValidationError(f"{where}: {name!r} is not a valid environment variable name")
        if name in TASK_ENV_RESERVED_NAMES or name.startswith(TASK_ENV_RESERVED_PREFIXES):
            raise DispatchValidationError(
                f"{where}: {name!r} is set by the service itself on this path and cannot be overridden "
                f"(reserved names: {', '.join(sorted(TASK_ENV_RESERVED_NAMES))}; reserved prefixes: "
                f"{', '.join(TASK_ENV_RESERVED_PREFIXES)})"
            )
        if not isinstance(value, str):
            raise DispatchValidationError(
                f"{where}[{name!r}] must be a string, got {type(value).__name__} -- "
                f'write the value as text (e.g. "1", not 1)'
            )
        bad = sorted(_TASK_ENV_VALUE_FORBIDDEN.intersection(value))
        if bad:
            raise DispatchValidationError(
                f"{where}[{name!r}] contains {''.join(bad)!r}; a value may not contain whitespace, "
                f"quotes, `$` or backslashes (it is rendered into a docker --env argument)"
            )
        out[name] = value
    return out


#: The dispatch blocks that may carry their own ``task_env``.
TASK_ENV_DISPATCH_BLOCKS: tuple[str, ...] = ("nextflow_dispatch", "multi_node_dispatch", "mbp_dispatch")


def validate_dispatch_task_envs(config_data: dict[str, Any]) -> None:
    """Boundary check for every place a request may carry ``task_env``."""
    validate_task_env(config_data.get("task_env"), where="task_env")
    for block in TASK_ENV_DISPATCH_BLOCKS:
        payload = config_data.get(block)
        if isinstance(payload, dict) and payload.get("task_env") is not None:
            validate_task_env(payload["task_env"], where=f"{block}.task_env")


def resolve_task_env(config: Any, dispatch: dict[str, Any] | None = None) -> dict[str, str]:
    """The env a dispatch's tasks get: the config's top-level ``task_env`` with
    the dispatch block's own ``task_env`` merged over it. Re-validated here so
    the service-level entry points (reachable without the API boundary) stay
    safe; an empty result is the byte-identical pre-feature behaviour."""
    merged: dict[str, str] = {}
    # The boundary (validate_dispatch_task_envs) is where a malformed top-level
    # value is refused with a 400, before anything durable is written. Down here
    # a non-mapping attribute means "not set": a pre-feature simulation row, or a
    # test double whose config auto-creates attributes.
    top = getattr(config, "task_env", None)
    merged.update(validate_task_env(top if isinstance(top, dict) else None, where="task_env"))
    if dispatch:
        merged.update(validate_task_env(dispatch.get("task_env"), where="dispatch.task_env"))
    return merged


def task_env_as_batch_list(task_env: dict[str, str] | None) -> list[dict[str, str]]:
    """``{NAME: value}`` -> Batch's ``[{"name": ..., "value": ...}]`` shape."""
    return [{"name": k, "value": v} for k, v in (task_env or {}).items()]
