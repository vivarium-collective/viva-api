"""Runtime capability advertisement, for clients that must feature-detect.

``GET /core/v1/capabilities`` returns ``{version, capabilities: [...]}`` where
``capabilities`` is a list of opaque, stable **strings**. Clients branch on
membership::

    if "container-jobs" in caps["capabilities"]:
        ...
    else:
        raise UserFacingError("this viva-api deployment doesn't support X yet")

WHY THIS EXISTS, AND WHY IT IS NOT A VERSION CHECK
--------------------------------------------------
``version`` is returned for humans, logs, and bug reports. It is deliberately
NOT the detection mechanism, and clients must never compare it (no
``>= "0.9.51"``). Two independent reasons, both observed in this project:

1. **A deployment can run code that is not on any released version.** On
   2026-08-19 production was running an image built from an unmerged branch,
   with a database schema ahead of ``main``. Any client reasoning "0.9.52 >
   0.9.50, therefore feature X is present" would have been guessing about a
   build whose contents no version ordering describes.
2. **Having the code is not the same as being able to use it.** Several
   capabilities here are gated on deployment configuration as well as on the
   code being present -- ``container-jobs`` needs its Batch queue and job
   definition configured, and a build carrying the code on an unconfigured
   deployment cannot honour the capability. Advertising it from the version
   number alone would be precisely the "assume, don't detect" failure this
   endpoint exists to prevent.

So every probe below answers one question: **can THIS running deployment, right
now, actually do this?** Code presence AND configuration AND the relevant
backend being wired.

CONTRACT FOR THE NAMES
----------------------
Capability names are a public API, additive and tolerant-reader:

* Names are stable. Once advertised, a name is never renamed or repurposed.
* The list is unordered by contract (this module sorts it for stable output and
  readable diffs); clients test membership, never position.
* A name absent from the list means "not available here" -- never "unknown". A
  client seeing an unfamiliar name ignores it.
* Removing a capability is a breaking change and needs the same care as removing
  an endpoint.

Adding one: append to ``CAPABILITY_REGISTRY`` with a probe that returns False
unless the deployment can genuinely serve it. A probe that raises is treated as
False (see ``detect_capabilities``) -- capability reporting is diagnostic and
must never be the thing that takes the endpoint down.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, Field

from viva_api.config import ComputeBackend, get_settings
from viva_api.version import __version__

logger = logging.getLogger(__name__)

# --- capability names (public API -- see the module docstring's contract) ---

#: Multi-seed x multi-generation chain-dispatch campaigns exist and can be
#: submitted (viva-api 0.9.42+, backlog item 33).
CAPABILITY_CHAIN_DISPATCH = "chain-dispatch"

#: ``GET /simulations/{id}/chain-progress`` serves real per-seed aggregate
#: progress (viva-api 0.9.49+, backlog item 6).
CAPABILITY_CHAIN_PROGRESS = "chain-progress"

#: Plain container-type AWS Batch job submission -- "image + command + env +
#: outputs-to-S3", no Ray and no MNP node overrides (viva-api PR #258, item 71).
#: This is what lets a non-Ray image (e.g. ``vecoli:<commit>-arm64``) run as a
#: single Batch job with one job id known at submission time.
CAPABILITY_CONTAINER_JOBS = "container-jobs"

#: Two-engine comparison dispatch: candidate + reference simulations submitted as
#: peers, plus a compare job wired to both via native Batch ``dependsOn``
#: (dual-engine-comparison spec, W4).
CAPABILITY_DUAL_ENGINE_COMPARISON = "dual-engine-comparison"


class ServerCapabilities(BaseModel):
    """What this running deployment can actually do."""

    version: str = Field(description="Server version, for humans and bug reports. NOT for feature detection.")
    capabilities: list[str] = Field(
        description=(
            "Stable capability names this deployment can serve right now. Test membership; "
            "absence means 'not available here'. Unknown names should be ignored."
        )
    )


def _ray_service() -> Any | None:
    """The Ray/Batch simulation service, or None when this deployment has none.

    Imported lazily: ``dependencies`` builds the service registry at startup and
    importing it at module load would invert the import order the routers rely
    on. Returns None on any failure -- a deployment without a Ray backend simply
    has none of the Batch-shaped capabilities.
    """
    try:
        from viva_api.dependencies import get_simulation_service_for_backend

        return get_simulation_service_for_backend(ComputeBackend.RAY)
    except Exception:
        return None


def _has_chain_dispatch() -> bool:
    return hasattr(_ray_service(), "submit_chain_dispatch_job")


def _has_chain_progress() -> bool:
    # The route's own handler, not the router table: this is the thing that
    # actually computes the answer, and it exists independently of mounting.
    return hasattr(_ray_service(), "get_chain_campaign_result")


def _has_container_jobs() -> bool:
    """Container-type Batch submission, gated on code AND configuration.

    BOTH halves are required. viva-api #258's ``_submit_container`` raises
    ``RuntimeError`` naming the unset setting when ``ray_container_queue`` /
    ``ray_container_job_definition`` are blank, so a build that carries the code
    onto an unconfigured deployment cannot serve this -- advertising it there
    would hand a client a capability that fails at dispatch.

    ``getattr(..., "")`` rather than attribute access: on a build predating #258
    the settings do not exist at all, and "setting absent" and "setting blank"
    mean the same thing here.
    """
    service = _ray_service()
    if service is None or not hasattr(service, "_submit_container"):
        return False
    settings = get_settings()
    return bool(getattr(settings, "ray_container_queue", "")) and bool(
        getattr(settings, "ray_container_job_definition", "")
    )


def _has_dual_engine_comparison() -> bool:
    """Two-engine comparison dispatch.

    Depends on ``container-jobs``, not merely alongside it: the reference engine
    (vEcoli) runs as a single container Batch job, which is what collapses it to
    one job id known at submission time so the compare job can be submitted
    upfront with ``depends_on=[candidate, reference]``. Without container jobs
    there is no reference job id to depend on.
    """
    service = _ray_service()
    if service is None or not hasattr(service, "submit_comparison_dispatch_job"):
        return False
    return _has_container_jobs()


#: (name, probe). Order here is irrelevant -- ``detect_capabilities`` sorts.
CAPABILITY_REGISTRY: list[tuple[str, Callable[[], bool]]] = [
    (CAPABILITY_CHAIN_DISPATCH, _has_chain_dispatch),
    (CAPABILITY_CHAIN_PROGRESS, _has_chain_progress),
    (CAPABILITY_CONTAINER_JOBS, _has_container_jobs),
    (CAPABILITY_DUAL_ENGINE_COMPARISON, _has_dual_engine_comparison),
]


def detect_capabilities() -> list[str]:
    """Probe every registered capability; return the supported names, sorted.

    A probe that raises is logged and treated as unsupported. Capability
    reporting is diagnostic -- a broken probe must degrade to "not advertised",
    never to a 500 that makes a feature-detecting client unable to detect
    anything at all.
    """
    supported: list[str] = []
    for name, probe in CAPABILITY_REGISTRY:
        try:
            if probe():
                supported.append(name)
        except Exception:
            logger.warning("Capability probe %r failed; reporting it as unsupported.", name, exc_info=True)
    return sorted(supported)


def get_server_capabilities() -> ServerCapabilities:
    return ServerCapabilities(version=__version__, capabilities=detect_capabilities())
