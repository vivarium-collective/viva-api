"""What core's routes accept and return. Pydantic, because FastAPI speaks it; the domain objects
(``viva_core.environments``) stay plain dataclasses and are translated at this edge."""

from typing import Literal

from pydantic import BaseModel, Field


class DependencyModel(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    name: str
    source: str = "pypi"
    version: str = ""


class ResolveEnvironmentRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """What is needed. ``explicit``: the environment named ``key`` (a commit, or its own marked tag),
    optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an EMPTY list
    is a real request, for a composite that needs only the built-ins."""

    kind: Literal["explicit", "derived"]
    key: str | None = None
    variant: str = ""
    dependencies: list[DependencyModel] = Field(default_factory=list)


class EnvironmentModel(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """What exists: the image a run would pull, and the two identities -- the request it answers
    (``spec_hash``) and what was built (``image_digest``; null until something records it)."""

    image: str
    spec_hash: str
    image_digest: str | None = None


class CoreHealth(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    status: Literal["ok"] = "ok"
    #: Core's own version (``viva_core.version``), for humans and bug reports -- not for feature detection.
    version: str
    #: Which core services this deployment provides, by name.
    services: dict[str, bool]


class CoreCapabilities(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """What this running deployment can serve. Clients test ``capabilities`` for membership; ``version``
    is core's own, for humans (``viva_core.api.capabilities`` has the contract)."""

    version: str
    capabilities: list[str] = Field(
        description="Stable capability names this deployment serves right now. Test membership; "
        "absence means 'not available here'. Unknown names should be ignored."
    )
