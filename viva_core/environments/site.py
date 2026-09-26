"""This site's environments: the ONE place that turns the deployment's settings into an
``EnvironmentResolver`` (``docs/plan-core.md`` P2.3b).

Every environment this service runs is an image in one ECR repository, tagged by its environment
key -- a commit, or a marked-temporary simulator's own tag (D11). That image reference used to be
derived four separate times from the same three settings (the Batch layer, compose, the env-worker
service, the K8s analysis Job), each spelling the registry host by hand. They ask here now.

The settings are HANDED IN rather than read: each caller already holds them, through its own seam
(``dispatch._seams.get_settings`` in the dispatch package, ``config.get_settings`` elsewhere), and
a test that patches that seam must be what this sees.
"""

import re
from dataclasses import dataclass
from typing import Protocol

from viva_core.environments import (
    DerivedSpec,
    Environment,
    EnvironmentResolver,
    EnvironmentResolverNotConfigured,
    EnvironmentSpec,
    ExplicitSpec,
    RegistryEnvironmentResolver,
    ecr_registry,
)


class RegistrySettings(Protocol):
    """The settings that say where this site's environments live: an ECR account and region (an AWS
    site), or any registry named in full (``environment_registry`` + ``environment_repository``:
    ``ghcr.io/vivarium-collective`` + ``viva-core`` on an on-premises site, U2d)."""

    @property
    def ecr_account_id(self) -> str: ...
    @property
    def batch_region(self) -> str: ...
    @property
    def ray_ecr_repository(self) -> str: ...


#: Named in the refusal, so whoever reads it knows what to set -- either way.
ACCOUNT_UNSET = (
    "ecr_account_id is unset (ECR_ACCOUNT_ID) and no registry is named (ENVIRONMENT_REGISTRY + "
    "ENVIRONMENT_REPOSITORY): cannot say where this site's environment images live"
)


def _named(settings: object, name: str) -> str:
    """An optional str setting, read with ``getattr``: the settings handed in are often a test double
    that names only what its test is about, and a MagicMock answers every attribute with a Mock."""
    value = getattr(settings, name, "")
    return value if isinstance(value, str) else ""


@dataclass(frozen=True, slots=True)
class _RegistryNotConfigured:
    """The site's resolver when ``ECR_ACCOUNT_ID`` is unset: it REFUSES, by name, the one kind of
    request that needs the registry -- an explicit spec, whose image lives in the site's ECR
    repository. Until 2026-09-21 that produced ``.dkr.ecr.<region>.amazonaws.com/<repo>:<key>``: a
    malformed name nobody looked at, which became a job definition, then a submitted job, then a
    Batch image-pull failure ten minutes later that never mentions the setting.

    What does NOT need the registry still works: a composite that needs nothing resolves to the
    runtime image (a full reference of its own), and building the resolver never fails -- core's
    health route, and every request that asks it nothing, are unaffected.
    """

    runtime_image: str | None

    def resolve(self, spec: EnvironmentSpec) -> Environment:
        if isinstance(spec, ExplicitSpec):
            raise EnvironmentResolverNotConfigured(ACCOUNT_UNSET)
        inner = RegistryEnvironmentResolver(
            registry="unconfigured.invalid", repository="-", runtime_image=self.runtime_image
        )
        return inner.resolve(spec)


def site_resolver(settings: RegistrySettings) -> EnvironmentResolver:
    """``CORE_RUNTIME_IMAGE``, when the site sets it, is registered as the environment for a composite
    that needs nothing beyond the built-ins. Read with ``getattr``: it is optional, and the settings
    handed in here are often a test double that names only what its test is about."""
    runtime_image = _named(settings, "core_runtime_image") or None
    if settings.ecr_account_id:
        return RegistryEnvironmentResolver(
            registry=ecr_registry(account_id=settings.ecr_account_id, region=settings.batch_region),
            repository=settings.ray_ecr_repository,
            runtime_image=runtime_image,
        )
    # No ECR account: a site that names its registry in full (a standalone core on RKE2 pulling
    # from ghcr, U2d) resolves the same way; a site that names neither is refused by name.
    registry, repository = _named(settings, "environment_registry"), _named(settings, "environment_repository")
    if registry and repository:
        return RegistryEnvironmentResolver(registry=registry, repository=repository, runtime_image=runtime_image)
    return _RegistryNotConfigured(runtime_image=runtime_image)


def environment_image(settings: RegistrySettings, key: str, *, variant: str = "") -> str:
    """The image of the environment named ``key`` (optionally its ``variant`` image) at this site."""
    return site_resolver(settings).resolve(ExplicitSpec(key=key, variant=variant)).image


#: The environments a request may ask for BY NAME -- registered ones, as opposed to the image of a
#: commit. One today: the core runtime image, for work that needs nothing of any application.
#: Curated environments (D10) are further names here, not a new field on every request.
RUNTIME_ENVIRONMENT = "runtime"
NAMED_ENVIRONMENTS = frozenset({RUNTIME_ENVIRONMENT})


def named_environment_image(settings: RegistrySettings, name: str) -> str:
    """The image of the registered environment ``name`` at this site.

    Raises ``EnvironmentNotResolvable`` when the site registers none (``CORE_RUNTIME_IMAGE`` unset):
    the request is refused rather than run in something else.
    """
    if name not in NAMED_ENVIRONMENTS:
        raise ValueError(f"unknown environment {name!r}; known: {sorted(NAMED_ENVIRONMENTS)}")
    return site_resolver(settings).resolve(DerivedSpec()).image


def job_definition_key(image: str) -> str:
    """A job-definition-safe key for an image that is NOT named by a commit: its repository and tag
    (``viva-core-runtime-0-1-0``). Batch derives one job definition per image, named ``<base>-<key>``;
    job definition names allow ``[A-Za-z0-9_-]``."""
    return re.sub(r"[^A-Za-z0-9_-]+", "-", image.rsplit("/", 1)[-1]).strip("-")[:64]
