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

from typing import Protocol

from viva_core.environments import DerivedSpec, ExplicitSpec, RegistryEnvironmentResolver, ecr_registry


class RegistrySettings(Protocol):
    """The three settings that say where this site's environments live."""

    @property
    def ecr_account_id(self) -> str: ...
    @property
    def batch_region(self) -> str: ...
    @property
    def ray_ecr_repository(self) -> str: ...


def _registry(settings: RegistrySettings) -> str:
    """The registry host. With ``ecr_account_id`` UNSET this is the malformed
    ``.dkr.ecr.<region>.amazonaws.com`` the four derivations produced -- preserved ON PURPOSE for
    now: P2.3b is a rewiring, proven by saying what they said for every input, and eight tests run
    with the account unset without ever looking at the image. Refusing here, by name, instead of at
    the Batch pull ten minutes later is a behaviour change, and its own PR (``docs/plan-core.md``,
    deferred list). The env-worker service already refuses, and still does.
    """
    if not settings.ecr_account_id:
        return f".dkr.ecr.{settings.batch_region}.amazonaws.com"
    return ecr_registry(account_id=settings.ecr_account_id, region=settings.batch_region)


def site_resolver(settings: RegistrySettings) -> RegistryEnvironmentResolver:
    """``CORE_RUNTIME_IMAGE``, when the site sets it, is registered as the environment for a composite
    that needs nothing beyond the built-ins. Read with ``getattr``: it is optional, and the settings
    handed in here are often a test double that names only what its test is about."""
    named = getattr(settings, "core_runtime_image", "")
    return RegistryEnvironmentResolver(
        registry=_registry(settings),
        repository=settings.ray_ecr_repository,
        # a str, and not empty: a MagicMock settings double answers every attribute with a Mock
        runtime_image=named if isinstance(named, str) and named else None,
    )


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
