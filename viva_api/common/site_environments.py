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

from viva_core.environments import ExplicitSpec, RegistryEnvironmentResolver, ecr_registry


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
    return RegistryEnvironmentResolver(
        registry=_registry(settings),
        repository=settings.ray_ecr_repository,
        runtime_image=getattr(settings, "core_runtime_image", "") or None,
    )


def environment_image(settings: RegistrySettings, key: str, *, variant: str = "") -> str:
    """The image of the environment named ``key`` (optionally its ``variant`` image) at this site."""
    return site_resolver(settings).resolve(ExplicitSpec(key=key, variant=variant)).image
