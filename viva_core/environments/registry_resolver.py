"""The *select* half of D10 for a site whose environments are images in ONE registry repository,
tagged by their key.

That is every environment this code base resolves today, and it was resolved four separate times
from the same two settings -- once per consumer, each spelling the registry host by hand
(``docs/architecture-core.md``, delta row 5). This is the one place that knows the shape of an
image reference. It is handed the registry coordinates; it reads no settings and makes no network
call, so *select* here means *say where the image for this key lives*, exactly as the four
derivations did. Whether the image is there is still found out by the pull -- knowing sooner
needs the ``environment`` table of P5.
"""

from dataclasses import dataclass

from viva_core.environments.model import (
    DerivedSpec,
    Environment,
    EnvironmentNotResolvable,
    EnvironmentSpec,
)


def ecr_registry(*, account_id: str, region: str) -> str:
    """``<account>.dkr.ecr.<region>.amazonaws.com`` -- the host of an AWS ECR registry."""
    if not account_id or not region:
        raise ValueError(f"an ECR registry needs an account id and a region; got {account_id!r}, {region!r}")
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com"


def image_reference(*, registry: str, repository: str, key: str, variant: str = "") -> str:
    """``<registry>/<repository>:<key>[-<variant>]``."""
    tag = f"{key}-{variant}" if variant else key
    return f"{registry}/{repository}:{tag}"


@dataclass(frozen=True, slots=True)
class RegistryEnvironmentResolver:
    """Explicit specs resolve to ``<registry>/<repository>:<key>[-<variant>]``.

    ``runtime_image`` is the environment for a composite that needs nothing beyond the built-ins
    (an EMPTY derived spec): the core runtime image, once a site has one. A derived spec that
    does name dependencies cannot be resolved here -- nothing is registered to select from and
    this resolver cannot build -- and is refused rather than answered with something close.
    """

    registry: str
    repository: str
    runtime_image: str | None = None

    def __post_init__(self) -> None:
        if not self.registry or not self.repository:
            raise ValueError(f"a registry resolver needs a registry and a repository; got {self!r}")

    def resolve(self, spec: EnvironmentSpec) -> Environment:
        if isinstance(spec, DerivedSpec):
            if spec.dependencies:
                needed = ", ".join(sorted(d.name for d in spec.dependencies))
                raise EnvironmentNotResolvable(
                    f"no environment is registered that provides {needed}, and this resolver cannot build one "
                    "(the build half of select-or-build is P5)"
                )
            if self.runtime_image is None:
                raise EnvironmentNotResolvable("this site has no runtime image for a composite that needs none")
            return Environment(spec=spec, image=self.runtime_image)
        image = image_reference(registry=self.registry, repository=self.repository, key=spec.key, variant=spec.variant)
        return Environment(spec=spec, image=image)
