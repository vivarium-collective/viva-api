"""Environments: what a run needs, what exists, and how the first becomes the second (D10).

``model`` is the vocabulary; ``registry_resolver`` is the one resolver there is so far -- the
*select* half for a site whose environments are images in one registry repository.
"""

from viva_core.environments.model import (
    REPO_RECIPE,
    Dependency,
    DerivedSpec,
    Environment,
    EnvironmentNotResolvable,
    EnvironmentResolver,
    EnvironmentResolverNotConfigured,
    EnvironmentSpec,
    ExplicitSpec,
    spec_hash,
)
from viva_core.environments.registry_resolver import RegistryEnvironmentResolver, ecr_registry, image_reference

__all__ = [
    "REPO_RECIPE",
    "Dependency",
    "DerivedSpec",
    "Environment",
    "EnvironmentNotResolvable",
    "EnvironmentResolver",
    "EnvironmentResolverNotConfigured",
    "EnvironmentSpec",
    "ExplicitSpec",
    "RegistryEnvironmentResolver",
    "ecr_registry",
    "image_reference",
    "spec_hash",
]
