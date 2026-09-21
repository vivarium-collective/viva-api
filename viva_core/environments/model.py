"""What an environment is: what is NEEDED, what EXISTS, and the one question that joins them.

Decision D10 (``docs/plan-core.md``): core's default path is *select or build an acceptable
environment, then run*. This module is the vocabulary for that sentence, and nothing else -- it
reads no settings, touches no registry, holds no table.

* An :class:`EnvironmentSpec` says what is **needed**. Either *explicit* -- this repository, at
  this commit, built by this recipe -- or *derived*: a set of dependencies read from a
  composite, to be satisfied by whatever environment can.
* An :class:`Environment` says what **exists**: an image a run can pull. It has two identities
  because they are two things -- the **spec hash** (what was asked for) and the **image digest**
  (what was built). A rebuild answers the same request with a different image: same spec hash,
  new digest, and by decision D11 a new tag, because a built environment is write-once.
* An :class:`EnvironmentResolver` turns the first into the second.

P2.3 is the *select* half: there is no table of environments yet and nothing here can build
one, so the fields that only a record can know -- status, the build job, what an environment
``provides`` -- are not declared until P5 gives them somewhere to live.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol

#: The recipe that clones a repository at a commit and runs its own build script. The only one
#: today; P5 adds ``python-deps`` and a registry of recipes.
REPO_RECIPE = "repo-recipe"


@dataclass(frozen=True, slots=True)
class ExplicitSpec:
    """This environment, named exactly.

    ``key`` is what names the environment in a registry: the commit for an authoritative build,
    or its own marked tag for a temporary one (D11) -- the two are different environments that
    answer the same request, which is why ``key`` is NOT part of :func:`spec_hash` and
    ``commit`` is. A caller that holds only the key (an env worker is handed a tag and nothing
    else) leaves ``repo_url`` and ``commit`` empty: it can be resolved, it cannot be rebuilt.

    ``variant`` names a second image of the SAME environment -- one built beside the first from
    the same source, for a different role (a workflow head that needs a JVM the tasks do not).
    """

    key: str
    repo_url: str = ""
    commit: str = ""
    recipe: str = REPO_RECIPE
    variant: str = ""

    def __post_init__(self) -> None:
        if not self.key or self.key != self.key.strip():
            raise ValueError(f"an explicit environment needs a key (a commit, or its marked tag); got {self.key!r}")
        if any(c in self.variant for c in ":/ "):
            raise ValueError(f"a variant is one tag suffix, not a path or a tag; got {self.variant!r}")


@dataclass(frozen=True, slots=True)
class Dependency:
    """One thing a composite needs installed: a package, where it comes from, and which version."""

    name: str
    source: str = "pypi"
    version: str = ""


@dataclass(frozen=True, slots=True)
class DerivedSpec:
    """Whatever satisfies these dependencies. An EMPTY set is a real request, not a missing one:
    a composite that needs nothing beyond the built-ins -- the core runtime image's case."""

    dependencies: tuple[Dependency, ...] = ()


EnvironmentSpec = ExplicitSpec | DerivedSpec


def spec_hash(spec: EnvironmentSpec) -> str:
    """The identity of a REQUEST: equal for two specs that ask for the same thing.

    Not the identity of an image -- a recipe may upgrade what it installs, so the same request
    built twice gives two images. That second identity is :attr:`Environment.image_digest`.
    For an explicit spec the registry ``key`` is left out on purpose (see :class:`ExplicitSpec`);
    a derived spec hashes its dependencies as a SET, so the order they were read in is not part
    of the question.
    """
    if isinstance(spec, ExplicitSpec):
        asked: dict[str, object] = {
            "kind": "explicit",
            "recipe": spec.recipe,
            "repo_url": spec.repo_url,
            "commit": spec.commit or spec.key,
            "variant": spec.variant,
        }
    else:
        asked = {
            "kind": "derived",
            "dependencies": sorted([d.source, d.name, d.version] for d in set(spec.dependencies)),
        }
    return hashlib.sha256(json.dumps(asked, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Environment:
    """An environment that exists: an image a run can pull, and the request it answers.

    ``image_digest`` is ``None`` until something records it; a resolver that only computes where
    an image lives (every one, before P5) does not know it.
    """

    spec: EnvironmentSpec
    image: str
    image_digest: str | None = None

    @property
    def spec_hash(self) -> str:
        return spec_hash(self.spec)


class EnvironmentNotResolvable(LookupError):
    """No environment answers this spec, and this resolver cannot make one."""


class EnvironmentResolver(Protocol):
    """Select an environment that satisfies the spec -- and, from P5, build one when none does.

    Raises :class:`EnvironmentNotResolvable` rather than returning something close: running
    under different dependencies than were asked for is the failure this whole model exists
    to make impossible.
    """

    def resolve(self, spec: EnvironmentSpec) -> Environment: ...
