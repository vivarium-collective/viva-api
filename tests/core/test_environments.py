"""The environment model and the one resolver (``docs/plan-core.md`` P2.3, decision D10).

Core-only: nothing here imports ``viva_api``. The last test is the reason the resolver exists --
it must say, for the same inputs, exactly what each of the four hand-rolled derivations said.
"""

import dataclasses

import pytest

from viva_core.backends.batch import ecr_image_uri
from viva_core.environments import (
    Dependency,
    DerivedSpec,
    Environment,
    EnvironmentNotResolvable,
    EnvironmentResolver,
    ExplicitSpec,
    RegistryEnvironmentResolver,
    ecr_registry,
    image_reference,
    spec_hash,
)

REGISTRY = "123456789012.dkr.ecr.us-gov-west-1.amazonaws.com"


def _resolver(**overrides: str | None) -> RegistryEnvironmentResolver:
    return RegistryEnvironmentResolver(**{"registry": REGISTRY, "repository": "sim", **overrides})  # type: ignore[arg-type]


# ------------------------------------------------------------------ what is needed


def test_an_explicit_spec_needs_a_key_and_a_variant_is_one_suffix() -> None:
    for bad in ("", " abc", "abc "):
        with pytest.raises(ValueError, match="needs a key"):
            ExplicitSpec(key=bad)
    for bad in ("a:b", "a/b", "a b"):
        with pytest.raises(ValueError, match="one tag suffix"):
            ExplicitSpec(key="abc1234", variant=bad)


def test_specs_are_values() -> None:
    spec = ExplicitSpec(key="abc1234", repo_url="https://example.org/r", commit="abc1234")
    assert spec == ExplicitSpec(key="abc1234", repo_url="https://example.org/r", commit="abc1234")
    assert len({spec, dataclasses.replace(spec)}) == 1  # hashable: usable as a cache key
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.key = "other"  # type: ignore[misc]


def test_the_spec_hash_is_the_identity_of_the_request_not_of_the_image() -> None:
    authoritative = ExplicitSpec(key="abc1234", repo_url="https://example.org/r", commit="abc1234")
    # D11: a marked-temporary build of the same commit is a DIFFERENT environment (its own tag)...
    temporary = dataclasses.replace(authoritative, key="tmp-abc1234-0a1b2c")
    assert spec_hash(temporary) == spec_hash(authoritative)  # ...answering the SAME request
    assert _resolver().resolve(temporary).image != _resolver().resolve(authoritative).image

    # what does change the request: the commit, the repository, the recipe, the variant
    for changed in (
        dataclasses.replace(authoritative, commit="def5678"),
        dataclasses.replace(authoritative, repo_url="https://example.org/other"),
        dataclasses.replace(authoritative, recipe="python-deps"),
        dataclasses.replace(authoritative, variant="submit"),
    ):
        assert spec_hash(changed) != spec_hash(authoritative)

    # a caller that holds only a key: two different keys are two different requests
    assert spec_hash(ExplicitSpec(key="abc1234")) != spec_hash(ExplicitSpec(key="def5678"))


def test_a_derived_spec_hashes_its_dependencies_as_a_set() -> None:
    a, b = Dependency("copasi-basico", version="0.80"), Dependency("tellurium")
    assert spec_hash(DerivedSpec((a, b))) == spec_hash(DerivedSpec((b, a, a)))
    assert spec_hash(DerivedSpec((a,))) != spec_hash(DerivedSpec((a, b)))
    assert spec_hash(DerivedSpec((a,))) != spec_hash(DerivedSpec((Dependency("copasi-basico", version="0.81"),)))
    assert spec_hash(DerivedSpec()) != spec_hash(ExplicitSpec(key="abc1234"))
    assert len(spec_hash(DerivedSpec())) == 64


# ------------------------------------------------------------------ what exists


def test_an_environment_carries_both_identities() -> None:
    spec = ExplicitSpec(key="abc1234")
    found = _resolver().resolve(spec)
    assert found == Environment(spec=spec, image=f"{REGISTRY}/sim:abc1234")
    assert found.spec_hash == spec_hash(spec)
    assert found.image_digest is None  # nothing records digests yet: a resolver only says WHERE


def test_the_resolver_is_an_environment_resolver() -> None:
    resolver: EnvironmentResolver = _resolver()  # the assignment is the assertion (mypy checks the tests)
    assert resolver.resolve(ExplicitSpec(key="k")).image.endswith(":k")


# ------------------------------------------------------------------ select, never something close


def test_a_derived_spec_is_refused_rather_than_answered_with_something_close() -> None:
    wants = DerivedSpec((Dependency("tellurium"), Dependency("copasi-basico")))
    with pytest.raises(EnvironmentNotResolvable, match="copasi-basico, tellurium"):
        _resolver(runtime_image="ghcr.io/x/core-runtime:1").resolve(wants)


def test_a_composite_that_needs_nothing_gets_the_runtime_image_where_there_is_one() -> None:
    with pytest.raises(EnvironmentNotResolvable, match="no runtime image"):
        _resolver().resolve(DerivedSpec())
    found = _resolver(runtime_image="ghcr.io/x/core-runtime:1").resolve(DerivedSpec())
    assert found.image == "ghcr.io/x/core-runtime:1"


def test_a_resolver_without_coordinates_is_refused_when_it_is_made_not_when_it_is_used() -> None:
    for missing in ({"registry": ""}, {"repository": ""}):
        with pytest.raises(ValueError, match="needs a registry and a repository"):
            _resolver(**missing)
    with pytest.raises(ValueError, match="account id and a region"):
        ecr_registry(account_id="", region="us-gov-west-1")


# ------------------------------------------------------------------ the four derivations it replaces


@pytest.mark.parametrize("key", ["abc1234", "tmp-abc1234-0a1b2c", "d67b0a7"])
def test_it_says_what_each_hand_rolled_derivation_said(key: str) -> None:
    """``docs/architecture-core.md`` delta row 5: the same image, derived four times from the same two
    settings. The expected strings below are those four, spelled the way each spelled them."""
    account, region, repository = "123456789012", "us-gov-west-1", "sim"
    resolver = RegistryEnvironmentResolver(
        registry=ecr_registry(account_id=account, region=region), repository=repository
    )

    # the Batch layer's image_uri (through core's ecr_image_uri)
    assert resolver.resolve(ExplicitSpec(key=key)).image == ecr_image_uri(
        account_id=account, region=region, repository=repository, tag=key
    )
    # compose's site-pinned tag, the env worker's image_for_commit, the K8s analysis Job: an f-string each
    by_hand = f"{account}.dkr.ecr.{region}.amazonaws.com/{repository}:{key}"
    assert resolver.resolve(ExplicitSpec(key=key)).image == by_hand
    # the Batch layer's submit_image_uri: the same environment's second image
    assert resolver.resolve(ExplicitSpec(key=key, variant="submit")).image == f"{by_hand}-submit"
    assert (
        image_reference(registry=f"{account}.dkr.ecr.{region}.amazonaws.com", repository=repository, key=key) == by_hand
    )


# ------------------------------------------------------------------ the site resolver (U2d)


def test_a_site_names_its_registry_by_ecr_account_or_in_full_and_is_refused_by_name_with_neither() -> None:
    """``site_resolver``: an AWS site names an ECR account and region; an on-premises site names its
    registry in full (``ghcr.io/…``); a site that names neither is refused -- by name, and only for
    the requests that need a registry."""
    from types import SimpleNamespace

    from viva_core.environments import EnvironmentResolverNotConfigured
    from viva_core.environments.site import environment_image, site_resolver

    aws = SimpleNamespace(ecr_account_id="123456789012", batch_region="us-gov-west-1", ray_ecr_repository="sim")
    assert environment_image(aws, "abc1234") == "123456789012.dkr.ecr.us-gov-west-1.amazonaws.com/sim:abc1234"

    on_prem = SimpleNamespace(
        ecr_account_id="",
        batch_region="",
        ray_ecr_repository="",
        environment_registry="ghcr.io/vivarium-collective",
        environment_repository="viva-core",
        core_runtime_image="ghcr.io/vivarium-collective/viva-core-runtime:0.1.0",
    )
    assert (
        environment_image(on_prem, "abc1234", variant="submit")
        == "ghcr.io/vivarium-collective/viva-core:abc1234-submit"
    )
    assert site_resolver(on_prem).resolve(DerivedSpec()).image == "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"

    neither = SimpleNamespace(ecr_account_id="", batch_region="", ray_ecr_repository="", core_runtime_image="r:1")
    with pytest.raises(EnvironmentResolverNotConfigured, match="ECR_ACCOUNT_ID.*ENVIRONMENT_REGISTRY"):
        environment_image(neither, "abc1234")
    assert site_resolver(neither).resolve(DerivedSpec()).image == "r:1"  # what needs no registry still answers
