"""One place turns this site's settings into an image reference (``docs/plan-core.md`` P2.3b).

Four did, each spelling the registry host by hand. The differential that proved the rewiring is in
the PR; what stays here is the behaviour, and a guard so a fifth derivation cannot appear quietly.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from viva_api.common.site_environments import environment_image, named_environment_image, site_resolver
from viva_core.environments import (
    DerivedSpec,
    EnvironmentNotResolvable,
    EnvironmentResolverNotConfigured,
    ExplicitSpec,
)


def _settings(**overrides: str) -> SimpleNamespace:
    return SimpleNamespace(**{
        "ecr_account_id": "476270107793",
        "batch_region": "us-gov-west-1",
        "ray_ecr_repository": "v2ecoli",
        **overrides,
    })


def test_an_environment_is_named_by_its_key_in_this_sites_one_repository() -> None:
    host = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com"
    assert environment_image(_settings(), "d67b0a7") == f"{host}/v2ecoli:d67b0a7"
    # a marked-temporary simulator is keyed by its own tag (D11), never by its commit
    assert environment_image(_settings(), "tmp-d67b0a7-0a1b2c") == f"{host}/v2ecoli:tmp-d67b0a7-0a1b2c"
    # the workflow head is the same environment's second image
    assert environment_image(_settings(), "d67b0a7", variant="submit") == f"{host}/v2ecoli:d67b0a7-submit"
    assert environment_image(_settings(batch_region="us-east-1", ray_ecr_repository="other"), "k") == (
        "476270107793.dkr.ecr.us-east-1.amazonaws.com/other:k"
    )


def test_the_site_resolver_is_cores_resolver_and_selects_nothing_it_was_not_asked_for() -> None:
    resolver = site_resolver(_settings())
    assert resolver.resolve(ExplicitSpec(key="d67b0a7")).image == environment_image(_settings(), "d67b0a7")
    with pytest.raises(EnvironmentNotResolvable):  # this site registers no runtime image...
        resolver.resolve(DerivedSpec())


def test_a_site_that_names_a_runtime_image_runs_a_composite_that_needs_nothing_in_it() -> None:
    image = "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"
    resolver = site_resolver(_settings(core_runtime_image=image))
    assert resolver.resolve(DerivedSpec()).image == image
    # ...and naming one changes nothing about where an explicit environment lives
    assert resolver.resolve(ExplicitSpec(key="d67b0a7")).image == environment_image(_settings(), "d67b0a7")


def test_the_real_settings_carry_the_runtime_image_and_default_to_none() -> None:
    from viva_api.config import get_settings
    from viva_core.settings import CoreSettings

    assert CoreSettings().core_runtime_image == ""
    # with no image named, a composite that needs nothing is refused -- never run somewhere else
    if not get_settings().core_runtime_image:
        with pytest.raises(EnvironmentNotResolvable):
            site_resolver(get_settings()).resolve(DerivedSpec())


def test_an_unset_account_is_refused_by_name_for_the_one_request_that_needs_it() -> None:
    """Until 2026-09-21 this produced ``.dkr.ecr.<region>.amazonaws.com/<repo>:<key>``: a malformed
    name that became a job definition, a submitted job, and ten minutes later a Batch image-pull
    failure that never mentions the setting. Now the site says what is missing, at once."""
    unset = _settings(ecr_account_id="")
    with pytest.raises(EnvironmentResolverNotConfigured, match=r"ecr_account_id is unset \(ECR_ACCOUNT_ID\)"):
        environment_image(unset, "d67b0a7")
    with pytest.raises(EnvironmentResolverNotConfigured):
        environment_image(unset, "d67b0a7", variant="submit")


def test_an_unset_account_refuses_nothing_that_does_not_need_the_registry() -> None:
    """Building the resolver never fails (core's health route builds one per request), and the
    runtime image is a full reference of its own: a composite that needs nothing still gets it."""
    image = "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"
    resolver = site_resolver(_settings(ecr_account_id="", core_runtime_image=image))
    assert resolver.resolve(DerivedSpec()).image == image
    assert named_environment_image(_settings(ecr_account_id="", core_runtime_image=image), "runtime") == image
    with pytest.raises(EnvironmentNotResolvable):  # unset account AND no runtime image: still the ordinary refusal
        site_resolver(_settings(ecr_account_id="")).resolve(DerivedSpec())


def test_an_unset_repository_is_refused() -> None:
    """The one input for which this rewiring does NOT say what the derivations said: they produced
    ``<host>/:<key>``. No configuration or test sets an empty repository; refusing it is free."""
    with pytest.raises(ValueError, match="needs a registry and a repository"):
        environment_image(_settings(ray_ecr_repository=""), "d67b0a7")


#: Still spelled by hand, knowingly: the UPSTREAM vEcoli path of ``SimulationServiceK8s`` names a
#: different repository and an ``-amd64-submit`` image. That service is out of the carve until P5
#: (``docs/plan-core.md``); this number may only shrink.
HAND_ROLLED_ELSEWHERE = {"viva_api/simulation/simulation_service_k8s.py": 2}


def test_nothing_else_spells_a_registry_host_by_hand() -> None:
    found: dict[str, int] = {}
    for path in sorted(Path("viva_api").rglob("*.py")):
        if path.parts[:3] == ("viva_api", "api", "client") or path.name == "site_environments.py":
            continue
        count = path.read_text(encoding="utf-8").count(".dkr.ecr.")
        if count:
            found[path.as_posix()] = count
    assert found == HAND_ROLLED_ELSEWHERE, (
        "an image reference is being derived by hand; ask viva_api.common.site_environments instead "
        f"(or, if one was removed, lower HAND_ROLLED_ELSEWHERE): {found}"
    )
