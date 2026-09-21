"""The Ray service reaches settings and AWS through ONE seam, and only through it.

``viva_api/simulation/dispatch/_seams.py`` explains why: the tests patch ``get_settings`` by
NAME, and a name patch is positional -- it reaches only the module it names. When the
4,305-line ``SimulationServiceRay`` is carved into several modules (``docs/plan-core.md``
P2.1), a module that imported ``get_settings`` directly would silently stop being patched
and run with the developer's real settings. These tests are the ratchet that keeps the carve
honest: they fail the moment a Ray-service module binds either name itself.
"""

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

import viva_api.simulation.dispatch as ray_package
from viva_api.simulation import simulation_service_ray

SEAM_NAMES = {"boto3", "get_settings"}
RAY_PACKAGE_DIR = Path(ray_package.__file__).resolve().parent
TESTS_DIR = Path(__file__).resolve().parents[1]


def _ray_service_modules() -> list[Path]:
    """``simulation_service_ray.py`` and everything in ``simulation/dispatch/`` except the seam."""
    carved = [p for p in sorted(RAY_PACKAGE_DIR.rglob("*.py")) if p.name != "_seams.py"]
    return [Path(simulation_service_ray.__file__).resolve(), *carved]


def _names_bound_by_imports(tree: ast.Module) -> set[str]:
    bound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            bound |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            bound |= {alias.asname or alias.name for alias in node.names}
    return bound


@pytest.mark.parametrize("module_path", _ray_service_modules(), ids=lambda p: p.name)
def test_no_ray_service_module_binds_a_seam_name_itself(module_path: Path) -> None:
    """Not ``import boto3``, not ``from viva_api.config import get_settings``, and not
    ``from ..._seams import get_settings`` either -- that last one LOOKS like using the seam
    and defeats it, because it copies the name into this module at import time."""
    bound = _names_bound_by_imports(ast.parse(module_path.read_text(encoding="utf-8")))
    offenders = sorted(bound & SEAM_NAMES)
    assert not offenders, (
        f"{module_path.name} imports {offenders} directly. Read them through the seam at call time "
        "(`_seams.get_settings()`, `_seams.boto3.client(...)`) so the tests' patches keep reaching this code."
    )


def test_the_service_module_no_longer_carries_the_names() -> None:
    for name in SEAM_NAMES:
        assert not hasattr(simulation_service_ray, name), f"simulation_service_ray.{name} is bound again"


def test_no_test_patches_the_old_location() -> None:
    """A patch aimed at ``simulation_service_ray.get_settings`` now either errors or --
    worse, if someone re-adds the import to make it pass -- patches a name nothing reads."""
    old = "viva_api.simulation.simulation_service_ray."
    this_file = Path(__file__).resolve()
    offenders = [
        f"{path.relative_to(TESTS_DIR)}:{number}"
        for path in sorted(TESTS_DIR.rglob("*.py"))
        if path.resolve() != this_file
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if any(f"{old}{name}" in line for name in SEAM_NAMES) and ("patch" in line or "setattr" in line)
    ]
    assert not offenders, "patch the seam (viva_api.simulation.dispatch._seams.<name>) instead:\n  " + "\n  ".join(
        offenders
    )


def test_a_patch_on_the_seam_reaches_the_service() -> None:
    """The point of the whole arrangement, exercised end to end on a cheap method."""
    from types import SimpleNamespace

    fake = SimpleNamespace(ecr_account_id="123456789012", batch_region="xx-test-1", ray_ecr_repository="some-repo")
    with patch("viva_api.simulation.dispatch._seams.get_settings", lambda: fake):
        uri = simulation_service_ray.SimulationServiceRay().batch.image_uri("abc1234")
    assert "123456789012" in uri and "xx-test-1" in uri and uri.endswith("some-repo:abc1234")


def test_the_service_composes_the_ray_package_and_inherits_nothing_from_it() -> None:
    """The carve ended with no mixin (``docs/plan-core.md`` P2.1, PR 5): the pieces in
    ``viva_api.simulation.dispatch`` are functions, composed services and -- soon -- strategies. A
    class from that package reappearing in the MRO would bring back shadowing by MRO, which
    is what the guard this one replaced existed to catch."""
    inherited = [
        c.__name__
        for c in simulation_service_ray.SimulationServiceRay.__mro__
        if c.__module__.startswith("viva_api.simulation.dispatch.")
    ]
    assert not inherited, f"SimulationServiceRay inherits from the dispatch package again: {inherited}"


def test_the_service_repeats_only_the_two_batch_questions_the_scheduler_asks_it() -> None:
    """A method on BOTH the service and its Batch layer is a delegate, and a delegate is a
    debt: it exists because something outside still asks the service. Two are intended (the
    scheduler's status and detail lookups, until P6). A third is someone re-growing the
    facade."""
    from viva_api.simulation.dispatch.batch_layer import BatchLayer

    def public(cls: type) -> set[str]:
        return {n for n, v in vars(cls).items() if callable(v) and not n.startswith("_")}

    both = public(simulation_service_ray.SimulationServiceRay) & public(BatchLayer)
    assert both == {"get_batch_job_statuses", "get_batch_job_details"}, both


#: Everything public on ``SimulationServiceRay`` that is not ``SimulationService``'s interface, by why it is
#: still there. After P2.1 PR 11 the class is its interface, five strategy builders, and THIS.
COMPOSED_SERVICES = {"parca", "tasks"}
#: one-call delegates: something outside (the scheduler, a handler, the capability probe, an integration
#: test) still asks the SERVICE. Each goes when the scheduler is split (``docs/plan-core.md`` P6).
DELEGATES_UNTIL_P6 = {
    "cache_s3_uri",
    "chain_base_tags",
    "get_batch_job_details",
    "get_batch_job_statuses",
    "reap_cancelled_campaign",
    "submit_campaign_analysis",
    "submit_chain_dispatch_job",
    "submit_chain_lineage_batch",
    "submit_multi_node_analysis",
}
#: real code that is progress, cancel or shared staging rather than dispatch: it joins the strategies
#: (or the scheduler's split halves) in P6, as the 2026-09-20 audit decided.
PROGRESS_CANCEL_AND_STAGING_UNTIL_P6 = {
    "stage_runner",
    "get_chain_campaign_result",
    "cancel_chain_campaign",
    "cancel_companion_jobs",
}


def test_the_services_public_surface_beyond_its_interface_is_named_and_may_only_shrink() -> None:
    """The carve ended with a class that is its interface plus a list. The list is debt with a
    due date; this pins it, so that re-growing the facade -- one convenient method at a time, which is
    how the class reached 5,019 lines -- has to be done on purpose, here, in a diff someone reads."""
    import inspect

    from viva_api.simulation.simulation_service import SimulationService

    service = simulation_service_ray.SimulationServiceRay
    interface = {name for name, _ in inspect.getmembers(SimulationService) if not name.startswith("_")}
    public = {name for name in vars(service) if not name.startswith("_")}
    beyond = public - interface
    expected = COMPOSED_SERVICES | DELEGATES_UNTIL_P6 | PROGRESS_CANCEL_AND_STAGING_UNTIL_P6
    assert beyond - expected == set(), f"new public surface on the service: {sorted(beyond - expected)}"
    assert expected - beyond == set(), (
        f"gone from the service -- delete it from the list too: {sorted(expected - beyond)}"
    )


def test_no_dispatch_mechanism_is_left_on_the_service() -> None:
    """Five mechanisms, five strategy objects. What the service keeps of each is a builder."""
    service = simulation_service_ray.SimulationServiceRay
    builders = {
        name for name in vars(service) if name in {"_chain", "_ensemble", "_mbp_tracked", "_multi_node", "_nextflow"}
    }
    assert builders == {"_chain", "_ensemble", "_mbp_tracked", "_multi_node", "_nextflow"}
    leftovers = sorted(
        name
        for name in vars(service)
        if name.startswith(("_submit_", "_seed_", "_sim_command", "_render_", "_nf_", "_mnp_", "_analysis_command"))
    )
    assert not leftovers, f"mechanism code is back on the service: {leftovers}"
