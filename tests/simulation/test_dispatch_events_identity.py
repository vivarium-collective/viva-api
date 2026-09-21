"""Every dispatch path must merge the events identity under the request's task_env.

The observability plan's D4a says "every dispatched unit of work gets a ``PBG_*``
block". Nothing enforced it, and two paths in ``simulation_service_ray`` shipped
without it: ``submit_ecoli_simulation_job`` (the MNP ParCa + simulation pair) and
``_submit_analysis_job`` (the gather). The failure was silent in the worst way --
the API still derives a ``trace_id`` from the run's ``correlation_id`` and writes
it to the ``HpcRun`` row, so ``/status`` showed an identity while the submitted
AWS Batch job carried no ``PBG_*`` at all and the run emitted nothing. It was
found by dispatching a real 1-generation campaign and reading the job's
environment back out of Batch, which is not a thing a test suite does.

So this is a STATIC check, deliberately: it reads the dispatch module's AST and
asserts the invariant at the seam where it is actually expressible -- a
``resolve_task_env(...)`` result must reach ``with_events_env(...)``, either
nested directly or by way of a local name. It cannot verify that the env
survives to Batch (only a live dispatch does that, see the module docstring of
``viva_api/common/events_env.py``), but it does catch the exact regression that
happened: a new dispatch method that resolves the caller's env and forgets the
identity.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

DISPATCH_MODULES = [
    "viva_api/simulation/simulation_service_ray.py",
    "viva_api/simulation/job_scheduler.py",
    # Added after this guard MISSED submit_standalone_analysis, which injected no
    # PBG_* at all: the module was simply not scanned. See the identity test below
    # for the other half of that miss.
    "viva_api/simulation/simulation_service_k8s.py",
    # The Ray service is being carved into viva_api/simulation/dispatch/ (docs/plan-core.md P2.1),
    # and this guard has already missed a dispatch once because "the module was simply not
    # scanned". So the package is globbed, not listed: a dispatch method that moves into a
    # new file there is scanned without anyone remembering to add the file.
    *sorted(str(path) for path in pathlib.Path("viva_api/simulation/dispatch").glob("*.py")),
]

#: Where the Ray service's dispatch methods may live: the service module and its package.
RAY_SERVICE_MODULES = [m for m in DISPATCH_MODULES if "simulation_service_ray" in m or "/simulation/dispatch/" in m]

# Methods that resolve a task env but deliberately do NOT carry an events identity.
# Keep this empty if you can: an entry here is a dispatch whose events are invisible.
EXEMPT: set[str] = set()


def _functions(tree: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)]


def _calls_named(node: ast.AST, name: str) -> list[ast.Call]:
    out = []
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name:
            out.append(n)
    return out


def _covered_resolve_calls(fn: ast.AST) -> set[int]:
    """ids() of ``resolve_task_env`` calls whose value reaches ``with_events_env``.

    Two shapes count, because both are used in the tree:

      * nested directly -- ``with_events_env(resolve_task_env(cfg), ...)``
      * via a local name -- ``env = resolve_task_env(cfg)`` then
        ``with_events_env(env, ...)``, which is how a method that submits more
        than one job (ParCa *and* simulation) shares one resolved env.
    """
    covered: set[int] = set()
    wrapped_names: set[str] = set()

    for wrap in _calls_named(fn, "with_events_env"):
        for inner in _calls_named(wrap, "resolve_task_env"):
            covered.add(id(inner))
        for arg in wrap.args:
            if isinstance(arg, ast.Name):
                wrapped_names.add(arg.id)

    if wrapped_names:
        for node in ast.walk(fn):
            if not isinstance(node, ast.Assign):
                continue
            targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if targets & wrapped_names:
                for inner in _calls_named(node.value, "resolve_task_env"):
                    covered.add(id(inner))
    return covered


@pytest.mark.parametrize("module_path", DISPATCH_MODULES)
def test_every_resolved_task_env_carries_the_events_identity(module_path: str) -> None:
    source = pathlib.Path(module_path).read_text()
    tree = ast.parse(source)

    offenders: list[str] = []
    for fn in _functions(tree):
        if fn.name in EXEMPT:
            continue
        resolves = _calls_named(fn, "resolve_task_env")
        if not resolves:
            continue
        covered = _covered_resolve_calls(fn)
        for call in resolves:
            if id(call) not in covered:
                offenders.append(f"{module_path}:{call.lineno} in {fn.name}()")

    assert not offenders, (
        "These dispatch paths resolve the caller's task_env but never merge the events "
        "identity under it, so the job they submit carries no PBG_* and emits nothing "
        "while its HpcRun row still shows a trace_id:\n  " + "\n  ".join(offenders)
    )


def test_the_known_dispatch_paths_are_all_still_covered() -> None:
    """A companion to the scan above: name the methods, so deleting one is visible.

    The scan passes vacuously if a method stops calling ``resolve_task_env``
    altogether (e.g. someone inlines it). This pins the list that must keep
    carrying an identity.
    """
    # A method still on the service is pinned by its bare name; one that moved into the package
    # is pinned as ``<module>.<name>``, because every strategy's entry point is called ``submit``
    # and five bare ``submit``s would pin nothing.
    with_identity = {
        fn.name if "/simulation/dispatch/" not in module_path else f"{pathlib.Path(module_path).stem}.{fn.name}"
        for module_path in RAY_SERVICE_MODULES
        for fn in _functions(ast.parse(pathlib.Path(module_path).read_text()))
        if _calls_named(fn, "with_events_env")
    }
    expected = {
        "nextflow.submit",  # NextflowStrategy (P2.1 PR 8)
        "mbp_tracked.submit",  # MbpTrackedStrategy (P2.1 PR 7)
        "multi_node.submit",  # MultiNodeCompositeStrategy (P2.1 PR 9)
        "chain.submit_chain_dispatch_job",  # ChainStrategy (P2.1 PR 11)
        "ensemble.submit",  # EnsembleStrategy: was the router's own tail (P2.1 PR 10)
        "chain._submit_analysis_job",
    }
    missing = expected - with_identity
    assert not missing, f"dispatch methods that lost their events identity: {sorted(missing)}"


#: Dispatch methods that build a job's environment BY HAND rather than through
#: ``resolve_task_env``. The scan above keys on ``resolve_task_env``, so it is
#: structurally blind to these -- a test that pins the right pattern still misses
#: a caller using a different one. ``submit_standalone_analysis`` proved it: it
#: shipped with no ``PBG_*`` at all and the guard stayed green.
HAND_BUILT_ENV_DISPATCHES = {
    # submit_ray_native_analysis: the path standalone analyses of sms-ecoli/v2ecoli
    # simulations actually take. It too shipped with no PBG_* while the guard was green
    # (found in data provenance slice 1); the #646 fix had covered only the legacy path.
    "viva_api/simulation/simulation_service_k8s.py": {"submit_standalone_analysis", "submit_ray_native_analysis"},
}


@pytest.mark.parametrize("module_path", sorted(HAND_BUILT_ENV_DISPATCHES))
def test_hand_built_dispatch_envs_still_inject_the_identity(module_path: str) -> None:
    """Cover the dispatches the resolve_task_env scan cannot see.

    ``submit_standalone_analysis`` constructs a K8s Job env as a literal list of
    ``V1EnvVar``. It never calls ``resolve_task_env``, so the scan above reports
    it as clean whether or not it injects anything. It did not -- ``atlantis
    simulation analysis`` produced runs with no identity and therefore no events,
    which looked exactly like the benign "no sink configured" case.
    """
    tree = ast.parse(pathlib.Path(module_path).read_text())
    for fn in _functions(tree):
        if fn.name not in HAND_BUILT_ENV_DISPATCHES[module_path]:
            continue
        assert _calls_named(fn, "events_env") or _calls_named(fn, "with_events_env"), (
            f"{module_path}:{fn.lineno} {fn.name}() builds a dispatch environment by hand "
            "and never adds the PBG_* identity, so its jobs emit nothing"
        )
