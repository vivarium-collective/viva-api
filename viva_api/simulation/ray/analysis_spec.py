"""What analysis a simulation asks for, and how much memory that analysis needs.

Carved out of ``simulation_service_ray.py`` (``docs/plan-core.md`` P2.1, PR 3 of the
2026-09-20 sequence) as a pure move: the constants and the two functions below are
byte-for-byte what they were there. They are pure functions of a config and an options
map -- no settings, no AWS, no database.

This is a SPECIFICATION, not a service. Analysis in this backend is three different things:

* a domain specification -- which modules, at which scales, in which memory class: this module;
* a shared compute pattern -- ``viva_api/common/analysis_dag.py``, used by Ray and by compose;
* per-mechanism glue -- ``submit_campaign_analysis`` belongs to the chain mechanism and
  ``submit_multi_node_analysis`` to the multi-node composite. They are not "the analysis
  service": they share a word, not a boundary, and each moves with its own mechanism when
  that mechanism becomes a strategy.

An earlier cut (#715) grouped all three into one ``RayAnalysisService``. The plan audit of
2026-09-20 withdrew it: a service is a common capability that works one way whatever the
dispatch mechanism (image build, the Batch seam), and analysis submission does not.

It is SMS code and stays SMS code: scales, generations and the sizing copied from the model
image are all domain knowledge. Nothing is re-exported from the old location.
"""

from typing import Any

from pydantic import BaseModel

# The analysis scales a v2ecoli ``analysis_options`` map can carry. Everything else
# in that (extra="allow") model — ``cpus``, ``memory_gb``, vEcoli-Nextflow-only keys —
# is not a scale and must not be forwarded as one.
ANALYSIS_SCALES = ("single", "multidaughter", "multigeneration", "multiseed", "multivariant")
# The composite's own "every analysis this batch's shape has the cells for" keyword
# (v2ecoli.steps.batch_baseline_runner.build_analysis_options). Used when the caller
# named no modules: sms-api runs outside the model image and has no ANALYSIS_REGISTRY
# to enumerate, so it asks the image to resolve the set with its own resolver rather
# than carrying a second, drift-prone copy of the list.
APPLICABLE_ANALYSES = "applicable"

# --- Analysis memory-class routing (viva-api#625 / v2ecoli#788) --------------
#
# An analysis container runs on one of two Batch instance classes: "standard"
# (the 60 GB box the analysis job def defaults to, which OOMed on CD2 --
# v2ecoli#786) and "large" (a 200 GB r7i queue). The class is picked HERE, at
# submission time, because the queue is chosen before the container runs -- so
# sms-api cannot ask the model image, where v2ecoli's own analysis_memory_class
# lives. This is a deliberate small local copy of that sizing (sms-api does not
# import v2ecoli at runtime; the analysis SCALES it reads are already carried in
# analysis_options). The constants below MUST stay in step with
# v2ecoli.workflow.analysis_runner: a per-lineage multiseed/multigeneration group
# peaks ~8 GB/generation (~78 GB over 10 generations, #786), past the 60 GB
# standard box; single/multidaughter read one cell and stay standard.
#
# DRIFT TRAP: v2ecoli's copy also honors an analysis class that DECLARES
# `memory_class = "large"` (analysis_runner._declared_memory_class); this copy
# does NOT -- it derives purely from scale x generations, because sms-api has no
# ANALYSIS_REGISTRY to read a class attribute from. So declaring memory_class on
# an analysis currently has NO effect on which queue the API picks. Latent today
# (nothing declares it). If a module ever needs to force "large" regardless of
# scale, thread the declared class through analysis_options / task_env so this
# function can see it; until then a declaration would route large in v2ecoli's
# in-image reasoning but STANDARD here -- do not let that gap go silent.
_ANALYSIS_STANDARD_INSTANCE_GB = 60
_ANALYSIS_GB_PER_GENERATION = 8.0
_ANALYSIS_MULTI_CELL_SCALES = frozenset({"multigeneration", "multiseed"})


def analysis_memory_class(
    analysis_options: dict[str, Any] | str | None,
    *,
    n_seeds: int | None = None,
    n_generations: int | None = None,
) -> str:
    """The Batch instance memory class an analysis submission needs: ``"standard"``
    or ``"large"``.

    Derived from the scales named in ``analysis_options`` (the ``{scale: {name:
    params}}`` shape, e.g. from ``analysis_modules_for``) and the sweep's
    ``n_generations``. Generations drive the per-lineage peak; ``n_seeds`` is
    accepted for interface parity with v2ecoli's function but the chunked readers
    make it a non-factor. Any multiseed/multigeneration analysis over enough
    generations routes the whole job to the large instance; everything else stays
    standard. Mirrors ``v2ecoli.workflow.analysis_runner.analysis_memory_class``
    -- see the note above on why sms-api keeps a local copy."""
    if not n_generations or not isinstance(analysis_options, dict):
        # No generation count, or the "applicable" keyword / any non-scale-map
        # (the image resolves the set itself) -- nothing to size on here.
        return "standard"
    over_standard = _ANALYSIS_GB_PER_GENERATION * int(n_generations) > _ANALYSIS_STANDARD_INSTANCE_GB
    for scale, entries in analysis_options.items():
        if scale in _ANALYSIS_MULTI_CELL_SCALES and isinstance(entries, dict) and entries and over_standard:
            return "large"
    return "standard"


def analysis_modules_for(config: Any) -> dict[str, dict[str, Any]] | str:
    """The analyses the analysis DAG node should run for this simulation.

    Reads the simulation's OWN ``config.analysis_options`` — the field the run
    endpoint already populates from the caller's ``--analysis-options`` (and that
    the workbench already fills from a study's ``spec.analyses``), and which this
    backend previously ignored entirely, so a remote dispatch's configured
    analyses never ran.

    Only real scale keys are forwarded, and only non-empty ones: the endpoint's
    own fallback default is ``{"multiseed": {}}`` — "no modules named", not "run
    nothing" — which resolves to the ``applicable`` keyword like any other
    unset case.
    """
    options: Any = getattr(config, "analysis_options", None)
    raw: dict[str, Any] = options.model_dump() if isinstance(options, BaseModel) else dict(options or {})
    modules = {
        scale: dict(entries)
        for scale, entries in raw.items()
        if scale in ANALYSIS_SCALES and isinstance(entries, dict) and entries
    }
    return modules or APPLICABLE_ANALYSES
