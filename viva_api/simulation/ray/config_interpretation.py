"""Reading a simulation config into dispatch parameters.

Carved out of ``simulation_service_ray.py`` (``docs/plan-core.md`` P2.1, cut 1) as a pure
move: the five functions below are byte-for-byte what they were there. They are pure
functions of a config -- no settings, no AWS, no database -- which is why they moved first.

They are SMS code and stay SMS code: every one of them knows what a strain, a ParCa
override or a vEcoli process swap is. Core never sees a config; it receives the parameters
these functions produce, as opaque values.

Nothing is re-exported from the old location. ``simulation_service_ray`` imports the four
it calls; ``job_scheduler``, ``scripts/cd2_nextflow_dispatches.py`` and the tests import
from here. ``strain_from_config`` was never used inside the service file at all -- it sat
there only because that was where everything sat.
"""

import logging

from viva_api.simulation.models import CompositeEngine

logger = logging.getLogger(__name__)


def _is_upstream_vecoli(composite: CompositeEngine | None) -> bool:
    """The pristine upstream-vEcoli engine (``--composite vecoli``).

    The single source of truth for the routing question "does this run need the
    separate upstream ParCa cache + ``--vecoli-source`` flag?" — used both to
    select the ParCa cache/command in ``submit_ecoli_simulation_job`` and to gate
    the ``--vecoli-source`` arg in ``_sim_command``.
    """
    return composite == "vecoli"


def strain_from_config(config: object) -> tuple[str | None, str | None]:
    """Return ``(new_genes, bundle_overrides)`` — the strain a run requested — from
    a config's ``parca_options``, or ``(None, None)`` for a wild-type/unset build.

    Same read the ParCa command uses (``getattr(config.parca_options, ...)``);
    threaded to the entrypoint as ``*_EXPECT_NEW_GENES`` / ``*_EXPECT_BUNDLE_OVERRIDES``
    so a wrong-strain staged cache is rejected (sms-ecoli#210 / #215). ``off``/empty
    is wild-type -> ``None`` (matches build_cache.py's own normalization).
    """
    parca_options = getattr(config, "parca_options", None)

    def _norm(value: object) -> str | None:
        # bundle_overrides can now be a list (multiple stacked --bundle-overrides
        # flags, see ParcaOptions.bundle_overrides) -- joined with "," for this
        # single EXPECT_* env-var string. No consumer reads *_EXPECT_BUNDLE_
        # OVERRIDES yet (confirmed empirically: no match for that name anywhere
        # in v2ecoli), so this is a viva-api-only convention pending a real
        # verifier, not a wire format anything downstream already expects.
        if isinstance(value, list):
            s: str | None = ",".join(str(v).strip() for v in value if str(v).strip()) or None
        else:
            s = value.strip() if isinstance(value, str) else None
        return None if not s or s == "off" else s

    return (
        _norm(getattr(parca_options, "new_genes", None)),
        _norm(getattr(parca_options, "bundle_overrides", None)),
    )


def injected_processes_from_config(config: object) -> dict[str, object] | None:
    """Build ``ecoli_baseline.baseline()``'s own ``injected_processes`` kwarg
    (backlog item 93) from a legacy config's ``swap_processes``/
    ``add_processes``/``exclude_processes`` -- real ``ExperimentRequest``
    fields (``viva_api/simulation/models.py``) that ride through
    ``SimulationConfig`` as extras (``extra="allow"``), so ``getattr`` is the
    correct read whether or not the field was ever declared on the model.

    Returns ``None`` when none of the three are set, so a config with no
    injection intent produces the exact ``overrides`` dict this dispatch path
    already built before this existed -- the byte-for-byte-unchanged
    regression property backlog items 86/88's own ``extra_params`` passthrough
    established for this same class of fix.

    ``fork_repo`` is always ``""``: every caller of this helper dispatches
    through ``ecoli_baseline``, the NATIVE (fork-free) composite --
    ``assert_injection_sourcing`` (v2ecoli's ``composites/ecoli_baseline.py``)
    rejects a non-empty ``fork_repo`` for a native composite outright, and
    ``scripts._compare.inject.resolve_injections`` indexes
    ``injected_processes["fork_repo"]`` directly (not ``.get``), so the key
    must be present even though it is always empty on this path.
    """

    # Two accepted shapes, resolved PER FIELD, not as a whole-block either/or:
    #   (1) FLAT: swap_processes / add_processes / exclude_processes as top-level
    #       config extras (the legacy shape this helper was written for).
    #   (2) NESTED: a whole ``injected_processes`` block passed through as an extra
    #       -- e.g. ``extra_params={"injected_processes": {"swap_processes": ...}}``,
    #       exactly the shape ``run_comparison_ensemble.py --from-vecoli-config``
    #       emits. This is what viva-api#385 hit: the nested block reached the
    #       resolved config, but this helper only read the flat fields, found none,
    #       returned None, and the swap was silently dropped at every downstream hop
    #       (chain-dispatch ran wild-type, reporting success).
    # A nested submit and the config's own flat fields are NOT mutually-exclusive
    # alternatives (viva-api#401): choosing the whole shape once meant a nested
    # submit setting only ``swap_processes`` silently dropped a config's own flat
    # ``add_processes``/``exclude_processes``, even though nothing about the
    # caller's request implied dropping them -- observed live (sim 296): a
    # mecillinam config's own 4 ``add_processes`` vanished under a nested swap
    # that never mentioned them. Each of the three fields is now resolved
    # independently -- the nested value wins when set, else fall back to flat --
    # so a caller sending only ``swap_processes`` keeps the config's own
    # ``add_processes``/``exclude_processes`` rather than losing them.
    def _read(src: object, key: str, default: object) -> object:
        if isinstance(src, dict):
            return src.get(key) or default
        return getattr(src, key, None) or default

    nested = getattr(config, "injected_processes", None)
    nested_has_intent = nested is not None and (
        _read(nested, "swap_processes", None)
        or _read(nested, "add_processes", None)
        or _read(nested, "exclude_processes", None)
    )

    flat_swap = _read(config, "swap_processes", {})
    flat_add = _read(config, "add_processes", [])
    flat_exclude = _read(config, "exclude_processes", [])
    nested_swap = _read(nested, "swap_processes", None)
    nested_add = _read(nested, "add_processes", None)
    nested_exclude = _read(nested, "exclude_processes", None)

    # A real conflict (both sides set the SAME field) is a silent override,
    # not just a merge -- worth a log line so it is at least discoverable,
    # per cplong90's own framing on #401 ("an override worth logging, not one
    # to make silently"), matching this codebase's own repeated fix pattern
    # for silent-success-masking-a-real-difference (items 93/103/111/114).
    for name, nested_val, flat_val in (
        ("swap_processes", nested_swap, flat_swap),
        ("add_processes", nested_add, flat_add),
        ("exclude_processes", nested_exclude, flat_exclude),
    ):
        if nested_val and flat_val:
            logger.warning(
                "injected_processes_from_config: nested %s overrides the config's own flat %s (nested=%r, flat=%r)",
                name,
                name,
                nested_val,
                flat_val,
            )

    swap_processes = nested_swap or flat_swap
    add_processes = nested_add or flat_add
    exclude_processes = nested_exclude or flat_exclude
    if not (swap_processes or add_processes or exclude_processes):
        return None

    # Carry the caller-supplied nested block through rather than rebuilding a
    # fresh dict from just the four keys below (viva-api#392): a nested submit
    # can carry additional real intent -- e.g. `cache_dir`, which a fork-free
    # swap's own resolve_injections() spec-building needs to load the target
    # process's config from the ParCa bundle. Reconstructing only
    # {swap_processes, add_processes, exclude_processes, fork_repo} silently
    # dropped it, so the swapped-in process mounted with an empty config
    # instead of failing loud or running correctly. The four keys are
    # normalized defaults layered ON TOP of the carried-through block, not a
    # replacement for it, so this stays byte-identical for the flat/legacy
    # shape (nothing to carry through there) and for any nested submit that
    # never set the four keys to begin with.
    result: dict[str, object] = dict(nested) if isinstance(nested, dict) and nested_has_intent else {}
    result.update({
        "swap_processes": swap_processes,
        "add_processes": add_processes,
        "exclude_processes": exclude_processes,
        "fork_repo": "",
    })
    return result


def _thread_injected_processes_into_params(params: dict[str, object], config: object) -> None:
    """Thread the config's own injection intent into a multi-node composite's params.

    sms-ecoli#166 (2026-09-09): a config's top-level ``swap_processes`` /
    ``add_processes`` / ``exclude_processes`` (or a nested ``injected_processes``
    block) never reached the multi-node path -- only chain dispatch and the
    single-shot ``_sim_command`` called ``injected_processes_from_config`` -- so
    every ``lineage_ray_batch`` CD2 Run 4 dispatch (both arms) silently ran CLASSIC
    ``ecoli-metabolism`` although its stored config declared the redux swap (787's
    GLP_UNBND traceback is in ``v2ecoli/processes/metabolism.py``; 744's history
    carries the classic-only FBA listeners and none of redux's). The composite
    already accepts ``injected_processes`` as a param (v2ecoli#663).

    Explicit ``params["injected_processes"]`` wins (the same "explicit params win"
    rule every other key follows); a config with no injection intent leaves
    ``params`` byte-for-byte unchanged (the helper returns None). Mutates in place.
    """
    if "injected_processes" in params:
        return
    injected = injected_processes_from_config(config)
    if injected:
        params["injected_processes"] = injected


def _batch_domain_overrides(
    *,
    injected_processes: dict[str, object] | None = None,
    variants: dict[str, object] | None = None,
    config_overrides: dict[str, object] | None = None,
    features: list[object] | None = None,
    exchange_fluxes: dict[str, object] | None = None,
    exchange_flux_basis: str | None = None,
) -> dict[str, object]:
    """The submitted config's DOMAIN fields as ``ecoli_baseline.baseline()``'s own
    batch-mode ``--overrides`` keys (the CD2 native seam).

    v2ecoli #640 threaded ``injected_processes``/``features``/``exchange_fluxes``/
    ``exchange_flux_basis`` through ``_build_batch_document``; ``config_overrides``
    and ``variants`` already existed. Each key is emitted ONLY when non-empty, so a
    config with no injection/variant intent yields ``{}`` and the caller's overrides
    dict is byte-for-byte what this path built before threading was added -- the
    same regression contract as ``injected_processes_from_config`` and the 0.9.79
    chain-dispatch passthrough. ``exchange_flux_basis`` rides only alongside a flux
    map (the composite defaults it to "").
    """
    out: dict[str, object] = {}
    if injected_processes:
        out["injected_processes"] = injected_processes
    if variants:
        out["variants"] = variants
    if config_overrides:
        out["config_overrides"] = config_overrides
    if features:
        out["features"] = features
    if exchange_fluxes:
        out["exchange_fluxes"] = exchange_fluxes
        if exchange_flux_basis:
            out["exchange_flux_basis"] = exchange_flux_basis
    return out
