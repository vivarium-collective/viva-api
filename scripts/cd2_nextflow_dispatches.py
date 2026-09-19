#!/usr/bin/env python3
"""Translate the CD2 team's workflow configs into Nextflow campaign dispatches.

The team's runs are authored as v2ecoli WORKFLOW-schema configs
(``sms-ecoli/configs/cd2/run*.json``: ``generations``, ``n_init_sims``,
``swap_processes``/``add_processes``/``injected_processes``, ``analysis_options``)
and dispatched today through ``multi_node_dispatch`` (``lineage_ray_batch``), one
dispatch per founder cache. The Nextflow path (``workflow_nf``) takes the SAME
science as one campaign: N variants x M seeds, each variant carrying its own
``cache_uri`` / ``new_genes`` / ``bundle_overrides`` / ``injected_processes`` /
``config_overrides``, ParCa (or cache fetch) per variant, and the analysis gather
in-campaign.

This script is the bridge. It is deliberately a *planner*: it never talks to the
API. It emits the exact ``atlantis composite nextflow`` command (and the raw
``--params`` JSON) for a campaign, so the dispatch itself stays on the proven CLI
path and nothing here can touch the Ray/MNP/chain dispatchers.

Mapping, and where each piece comes from:

* ``injected_processes``  <- ``viva_api.simulation.ray.config_interpretation
  .injected_processes_from_config`` -- the API's OWN reading of the config's flat
  (Runs 1/2/4: swap/exclude) or nested (Run 3: add_processes + process_configs
  + seed_bulk_species + ...) form. On the Nextflow path this rides INSIDE each
  variant spec (``workflow_nf`` has no campaign-level key), exactly as sim 683.
* ``analysis_options``     <- ``analysis_modules_for`` -- only real scales,
  non-empty; ``--include-analysis`` is set whenever any survive.
* ``exchange_fluxes`` / ``exchange_flux_basis`` -- a DISPATCH decision, not a
  config default (met-eng Q6): the violacein product axis for Runs 1/2/4, omitted
  for Run 3. Top-level ``workflow_nf`` knobs since v2ecoli#746.
* founders -- Runs 1/2 are "ten dispatches of one" on the MNP path so each seed
  gets its own founder cache. Here that is ten ONE-SEED VARIANTS, each with its
  own ``cache_uri`` (shape B), or one variant on a chassis cache with
  ``--independent-founders`` (shape A, sim 683). ``n_init_sims`` must be 1 in
  the config for the same reason it must be 1 there.
* genotypes (Run 4) -- identity lives ENTIRELY in the per-genotype ParCa cache
  (``cd2-run4-carina-genotype{N}``, 42 caches serving 84 dispatches over two
  media; config B adds 16 ``strain_design`` variant caches). Here: one variant
  per cache (``--cache-commit`` + ``--cache-variants``), one campaign per
  medium (``--media``). No ``new_genes`` needed -- the cache is the strain.
* doses (Run 3) -- there is NO dose sweep on the MNP path today
  (``field_timeline.timeline`` is ``[]``; vEcoli's variants grammar is
  un-ported). ``workflow_nf`` carries ``injected_processes`` PER VARIANT, so a
  sweep is expressible here: a variant spec may carry
  ``injected_processes_patch``, deep-merged over the config's block (e.g. a
  per-dose ``process_configs.field_timeline.timeline``). What goes in the patch
  is met-eng's; the mechanism is generic.
* any variant spec (``--variants-json``) may carry ``variant_name`` plus
  ``new_genes``, ``bundle_overrides``, ``cache_uri``, ``config_overrides``,
  ``injected_processes`` (wins outright) or ``injected_processes_patch``.

Nothing here is dispatched automatically -- the operator runs the printed command.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from viva_api.simulation.models import SimulationConfig
from viva_api.simulation.ray.config_interpretation import injected_processes_from_config
from viva_api.simulation.simulation_service_ray import analysis_modules_for

#: The product-axis listener pair Runs 1/2/4 dispatch with (met-eng Q6); Run 3 has none.
VIOLACEIN_EXCHANGE_FLUXES: dict[str, str] = {"glucose_exchange": "GLC", "violacein_exchange": "VIOLACEIN"}
EXCHANGE_FLUX_BASIS = "gdcw"

#: Config file per CD2 run, relative to an sms-ecoli checkout.
RUN_CONFIGS: dict[str, str] = {
    "1": "configs/cd2/run1_k4_cellonly.json",
    "2": "configs/cd2/run2_j3_injected_metabolism.json",
    "3": "configs/cd2/run3_antibiotic_pg_sulfadiazine.json",
    "4": "configs/cd2/run4_fss_bioproduction.json",
}
#: Which runs carry the violacein product axis.
RUNS_WITH_EXCHANGE_FLUXES = frozenset({"1", "2", "4"})
#: Founder-mapping arms (``out/founder_ensemble_mapping.json``) per run.
FOUNDER_ARMS: dict[str, str] = {"1": "k4_cell_only", "2": "j3_cell_only"}


#: The shared bucket's ParCa cache root; a cache lives at <root>/<commit>/<variant>/.
DEFAULT_CACHE_ROOT = "s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/ray-parca-cache"


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Return ``base`` with ``patch`` merged in: nested dicts merge, anything else is replaced."""
    out = dict(base)
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def cache_uri_for(commit: str, variant: str | None, root: str = DEFAULT_CACHE_ROOT) -> str:
    """``<root>/<commit>/<variant>/`` -- the layout every arm stages from (the bare
    ``<commit>/`` form is the stock cache). Trailing slash kept: the fetch is recursive."""
    base = f"{root.rstrip('/')}/{commit}"
    return f"{base}/{variant}/" if variant else f"{base}/"


def cache_variants(names: list[str], commit: str, prefix: str, root: str = DEFAULT_CACHE_ROOT) -> list[dict[str, Any]]:
    """One variant per named ParCa cache (Run 4's genotypes; any cache_variant list)."""
    if not commit:
        raise TranslationError("--cache-commit is required with --cache-variants")
    return [{"variant_name": f"{prefix}_{n}", "cache_uri": cache_uri_for(commit, n, root)} for n in names]


def expand_template(template: str, first: int, last: int) -> list[str]:
    """``cd2-run4-carina-genotype{n}`` x 1..42 -> the 42 names."""
    return [template.format(n=i) for i in range(first, last + 1)]


class TranslationError(ValueError):
    """The config or inputs cannot be turned into a sound campaign."""


@dataclass
class CampaignPlan:
    label: str
    simulator_id: int
    simulation_config: str
    n_seeds: int
    n_generations: int
    params: dict[str, Any]
    analysis_options: dict[str, Any] | None
    independent_founders: bool = False
    cache_uri: str | None = None
    tag: str = "cd2-nextflow"
    description: str = ""
    base_url: str = "http://localhost:8080"
    extra_args: list[str] = field(default_factory=list)

    def cli_args(self) -> list[str]:
        args = [
            "uv",
            "run",
            "atlantis",
            "composite",
            "nextflow",
            self.label,
            str(self.simulator_id),
            "--simulation-config",
            self.simulation_config,
            "--executor",
            "awsbatch",
            "--seeds",
            str(self.n_seeds),
            "--generations",
            str(self.n_generations),
        ]
        if self.analysis_options:
            args += [
                "--include-analysis",
                "--analysis-options",
                json.dumps(self.analysis_options, separators=(",", ":")),
            ]
        if self.independent_founders:
            args.append("--independent-founders")
        if self.cache_uri:
            args += ["--cache-uri", self.cache_uri]
        args += ["--params", json.dumps(self.params, separators=(",", ":"))]
        args += ["--tag", self.tag]
        if self.description:
            args += ["--description", self.description]
        args += ["--no-poll", "--base-url", self.base_url, *self.extra_args]
        return args

    def command(self) -> str:
        return " ".join(shlex.quote(a) for a in self.cli_args())


def load_workflow_config(path: Path) -> dict[str, Any]:
    cfg: dict[str, Any] = json.loads(path.read_text())
    cfg.pop("_provenance", None)
    return cfg


def config_model(cfg: dict[str, Any], campaign_id: str) -> SimulationConfig:
    """The API's own model over the workflow config, so its readers apply verbatim.

    ``experiment_id`` is required by the model and deliberately ABSENT from the
    team's configs (it would shadow the per-dispatch id on the MNP path); here it
    is the campaign id, which is also what ``_nf_generator_params`` would default
    it to on the API side.
    """
    if "experiment_id" in cfg:
        raise TranslationError(
            "the workflow config carries experiment_id; the team's configs keep it ABSENT on "
            "purpose (sms-ecoli#235) -- drop it and let the campaign id name the run"
        )
    return SimulationConfig(**{**cfg, "experiment_id": campaign_id})


def injected_processes_for(cfg: dict[str, Any], campaign_id: str) -> dict[str, Any] | None:
    return injected_processes_from_config(config_model(cfg, campaign_id))


def analysis_options_for(cfg: dict[str, Any], campaign_id: str) -> dict[str, Any] | None:
    modules = analysis_modules_for(config_model(cfg, campaign_id))
    return modules if isinstance(modules, dict) and modules else None


def variant_specs(
    base_variants: list[dict[str, Any]],
    injected_processes: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Every variant carries the config's injection block (there is no campaign-level
    key on ``workflow_nf``); a variant's own ``injected_processes`` wins if present."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, v in enumerate(base_variants):
        spec = dict(v)
        spec.setdefault("variant_name", f"variant_{i}")
        if spec["variant_name"] in seen:
            raise TranslationError(f"duplicate variant_name {spec['variant_name']!r}")
        seen.add(spec["variant_name"])
        patch = spec.pop("injected_processes_patch", None)
        if not spec.get("injected_processes") and injected_processes:
            spec["injected_processes"] = deep_merge(injected_processes, patch) if patch else injected_processes
        elif patch:
            raise TranslationError(
                f"variant {spec['variant_name']!r} carries injected_processes_patch but there is no block to patch"
            )
        out.append(spec)
    return out


def founder_variants(mapping: dict[str, Any], arm: str, prefix: str) -> list[dict[str, Any]]:
    """Ten one-seed variants from DI's founder mapping, one per staged cache (shape B)."""
    rows = (mapping.get(arm) or {}).get("rows") or []
    if not rows:
        raise TranslationError(f"founder mapping has no rows for arm {arm!r}")
    out = []
    for r in rows:
        uri = r.get("cache_s3_uri")
        if not uri or uri == "PENDING":
            raise TranslationError(f"founder seed {r.get('seed')} has no staged cache_s3_uri (got {uri!r})")
        out.append({"variant_name": f"{prefix}_seed{r['seed']}", "cache_uri": uri})
    return out


def parse_index_set(spec: str) -> set[int]:
    """``"0,5-7"`` -> ``{0, 5, 6, 7}``."""
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def run3_sweep_variants(
    base_cfg: dict[str, Any], sms_ecoli: Path, campaign_id: str, combos: set[int] | None = None
) -> tuple[list[dict[str, Any]], int, int]:
    """One variant per (mecillinam, sulfadiazine) dose combo, from sms-ecoli#299's own generator.

    ``sms_modules.bridge.antibiotic_cocktail_sweep.build_all_combo_configs`` resolves the
    36-point grid the way vEcoli-private's ``antibiotic_cocktail_timeline`` does -- one
    complete config per combo, ``field_timeline.timeline`` populated, scale bumped to the
    real 4 seeds x 20 generations. On the MNP path that is 36 dispatches; here it is ONE
    campaign whose variants each carry their own resolved injection block (the campaign
    id names the run, so the per-combo ``experiment_id`` the generator stamps is dropped
    -- the ``variant=`` partition is the combo's identity). ``combos`` keeps only those
    grid indices (0-35, the generator's order: mecillinam outer, sulfadiazine inner) -- a
    pilot is one combo, the real Run 3 is all 36. Returns ``(variants, n_seeds, n_generations)``.
    """
    import importlib.util

    mod_path = sms_ecoli / "sms_modules" / "bridge" / "antibiotic_cocktail_sweep.py"
    if not mod_path.exists():
        raise TranslationError(f"{mod_path} not found -- needs sms-ecoli >= #299")
    spec = importlib.util.spec_from_file_location("antibiotic_cocktail_sweep", mod_path)
    if spec is None or spec.loader is None:
        raise TranslationError(f"cannot load {mod_path}")
    sweep: Any = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = sweep
    spec.loader.exec_module(sweep)
    base = {**base_cfg, "experiment_id": base_cfg.get("experiment_id") or campaign_id}  # the generator indexes it
    all_combos = sweep.build_all_combo_configs(base)
    if combos is not None:
        bad = sorted(c for c in combos if c < 0 or c >= len(all_combos))
        if bad:
            raise TranslationError(f"--run3-combos out of range {bad}: the grid has {len(all_combos)} combos")
    variants: list[dict[str, Any]] = []
    for i, combo in enumerate(all_combos):
        if combos is not None and i not in combos:
            continue
        tl = combo["injected_processes"]["process_configs"]["field_timeline"]["timeline"]
        mec = next(v["mecillinam"] for _, v in tl if "mecillinam" in v)
        sulf = next(v["sulfadiazine"] for _, v in tl if "sulfadiazine" in v)
        cfg = {k: v for k, v in combo.items() if k not in ("experiment_id", "_note", "_provenance")}
        inj = injected_processes_for(cfg, campaign_id)
        if not inj:
            raise TranslationError(f"combo {i}: no injection block came out of the generated config")
        variants.append({"variant_name": f"combo{i:02d}_mec{mec:g}_sulf{sulf:g}", "injected_processes": inj})
    return variants, int(all_combos[0]["n_init_sims"]), int(all_combos[0]["generations"])


def plan_campaign(
    *,
    run: str,
    cfg: dict[str, Any],
    label: str,
    simulator_id: int,
    simulation_config: str,
    variants: list[dict[str, Any]] | None = None,
    n_seeds: int | None = None,
    n_generations: int | None = None,
    cache_uri: str | None = None,
    independent_founders: bool = False,
    media: str | None = None,
    exchange_fluxes: dict[str, str] | None = None,
    base_url: str = "http://localhost:8080",
    tag: str = "cd2-nextflow",
    description: str = "",
    legacy_image: bool = False,
) -> CampaignPlan:
    if run not in RUN_CONFIGS:
        raise TranslationError(f"unknown run {run!r}")
    gens = int(n_generations if n_generations is not None else cfg.get("generations") or 0)
    seeds = int(n_seeds if n_seeds is not None else cfg.get("n_init_sims") or 0)
    if gens < 1 or seeds < 1:
        raise TranslationError(f"need generations >= 1 and seeds >= 1 (got {gens}, {seeds})")
    if (
        variants
        and len(variants) > 1
        and seeds > 1
        and any(v.get("cache_uri") for v in variants)
        and not independent_founders
    ):
        # per-variant founder caches with several seeds each would share a founder per variant
        raise TranslationError(
            "several seeds per founder-cache variant share one founder (v2ecoli#693); "
            "use one seed per variant or --independent-founders"
        )
    inj = injected_processes_for(cfg, label)
    fluxes = (
        (exchange_fluxes or VIOLACEIN_EXCHANGE_FLUXES)
        if (run in RUNS_WITH_EXCHANGE_FLUXES or exchange_fluxes)
        else None
    )
    if fluxes:
        # In BOTH places on purpose: the top-level knobs exist only since
        # v2ecoli#746 (simulators >= 178); on an older image they are swallowed
        # by the generator's **_ignored and the two KPI columns silently vanish
        # (sim 679). LineageProcess reads the injection block first, so carrying
        # them there too is what sim 683 did and works on every image.
        inj = {
            **(inj or {"swap_processes": {}, "add_processes": [], "exclude_processes": [], "fork_repo": ""}),
            "exchange_fluxes": fluxes,
            "exchange_flux_basis": EXCHANGE_FLUX_BASIS,
        }
    specs = variant_specs(variants or [{"variant_name": f"run{run}"}], inj)
    params: dict[str, Any] = {"variants": specs}
    # Top-level lineage knobs (media, time_step, exchange_fluxes, ...) exist on
    # workflow_nf only since v2ecoli#746 (simulators >= 178). On an older image
    # CompositeSpec.to_document RAISES `KeyError: unknown override(s)` for them
    # (sim 732 on simulator 173), so --legacy-image emits none of them; the
    # injection block above still carries the fluxes, which is all a pre-#746
    # LineageProcess reads anyway. `media` cannot be set on such an image.
    if not legacy_image:
        if media:
            params["media"] = media
        if "time_step" in cfg and cfg["time_step"] is not None:
            params["time_step"] = cfg["time_step"]
        if fluxes:
            params["exchange_fluxes"] = fluxes
            params["exchange_flux_basis"] = EXCHANGE_FLUX_BASIS
    elif media:
        raise TranslationError("--media needs a #746 image (simulator >= 178); a legacy image cannot set it")
    return CampaignPlan(
        label=label,
        simulator_id=simulator_id,
        simulation_config=simulation_config,
        n_seeds=seeds,
        n_generations=gens,
        params=params,
        analysis_options=analysis_options_for(cfg, label),
        independent_founders=independent_founders,
        cache_uri=cache_uri,
        tag=tag,
        description=description,
        base_url=base_url,
    )


def resolve_variants(a: argparse.Namespace, cfg: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Turn the CLI's founder / cache / sweep / JSON options into the variant list (and default seeds/gens)."""
    variants: list[dict[str, Any]] | None = None
    if a.founders:
        variants = founder_variants(json.loads(a.founders.read_text()), FOUNDER_ARMS[a.run], f"run{a.run}")
        if a.seeds is None:
            a.seeds = 1
    if a.cache_variants or a.cache_template:
        names = [n.strip() for n in (a.cache_variants or "").split(",") if n.strip()]
        if a.cache_template:
            if not a.cache_range or "-" not in a.cache_range:
                raise SystemExit("--cache-template needs --cache-range first-last")
            lo, hi = (int(x) for x in a.cache_range.split("-", 1))
            names += expand_template(a.cache_template, lo, hi)
        variants = cache_variants(names, a.cache_commit or "", f"run{a.run}", a.cache_root)
        if a.seeds is None:
            a.seeds = int(cfg.get("n_init_sims") or 1)
    if a.run3_sweep:
        if a.run != "3":
            raise SystemExit("--run3-sweep is for --run 3")
        variants, sweep_seeds, sweep_gens = run3_sweep_variants(
            cfg, a.sms_ecoli, a.label, combos=parse_index_set(a.run3_combos) if a.run3_combos else None
        )
        a.seeds = a.seeds if a.seeds is not None else sweep_seeds
        a.generations = a.generations if a.generations is not None else sweep_gens
    if a.variants_json:
        variants = json.loads(a.variants_json)
    return variants


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--run", required=True, choices=sorted(RUN_CONFIGS))
    p.add_argument("--sms-ecoli", type=Path, required=True, help="path to an sms-ecoli checkout")
    p.add_argument("--simulator", type=int, required=True, help="simulator database id (e.g. 181)")
    p.add_argument("--label", required=True, help="campaign label -> experiment_id sim<id>-<label>-<hex>")
    p.add_argument(
        "--simulation-config",
        default="experiments/test_violacein_with_metabolism_native_run.json",
        help="the API-side config filename the CLI names (as on sim 683)",
    )
    p.add_argument("--founders", type=Path, help="Runs 1/2: DI's founder_ensemble_mapping.json (shape B)")
    p.add_argument("--cache-uri", help="shape A: one chassis cache for every seed (pair with --independent-founders)")
    p.add_argument("--independent-founders", action="store_true")
    p.add_argument(
        "--run3-sweep", action="store_true", help="Run 3: one variant per mec x sulf dose combo (sms-ecoli#299 grid)"
    )
    p.add_argument(
        "--run3-combos", help="with --run3-sweep: keep only these grid indices, e.g. '0' or '0,5-7' (default all 36)"
    )
    p.add_argument(
        "--variants-json",
        help="Runs 3/4: JSON list of variant specs (name, new_genes, bundle_overrides, cache_uri, config_overrides)",
    )
    p.add_argument(
        "--cache-commit", help="commit prefix the named caches were staged under (<root>/<commit>/<variant>/)"
    )
    p.add_argument("--cache-variants", help="comma-separated cache_variant names -> one variant each (Run 4 genotypes)")
    p.add_argument("--cache-template", help="e.g. 'cd2-run4-carina-genotype{n}' with --cache-range")
    p.add_argument("--cache-range", help="first-last, e.g. 1-42 (with --cache-template)")
    p.add_argument("--cache-root", default=DEFAULT_CACHE_ROOT)
    p.add_argument("--seeds", type=int)
    p.add_argument("--generations", type=int)
    p.add_argument("--media", help="Run 4: 'minimal' or the tryptophan condition name")
    p.add_argument("--tag", default="cd2-nextflow")
    p.add_argument("--description", default="")
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--emit", choices=("cli", "json", "both"), default="both")
    p.add_argument(
        "--legacy-image",
        action="store_true",
        help="simulator predates v2ecoli#746 (< 178): emit no top-level lineage knobs",
    )
    a = p.parse_args(argv)

    cfg = load_workflow_config(a.sms_ecoli / RUN_CONFIGS[a.run])
    variants = resolve_variants(a, cfg)
    plan = plan_campaign(
        run=a.run,
        cfg=cfg,
        label=a.label,
        simulator_id=a.simulator,
        simulation_config=a.simulation_config,
        variants=variants,
        n_seeds=a.seeds,
        n_generations=a.generations,
        cache_uri=a.cache_uri,
        independent_founders=a.independent_founders,
        media=a.media,
        base_url=a.base_url,
        tag=a.tag,
        description=a.description,
        legacy_image=a.legacy_image,
    )
    if a.emit in ("json", "both"):
        print(
            json.dumps(
                {
                    "params": plan.params,
                    "analysis_options": plan.analysis_options,
                    "n_seeds": plan.n_seeds,
                    "n_generations": plan.n_generations,
                },
                indent=2,
            )
        )
    if a.emit in ("cli", "both"):
        print(plan.command())
    return 0


if __name__ == "__main__":
    sys.exit(main())
