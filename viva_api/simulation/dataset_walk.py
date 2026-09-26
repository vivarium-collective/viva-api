"""The application's half of the reconciliation walk (core split, ``docs/plan-core.md`` P4a-2).

The walk itself -- listing a root, grouping bundles, registering what the classifier recognises,
marking vanished objects, never opening one -- moved verbatim to :mod:`viva_core.datasets.walk`.
What stays here is what core cannot know: **this application's file-naming convention** and
**what it walks**.

What a bundle looks like (verified on dev, 2026-09-15)::

    <out_uri>/analyses/<bundle>/analysis.json                              -> report (listed, never read; 240 MB seen)
    <out_uri>/analyses/<bundle>/ptools/ptools_rna_multiseed__variant=0.tsv      -> ptools-analysis
    <out_uri>/analyses/<bundle>/ptools/ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv
    <out_uri>/analyses/<bundle>/viz/<same naming>.html                         -> figure
    <out_uri>/analyses/<bundle>/driver.log                                      -> not a dataset

File names follow v2ecoli's ``analysis_runner``: ``f"{name}__{group}"`` with the group key's ``/``
turned into ``_``. The group key's shape gives the protocol (``variant`` only = multiseed;
``variant, seed`` = multigeneration; ``variant, seed, gen, agent`` = single; ``..., parent`` =
multidaughter; ``all``), and a ``_<scale>`` suffix on the name, when present, wins. That is
:class:`SmsArtifactClassifier` (core's ``ArtifactClassifier``).

What is walked is a simulation's ``<out_uri>/analyses/`` prefix; a bundle belongs to the analysis
run whose ``result_uri`` is the bundle directory, else to the simulation whose output it sits
under. That is :class:`SimulationWalkSource` (core's ``WalkSource``).

:func:`reconcile_simulation` and :func:`register_bundle` keep the signatures the scheduler, the
scripts and the tests call, and hand core these implementations plus the store. Not a
``sys.modules`` shim: the old functions take the application's types (the P3d-3 pattern).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from viva_api.analysis.models import ProducerRef
from viva_api.common.storage import data_layout
from viva_api.simulation.dataset_store import SmsDatasetStore, owner_ref
from viva_core.datasets.models import DatasetRecord, JsonDict, OwnerRef
from viva_core.datasets.walk import Artifact, WalkResult, split_s3_uri
from viva_core.datasets.walk import reconcile as _reconcile
from viva_core.datasets.walk import register_bundle as _register_bundle

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import Simulation
    from viva_core.storage.file_service import FileService, ListingItem

__all__ = [
    "FIGURE_SUFFIXES",
    "PTOOLS_DIR",
    "REPORT_NAME",
    "SCALES",
    "VIZ_DIR",
    "ArtifactName",
    "SimulationWalkSource",
    "SmsArtifactClassifier",
    "WalkResult",
    "analyses_root_uri",
    "classify",
    "parse_artifact_name",
    "reconcile_simulation",
    "register_bundle",
    "split_s3_uri",
]

PTOOLS_DIR = "ptools/"
VIZ_DIR = "viz/"
REPORT_NAME = "analysis.json"
FIGURE_SUFFIXES: tuple[str, ...] = (".html", ".svg", ".png")
#: Scale suffixes an analysis name can carry (``ptools_rna_multiseed``).
SCALES: tuple[str, ...] = ("multigeneration", "multidaughter", "multiseed", "multivariant", "multiexperiment", "single")

_INT_KEYS = {"variant": "variant", "seed": "seed", "gen": "generation"}
_SHAPES: dict[frozenset[str], str] = {
    frozenset({"variant", "seed", "generation", "agent"}): "single",
    frozenset({"variant", "seed", "generation", "parent"}): "multidaughter",
    frozenset({"variant", "seed"}): "multigeneration",
    frozenset({"variant"}): "multiseed",
}


# ---------------------------------------------------------------------------
# the naming convention (pure)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactName:
    view: str
    protocol: str | None
    coordinate: JsonDict


def _coordinate_from_group(group: str) -> JsonDict | None:
    coordinate: JsonDict = {}
    if group == "all":
        return coordinate
    for part in group.split("_"):
        key, eq, value = part.partition("=")
        if not eq or not key:
            return None
        if key in _INT_KEYS:
            if not value.lstrip("-").isdigit():
                return None
            coordinate[_INT_KEYS[key]] = int(value)
        else:
            coordinate[key] = value
    return coordinate


def parse_artifact_name(filename: str) -> ArtifactName | None:
    """``view``, ``protocol`` and coordinate from a v2ecoli analysis output name, or ``None``
    when the name does not follow the ``<name>__<group>`` convention."""
    stem = filename.rsplit("/", 1)[-1]
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]
    name, sep, group = stem.partition("__")
    if not sep or not name or not group:
        return None
    coordinate = _coordinate_from_group(group)
    if coordinate is None:
        return None
    view, protocol = name, None
    for scale in SCALES:
        if name.endswith(f"_{scale}"):
            view, protocol = name[: -len(scale) - 1], scale
            break
    if protocol is None:
        protocol = "all" if group == "all" else _SHAPES.get(frozenset(coordinate))
    return ArtifactName(view=view, protocol=protocol, coordinate=coordinate)


def classify(relative: str) -> str | None:
    """The dataset kind of a path relative to a bundle directory, or ``None``."""
    if relative == REPORT_NAME:
        return "report"
    for directory, kind, suffixes in ((PTOOLS_DIR, "ptools-analysis", (".tsv",)), (VIZ_DIR, "figure", FIGURE_SUFFIXES)):
        rest = relative[len(directory) :] if relative.startswith(directory) else None
        if rest and "/" not in rest and rest.lower().endswith(suffixes):
            return kind
    return None


class SmsArtifactClassifier:
    """Core's ``ArtifactClassifier`` for this application's bundles: :func:`classify` says the
    kind, :func:`parse_artifact_name` the view, protocol and coordinate (a report has none)."""

    def classify(self, relative: str) -> Artifact | None:
        kind = classify(relative)
        if kind is None:
            return None
        parsed = parse_artifact_name(relative.rsplit("/", 1)[-1]) if kind != "report" else None
        if parsed is None:
            return Artifact(kind=kind)
        return Artifact(kind=kind, view=parsed.view, protocol=parsed.protocol, coordinate=parsed.coordinate)


# ---------------------------------------------------------------------------
# what is walked
# ---------------------------------------------------------------------------


def analyses_root_uri(simulation: Simulation) -> str:
    """``<out_uri>/analyses`` from the simulation's emitter config, else the Ray layout."""
    emitter_arg = getattr(simulation.config, "emitter_arg", None)
    out_uri = emitter_arg.get("out_uri") if isinstance(emitter_arg, dict) else None
    if isinstance(out_uri, str) and out_uri.startswith("s3://"):
        return f"{out_uri.rstrip('/')}/analyses"
    return data_layout.s3_uri(f"{data_layout.RayLayout.experiment_prefix(simulation.experiment_id)}/analyses")


class SimulationWalkSource:
    """Core's ``WalkSource`` for one simulation: its ``analyses/`` prefix, the simulation as owner
    and subject, its experiment id as label, its tags on every row; a bundle is claimed by the
    analysis run whose ``result_uri`` is the bundle directory."""

    def __init__(self, simulation: Simulation, db: DatabaseService) -> None:
        self._simulation = simulation
        self._db = db

    @property
    def root_uri(self) -> str:
        return analyses_root_uri(self._simulation)

    @property
    def owner(self) -> OwnerRef:
        return {"owner_kind": "simulation", "owner_id": str(self._simulation.database_id)}

    @property
    def label(self) -> str:
        return self._simulation.experiment_id

    @property
    def tags(self) -> list[str]:
        return list(self._simulation.tags)

    @property
    def subject(self) -> JsonDict:
        return {
            "kind": "simulation",
            "ref": str(self._simulation.database_id),
            "resolved_id": self._simulation.database_id,
        }

    async def claimant(self, bundle_uri: str) -> OwnerRef | None:
        claimed = await self._db.get_analysis_by_result_uri(bundle_uri)
        if claimed is None:
            return None
        return {"owner_kind": "analysis", "owner_id": str(claimed.database_id)}


# ---------------------------------------------------------------------------
# the entry points the scheduler, the scripts and the tests call
# ---------------------------------------------------------------------------


async def register_bundle(
    items: list[ListingItem],
    *,
    bucket: str,
    bundle_key: str,
    producer: ProducerRef,
    simulation: Simulation,
    db: DatabaseService,
    tags: list[str] | None = None,
    existing: Mapping[str, DatasetRecord] | None = None,
) -> WalkResult:
    """Register every consumable file under one bundle directory for ``producer``
    (``{"analysis_id": id}`` or ``{"simulation_id": id}``): core's
    :func:`viva_core.datasets.walk.register_bundle` with this application's classifier, source
    and store handed in."""
    return await _register_bundle(
        items,
        bucket=bucket,
        bundle_key=bundle_key,
        owner=owner_ref(producer),
        source=SimulationWalkSource(simulation, db),
        classifier=SmsArtifactClassifier(),
        store=SmsDatasetStore(db),
        tags=tags,
        existing=existing,
    )


async def reconcile_simulation(
    simulation: Simulation,
    *,
    db: DatabaseService,
    file_service: FileService,
    storage_bucket: str | None,
) -> WalkResult:
    """Walk one simulation's ``analyses/`` prefix: core's :func:`viva_core.datasets.walk.reconcile`
    over :class:`SimulationWalkSource`."""
    return await _reconcile(
        SimulationWalkSource(simulation, db),
        classifier=SmsArtifactClassifier(),
        store=SmsDatasetStore(db),
        file_service=file_service,
        storage_bucket=storage_bucket,
    )
