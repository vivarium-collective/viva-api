"""BioModels integration for the compose subsystem.

Fetches SBML/SED-ML files from the EBI BioModels database, parses
UniformTimeCourse specs, and submits them through the existing
Copasi/Tellurium curated simulation pipeline.
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import libsedml

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UniformTimeCourseSpec:
    initial_time: float
    output_start_time: float
    output_end_time: float
    number_of_points: int

    @property
    def duration(self) -> float:
        return float(self.output_end_time - self.output_start_time)


@dataclass(frozen=True)
class BiomodelLoadResult:
    biomodel_id: str
    sbml_path: str
    sedml_path: str
    utc: UniformTimeCourseSpec


# ---------------------------------------------------------------------------
# File-type helpers (ported from biomodels-regression)
# ---------------------------------------------------------------------------

_SBML_RE = re.compile(r"\.(xml|sbml)$", re.IGNORECASE)
_SEDML_RE = re.compile(r"\.sedml$", re.IGNORECASE)


def _file_name(obj: Any) -> str:
    return getattr(obj, "name", str(obj))


def _iter_entry_files(entry: Any) -> list[Any]:
    if entry is None:
        return []
    if isinstance(entry, list | tuple):
        return list(entry)
    if isinstance(entry, dict):
        for key in ("files", "main_files", "model_files"):
            v = entry.get(key)
            if isinstance(v, list | tuple):
                return list(v)
        return []
    try:
        return list(entry)
    except TypeError:
        return []


def _find_first_sedml(entry_files: list[Any]) -> Any | None:
    for f in entry_files:
        if _SEDML_RE.search(_file_name(f)):
            return f
    return None


def _find_first_sbml(entry_files: list[Any]) -> Any | None:
    candidates = []
    for f in entry_files:
        name = _file_name(f)
        if _SEDML_RE.search(name):
            continue
        if _SBML_RE.search(name):
            candidates.append(f)
    for key in ("sbml", "model"):
        for c in candidates:
            if key in _file_name(c).lower():
                return c
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# SED-ML parsing (ported from biomodels-regression)
# ---------------------------------------------------------------------------


def _read_sedml_doc(sedml_path: str) -> libsedml.SedDocument:
    doc = libsedml.readSedMLFromFile(str(sedml_path))
    if doc is None:
        raise RuntimeError(f"libsedml returned None reading: {sedml_path}")
    return doc


def _extract_first_utc(sed_doc: libsedml.SedDocument) -> UniformTimeCourseSpec:
    for i in range(int(sed_doc.getNumSimulations())):
        sim = sed_doc.getSimulation(i)
        if sim is None:
            continue
        needed = ("getInitialTime", "getOutputStartTime", "getOutputEndTime", "getNumberOfPoints")
        if not all(hasattr(sim, m) for m in needed):
            continue
        return UniformTimeCourseSpec(
            initial_time=float(sim.getInitialTime()),
            output_start_time=float(sim.getOutputStartTime()),
            output_end_time=float(sim.getOutputEndTime()),
            number_of_points=int(sim.getNumberOfPoints()),
        )
    raise ValueError("No UniformTimeCourse simulation found in SED-ML.")


def _resolve_sbml_source(sed_doc: libsedml.SedDocument, sedml_dir: str, fallback: str) -> str:
    if sed_doc.getNumModels() == 0:
        return fallback
    model = sed_doc.getModel(0)
    if model is None:
        return fallback
    src = model.getSource()
    if not src or src.startswith(("http://", "https://", "urn:", "biomodels:", "BIOMD")):
        return fallback
    candidate = os.path.abspath(os.path.join(sedml_dir, src))
    return candidate if os.path.exists(candidate) else fallback


# ---------------------------------------------------------------------------
# BiomodelsService
# ---------------------------------------------------------------------------


class BiomodelsService:
    """Thin wrapper over the `biomodels` Python client for the compose subsystem."""

    @staticmethod
    def get_identifiers(n: int | None = None) -> list[str]:
        """Return BioModels identifiers. Optionally limited to the first *n*."""
        import biomodels

        ids: list[str] = biomodels.get_all_identifiers()
        return ids[:n] if n is not None else ids

    @staticmethod
    def get_metadata(biomodel_id: str) -> dict[str, Any]:
        """Return raw metadata dict for a single BioModel."""
        import biomodels

        meta = biomodels.get_metadata(biomodel_id)
        if hasattr(meta, "dict"):
            return meta.dict()  # type: ignore[no-any-return]
        if hasattr(meta, "model_dump"):
            return meta.model_dump()  # type: ignore[no-any-return]
        if isinstance(meta, dict):
            return meta
        return {"biomodel_id": biomodel_id, "raw": str(meta)}

    @staticmethod
    def load_biomodel(biomodel_id: str, stable_dir: Path) -> BiomodelLoadResult:
        """Fetch a BioModel from EBI, parse its SED-ML, and return paths + UTC spec.

        Files are written to *stable_dir* so callers can read them after this
        method returns (unlike the internal tempdir which is cleaned up here).
        """
        import biomodels

        meta = biomodels.get_metadata(biomodel_id)
        entry_files = _iter_entry_files(meta)

        sedml_entry = _find_first_sedml(entry_files)
        sbml_entry = _find_first_sbml(entry_files)

        if sedml_entry is None:
            raise ValueError(f"{biomodel_id}: no SED-ML file found in entry")
        if sbml_entry is None:
            raise ValueError(f"{biomodel_id}: no SBML file found in entry")

        stable_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=f"biomodel_{biomodel_id}_") as tmp:
            # Fetch files
            def _fetch(entry: Any, out_dir: str) -> str:
                f = biomodels.get_file(entry)
                name = _file_name(entry)
                out_path = os.path.join(out_dir, name)
                if isinstance(f, str | os.PathLike) and os.path.exists(str(f)):
                    return str(f)
                if isinstance(f, bytes):
                    Path(out_path).write_bytes(f)
                else:
                    Path(out_path).write_text(str(f), encoding="utf-8")
                return out_path

            sedml_path = _fetch(sedml_entry, tmp)
            sbml_path = _fetch(sbml_entry, tmp)

            sed_doc = _read_sedml_doc(sedml_path)
            utc = _extract_first_utc(sed_doc)
            resolved_sbml = _resolve_sbml_source(sed_doc, os.path.dirname(sedml_path), sbml_path)

            stable_sedml = stable_dir / os.path.basename(sedml_path)
            stable_sbml = stable_dir / os.path.basename(resolved_sbml)

            stable_sedml.write_bytes(Path(sedml_path).read_bytes())
            stable_sbml.write_bytes(Path(resolved_sbml).read_bytes())

        return BiomodelLoadResult(
            biomodel_id=biomodel_id,
            sbml_path=str(stable_sbml),
            sedml_path=str(stable_sedml),
            utc=utc,
        )
