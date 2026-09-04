import json
from pathlib import Path
from typing import Any

import pydantic
import pytest

from viva_api.common.models import JobBackend
from viva_api.simulation.models import (
    BaseModel,
    HpcRun,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    trim_attributes,
)


@pytest.mark.asyncio
async def test_serialize_sim_config() -> None:
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "configs"
    with open(fixtures_dir / "sms_single_cell.json") as f:
        simulation_config_raw = json.load(f)
    config = SimulationConfig(**simulation_config_raw)
    serialized = config.model_dump_json()
    assert isinstance(serialized, str)
    assert isinstance(json.loads(serialized), dict)
    # Verify round-trip preserves key fields
    deserialized = json.loads(serialized)
    assert deserialized["experiment_id"] == simulation_config_raw["experiment_id"]


class TestParcaOptionsNewGenes:
    """P0-2: new_genes must be a REAL field that survives model construction
    (previously commented out → pydantic default extra="ignore" stripped it, so a
    custom strain silently reached ParCa as wild-type), and an unknown parca field
    must fail loud (extra="forbid") instead of being silently dropped."""

    def test_new_genes_survives_construction(self) -> None:
        opts = ParcaOptions(new_genes="violacein")
        assert opts.new_genes == "violacein"
        assert opts.model_dump()["new_genes"] == "violacein"

    def test_new_genes_survives_through_a_full_simulation_config(self) -> None:
        config = SimulationConfig(experiment_id="exp-strain", parca_options={"new_genes": "violacein"})  # type: ignore[arg-type]
        assert config.parca_options.new_genes == "violacein"

    def test_new_genes_defaults_to_off(self) -> None:
        assert ParcaOptions().new_genes == "off"

    def test_unknown_parca_field_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            ParcaOptions(definitely_not_a_real_parca_option=True)  # type: ignore[call-arg]

    def test_every_field_the_default_template_carries_is_accepted(self) -> None:
        """The embedded default template + real config fixtures collectively name
        the full vEcoli ParcaOptions set — forbid must accept all of them."""
        full = {
            "cpus": 6,
            "outdir": "/out",
            "operons": True,
            "ribosome_fitting": True,
            "rnapoly_fitting": True,
            "remove_rrna_operons": False,
            "remove_rrff": False,
            "stable_rrna": False,
            "new_genes": "off",
            "debug_parca": False,
            "load_intermediate": None,
            "save_intermediates": False,
            "intermediates_directory": "",
            "variable_elongation_transcription": True,
            "variable_elongation_translation": False,
        }
        opts = ParcaOptions(**full)  # type: ignore[arg-type]
        assert opts.new_genes == "off"


@pytest.mark.asyncio
async def test_trim_attributes() -> None:
    class A(BaseModel):
        x: float
        k: float | None = None
        args: list[float] | None = None

        def model_post_init(self, context: Any, /) -> None:
            trim_attributes(self)

    a = A(x=11.11)
    assert a.model_dump() == {"x": 11.11}


def test_hpc_run_parses_modern_api_response() -> None:
    """The current server serialization uses ``job_id_ext`` + ``job_backend``."""
    payload: dict[str, Any] = {
        "database_id": 7,
        "job_id_ext": "1881684",
        "job_backend": "slurm",
        "correlation_id": "N/A",
        "job_type": "build_image",
        "ref_id": 37,
        "status": "completed",
        "start_time": "2026-04-10 11:11:58",
        "end_time": "2026-04-10 11:16:27",
        "error_message": None,
    }
    hr = HpcRun.model_validate(payload)
    assert hr.database_id == 7
    assert hr.job_id.value == "1881684"
    assert hr.job_id.backend is JobBackend.SLURM


def test_hpc_run_parses_legacy_slurmjobid() -> None:
    """The CLI must remain compatible with older CCAM deployments that still
    serialize ``slurmjobid`` as a bare int (pre-JobBackend release).

    This is the exact payload observed against ``https://sms.cam.uchc.edu`` at
    the time the task-7a compatibility fix landed.
    """
    payload: dict[str, Any] = {
        "database_id": 51,
        "slurmjobid": 1881684,
        "correlation_id": "N/A",
        "job_type": "build_image",
        "ref_id": 37,
        "status": "completed",
        "start_time": "2026-04-10 11:11:58",
        "end_time": "2026-04-10 11:16:27",
        "error_message": None,
    }
    hr = HpcRun.model_validate(payload)
    assert hr.database_id == 51
    assert hr.job_id.value == "1881684"
    assert hr.job_id.backend is JobBackend.SLURM
    assert hr.status is not None and hr.status.value == "completed"


class TestSimulationNumSeeds:
    """Verify that Simulation.num_seeds is derived from config.n_init_sims."""

    def test_num_seeds_from_config(self) -> None:
        config = SimulationConfig(experiment_id="test", generations=5, n_init_sims=10)  # type: ignore[call-arg]
        sim = Simulation(
            database_id=1,
            simulator_id=1,
            parca_dataset_id=1,
            config=config,
            simulation_config_filename="test.json",
            experiment_id="test",
        )
        assert sim.num_seeds == 10

    def test_num_seeds_none_when_not_in_config(self) -> None:
        config = SimulationConfig(experiment_id="test", generations=5)
        sim = Simulation(
            database_id=1,
            simulator_id=1,
            parca_dataset_id=1,
            config=config,
            simulation_config_filename="test.json",
            experiment_id="test",
        )
        assert sim.num_seeds is None

    def test_num_seeds_explicit_override(self) -> None:
        config = SimulationConfig(experiment_id="test", generations=5, n_init_sims=3)  # type: ignore[call-arg]
        sim = Simulation(
            database_id=1,
            simulator_id=1,
            parca_dataset_id=1,
            config=config,
            simulation_config_filename="test.json",
            experiment_id="test",
            num_seeds=7,
        )
        # Explicit num_seeds takes precedence
        assert sim.num_seeds == 7
