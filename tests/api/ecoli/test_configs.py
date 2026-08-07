import json
from pathlib import Path
from typing import Any

import pytest

from viva_api.analysis.models import AnalysisConfig
from viva_api.simulation.models import SimulationConfig

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "configs"


def _is_analysis_config(data: dict[str, Any]) -> bool:
    """Determine if JSON data represents an AnalysisConfig vs SimulationConfig.

    AnalysisConfig has analysis_options at top level with experiment_id as a list inside.
    SimulationConfig has experiment_id at top level as a string.
    """
    return "analysis_options" in data and "experiment_id" not in data


def _round_trip_config(file_path: Path) -> None:
    """Load a config file, parse it, serialize back, and verify equivalence."""
    with open(file_path) as f:
        original_data = json.load(f)

    if _is_analysis_config(original_data):
        analysis_config = AnalysisConfig(**original_data)
        round_tripped = json.loads(analysis_config.model_dump_json(exclude_none=True))
        assert "analysis_options" in round_tripped
    else:
        sim_config = SimulationConfig(**original_data)
        round_tripped = json.loads(sim_config.model_dump_json(exclude_none=True))
        assert round_tripped.get("experiment_id") == original_data.get("experiment_id")


@pytest.mark.parametrize("config_file", list(FIXTURES_DIR.glob("*.json")))
def test_config_round_trip(config_file: Path) -> None:
    """Test that all config files in fixtures/configs can be parsed and round-tripped."""
    _round_trip_config(config_file)


@pytest.mark.asyncio
async def test_analysis_config() -> None:
    conf_path = FIXTURES_DIR / "sms_multigen_analysis.json"
    conf = AnalysisConfig.from_file(fp=conf_path)
    assert conf.analysis_options.experiment_id is not None
    # assert len(conf.emitter_arg["out_dir"])


def test_load_simulation_config() -> None:
    """Test loading a simulation config from an existing asset file."""
    with open(FIXTURES_DIR / "sms_single_cell.json") as f:
        config = SimulationConfig(**json.load(f))

    # Verify key fields are loaded correctly
    assert config.experiment_id == "sms_api_single"
    assert config.generations == 1
    # assert config.n_init_sims == 1
    # assert config.emitter == "parquet"
    # assert config.parca_options.cpus == 3
