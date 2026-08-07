from viva_api.simulation.models import (
    ObservableInfoModel,
    SimulationObservableIndex,
    SimulationObservables,
)


def test_index_model_roundtrips() -> None:
    idx = SimulationObservableIndex(
        simulation_id=49,
        experiment_id="exp-abc",
        seed=0,
        store="zarr",
        observables=[ObservableInfoModel(name="mass", dims=["time"], shape=[3])],
    )
    dumped = idx.model_dump()
    assert dumped["observables"][0]["name"] == "mass"


def test_observables_model_roundtrips() -> None:
    obs = SimulationObservables(
        simulation_id=49,
        experiment_id="exp-abc",
        seed=0,
        store="zarr",
        time=[0.0, 1.0],
        series={"mass": [1.0, 2.0]},
    )
    assert obs.model_dump()["series"]["mass"] == [1.0, 2.0]
