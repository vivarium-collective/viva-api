from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.simulation_observables_store import SimulationObservablesStore

if TYPE_CHECKING:
    from ..models.simulation_observables_series import SimulationObservablesSeries


T = TypeVar("T", bound="SimulationObservables")


@_attrs_define
class SimulationObservables:
    """
    Attributes:
        simulation_id (int):
        experiment_id (str):
        seed (int):
        store (SimulationObservablesStore):
        time (list[float]):
        series (SimulationObservablesSeries):
    """

    simulation_id: int
    experiment_id: str
    seed: int
    store: SimulationObservablesStore
    time: list[float]
    series: "SimulationObservablesSeries"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        simulation_id = self.simulation_id

        experiment_id = self.experiment_id

        seed = self.seed

        store = self.store.value

        time = self.time

        series = self.series.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "simulation_id": simulation_id,
                "experiment_id": experiment_id,
                "seed": seed,
                "store": store,
                "time": time,
                "series": series,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.simulation_observables_series import SimulationObservablesSeries

        d = dict(src_dict)
        simulation_id = d.pop("simulation_id")

        experiment_id = d.pop("experiment_id")

        seed = d.pop("seed")

        store = SimulationObservablesStore(d.pop("store"))

        time = cast(list[float], d.pop("time"))

        series = SimulationObservablesSeries.from_dict(d.pop("series"))

        simulation_observables = cls(
            simulation_id=simulation_id,
            experiment_id=experiment_id,
            seed=seed,
            store=store,
            time=time,
            series=series,
        )

        simulation_observables.additional_properties = d
        return simulation_observables

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
