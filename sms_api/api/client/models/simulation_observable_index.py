from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.simulation_observable_index_store import SimulationObservableIndexStore

if TYPE_CHECKING:
    from ..models.observable_info_model import ObservableInfoModel


T = TypeVar("T", bound="SimulationObservableIndex")


@_attrs_define
class SimulationObservableIndex:
    """
    Attributes:
        simulation_id (int):
        experiment_id (str):
        seed (int):
        store (SimulationObservableIndexStore):
        observables (list['ObservableInfoModel']):
    """

    simulation_id: int
    experiment_id: str
    seed: int
    store: SimulationObservableIndexStore
    observables: list["ObservableInfoModel"]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        simulation_id = self.simulation_id

        experiment_id = self.experiment_id

        seed = self.seed

        store = self.store.value

        observables = []
        for observables_item_data in self.observables:
            observables_item = observables_item_data.to_dict()
            observables.append(observables_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "simulation_id": simulation_id,
                "experiment_id": experiment_id,
                "seed": seed,
                "store": store,
                "observables": observables,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.observable_info_model import ObservableInfoModel

        d = dict(src_dict)
        simulation_id = d.pop("simulation_id")

        experiment_id = d.pop("experiment_id")

        seed = d.pop("seed")

        store = SimulationObservableIndexStore(d.pop("store"))

        observables = []
        _observables = d.pop("observables")
        for observables_item_data in _observables:
            observables_item = ObservableInfoModel.from_dict(observables_item_data)

            observables.append(observables_item)

        simulation_observable_index = cls(
            simulation_id=simulation_id,
            experiment_id=experiment_id,
            seed=seed,
            store=store,
            observables=observables,
        )

        simulation_observable_index.additional_properties = d
        return simulation_observable_index

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
