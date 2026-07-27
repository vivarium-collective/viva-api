from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.biomodel_simulator import BiomodelSimulator

if TYPE_CHECKING:
    from ..models.compose_simulation_experiment import ComposeSimulationExperiment


T = TypeVar("T", bound="BiomodelsAuditResult")


@_attrs_define
class BiomodelsAuditResult:
    """
    Attributes:
        experiment (ComposeSimulationExperiment):
        simulators_used (list[BiomodelSimulator]):
    """

    experiment: "ComposeSimulationExperiment"
    simulators_used: list[BiomodelSimulator]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        experiment = self.experiment.to_dict()

        simulators_used = []
        for simulators_used_item_data in self.simulators_used:
            simulators_used_item = simulators_used_item_data.value
            simulators_used.append(simulators_used_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "experiment": experiment,
                "simulators_used": simulators_used,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.compose_simulation_experiment import ComposeSimulationExperiment

        d = dict(src_dict)
        experiment = ComposeSimulationExperiment.from_dict(d.pop("experiment"))

        simulators_used = []
        _simulators_used = d.pop("simulators_used")
        for simulators_used_item_data in _simulators_used:
            simulators_used_item = BiomodelSimulator(simulators_used_item_data)

            simulators_used.append(simulators_used_item)

        biomodels_audit_result = cls(
            experiment=experiment,
            simulators_used=simulators_used,
        )

        biomodels_audit_result.additional_properties = d
        return biomodels_audit_result

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
