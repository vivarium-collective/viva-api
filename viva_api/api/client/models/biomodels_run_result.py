from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.compose_simulation_experiment import ComposeSimulationExperiment


T = TypeVar("T", bound="BiomodelsRunResult")


@_attrs_define
class BiomodelsRunResult:
    """
    Attributes:
        submitted (list['ComposeSimulationExperiment']):
        failed (Union[Unset, list[str]]): BioModel IDs that failed to submit.
    """

    submitted: list["ComposeSimulationExperiment"]
    failed: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        submitted = []
        for submitted_item_data in self.submitted:
            submitted_item = submitted_item_data.to_dict()
            submitted.append(submitted_item)

        failed: Union[Unset, list[str]] = UNSET
        if not isinstance(self.failed, Unset):
            failed = self.failed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "submitted": submitted,
        })
        if failed is not UNSET:
            field_dict["failed"] = failed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.compose_simulation_experiment import ComposeSimulationExperiment

        d = dict(src_dict)
        submitted = []
        _submitted = d.pop("submitted")
        for submitted_item_data in _submitted:
            submitted_item = ComposeSimulationExperiment.from_dict(submitted_item_data)

            submitted.append(submitted_item)

        failed = cast(list[str], d.pop("failed", UNSET))

        biomodels_run_result = cls(
            submitted=submitted,
            failed=failed,
        )

        biomodels_run_result.additional_properties = d
        return biomodels_run_result

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
