from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.containerization_engine import ContainerizationEngine
from ..types import UNSET, Unset

T = TypeVar("T", bound="ContainerizationFileRepr")


@_attrs_define
class ContainerizationFileRepr:
    """A textual container-definition file (e.g. a Singularity/apptainer def).

    Attributes:
        representation (str):
        containerization_engine (Union[Unset, ContainerizationEngine]): The container engine a definition targets.
    """

    representation: str
    containerization_engine: Union[Unset, ContainerizationEngine] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        representation = self.representation

        containerization_engine: Union[Unset, int] = UNSET
        if not isinstance(self.containerization_engine, Unset):
            containerization_engine = self.containerization_engine.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "representation": representation,
        })
        if containerization_engine is not UNSET:
            field_dict["containerization_engine"] = containerization_engine

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        representation = d.pop("representation")

        _containerization_engine = d.pop("containerization_engine", UNSET)
        containerization_engine: Union[Unset, ContainerizationEngine]
        if isinstance(_containerization_engine, Unset):
            containerization_engine = UNSET
        else:
            containerization_engine = ContainerizationEngine(_containerization_engine)

        containerization_file_repr = cls(
            representation=representation,
            containerization_engine=containerization_engine,
        )

        containerization_file_repr.additional_properties = d
        return containerization_file_repr

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
