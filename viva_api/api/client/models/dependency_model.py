from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DependencyModel")


@_attrs_define
class DependencyModel:
    """
    Attributes:
        name (str):
        source (Union[Unset, str]):  Default: 'pypi'.
        version (Union[Unset, str]):  Default: ''.
    """

    name: str
    source: Union[Unset, str] = "pypi"
    version: Union[Unset, str] = ""
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        source = self.source

        version = self.version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "name": name,
        })
        if source is not UNSET:
            field_dict["source"] = source
        if version is not UNSET:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        source = d.pop("source", UNSET)

        version = d.pop("version", UNSET)

        dependency_model = cls(
            name=name,
            source=source,
            version=version,
        )

        dependency_model.additional_properties = d
        return dependency_model

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
