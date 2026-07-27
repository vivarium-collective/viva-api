from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ObservableInfoModel")


@_attrs_define
class ObservableInfoModel:
    """
    Attributes:
        name (str):
        dims (list[str]):
        shape (list[int]):
    """

    name: str
    dims: list[str]
    shape: list[int]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        dims = self.dims

        shape = self.shape

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "dims": dims,
                "shape": shape,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        dims = cast(list[str], d.pop("dims"))

        shape = cast(list[int], d.pop("shape"))

        observable_info_model = cls(
            name=name,
            dims=dims,
            shape=shape,
        )

        observable_info_model.additional_properties = d
        return observable_info_model

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
