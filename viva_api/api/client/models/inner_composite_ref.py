from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InnerCompositeRef")


@_attrs_define
class InnerCompositeRef:
    """`hops` is a LIST OF NODE PATHS, each itself a list of key segments -- which
    is why this is a POST and not the GET the plan first assumed.

        Attributes:
            ref (str):
            hops (list[list[str]]): One node path per drill level
    """

    ref: str
    hops: list[list[str]]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ref = self.ref

        hops = []
        for hops_item_data in self.hops:
            hops_item = hops_item_data

            hops.append(hops_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "ref": ref,
            "hops": hops,
        })

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ref = d.pop("ref")

        hops = []
        _hops = d.pop("hops")
        for hops_item_data in _hops:
            hops_item = cast(list[str], hops_item_data)

            hops.append(hops_item)

        inner_composite_ref = cls(
            ref=ref,
            hops=hops,
        )

        inner_composite_ref.additional_properties = d
        return inner_composite_ref

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
