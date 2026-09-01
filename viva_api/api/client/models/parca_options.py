from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ParcaOptions")


@_attrs_define
class ParcaOptions:
    """
    Attributes:
        outdir (Union[Unset, str]):  Default: '/projects/SMS/sms_api/prod/sims'.
    """

    outdir: Union[Unset, str] = "/projects/SMS/sms_api/prod/sims"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        outdir = self.outdir

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if outdir is not UNSET:
            field_dict["outdir"] = outdir

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        outdir = d.pop("outdir", UNSET)

        parca_options = cls(
            outdir=outdir,
        )

        parca_options.additional_properties = d
        return parca_options

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
