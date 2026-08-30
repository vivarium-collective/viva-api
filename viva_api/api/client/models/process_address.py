from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.process_address_config_type_0 import ProcessAddressConfigType0


T = TypeVar("T", bound="ProcessAddress")


@_attrs_define
class ProcessAddress:
    """
    Attributes:
        address (str): Registry address of a Process or Step
        config (Union['ProcessAddressConfigType0', None, Unset]):
    """

    address: str
    config: Union["ProcessAddressConfigType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.process_address_config_type_0 import ProcessAddressConfigType0

        address = self.address

        config: Union[None, Unset, dict[str, Any]]
        if isinstance(self.config, Unset):
            config = UNSET
        elif isinstance(self.config, ProcessAddressConfigType0):
            config = self.config.to_dict()
        else:
            config = self.config

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "address": address,
        })
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.process_address_config_type_0 import ProcessAddressConfigType0

        d = dict(src_dict)
        address = d.pop("address")

        def _parse_config(data: object) -> Union["ProcessAddressConfigType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_0 = ProcessAddressConfigType0.from_dict(data)

                return config_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ProcessAddressConfigType0", None, Unset], data)

        config = _parse_config(d.pop("config", UNSET))

        process_address = cls(
            address=address,
            config=config,
        )

        process_address.additional_properties = d
        return process_address

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
