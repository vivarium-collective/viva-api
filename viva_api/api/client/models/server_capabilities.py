from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ServerCapabilities")


@_attrs_define
class ServerCapabilities:
    """What this running deployment can actually do.

    Attributes:
        version (str): Server version, for humans and bug reports. NOT for feature detection.
        capabilities (list[str]): Stable capability names this deployment can serve right now. Test membership; absence
            means 'not available here'. Unknown names should be ignored.
    """

    version: str
    capabilities: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        capabilities = self.capabilities

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "version": version,
                "capabilities": capabilities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        version = d.pop("version")

        capabilities = cast(list[str], d.pop("capabilities"))

        server_capabilities = cls(
            version=version,
            capabilities=capabilities,
        )

        server_capabilities.additional_properties = d
        return server_capabilities

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
