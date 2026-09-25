from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.core_health_services import CoreHealthServices


T = TypeVar("T", bound="CoreHealth")


@_attrs_define
class CoreHealth:
    """
    Attributes:
        services (CoreHealthServices):
        status (Union[Literal['ok'], Unset]):  Default: 'ok'.
    """

    services: "CoreHealthServices"
    status: Union[Literal["ok"], Unset] = "ok"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        services = self.services.to_dict()

        status = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "services": services,
        })
        if status is not UNSET:
            field_dict["status"] = status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.core_health_services import CoreHealthServices

        d = dict(src_dict)
        services = CoreHealthServices.from_dict(d.pop("services"))

        status = cast(Union[Literal["ok"], Unset], d.pop("status", UNSET))
        if status != "ok" and not isinstance(status, Unset):
            raise ValueError(f"status must match const 'ok', got '{status}'")

        core_health = cls(
            services=services,
            status=status,
        )

        core_health.additional_properties = d
        return core_health

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
