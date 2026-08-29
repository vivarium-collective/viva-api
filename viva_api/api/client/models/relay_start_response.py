from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="RelayStartResponse")


@_attrs_define
class RelayStartResponse:
    """
    Attributes:
        job_name (str):
        image (str):
        namespace (str):
        connected (bool):
    """

    job_name: str
    image: str
    namespace: str
    connected: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_name = self.job_name

        image = self.image

        namespace = self.namespace

        connected = self.connected

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "job_name": job_name,
            "image": image,
            "namespace": namespace,
            "connected": connected,
        })

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        job_name = d.pop("job_name")

        image = d.pop("image")

        namespace = d.pop("namespace")

        connected = d.pop("connected")

        relay_start_response = cls(
            job_name=job_name,
            image=image,
            namespace=namespace,
            connected=connected,
        )

        relay_start_response.additional_properties = d
        return relay_start_response

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
