from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnvironmentModel")


@_attrs_define
class EnvironmentModel:
    """What exists: the image a run would pull, and the two identities -- the request it answers
    (``spec_hash``) and what was built (``image_digest``; null until something records it).

        Attributes:
            image (str):
            spec_hash (str):
            image_digest (Union[None, Unset, str]):
    """

    image: str
    spec_hash: str
    image_digest: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        image = self.image

        spec_hash = self.spec_hash

        image_digest: Union[None, Unset, str]
        if isinstance(self.image_digest, Unset):
            image_digest = UNSET
        else:
            image_digest = self.image_digest

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "image": image,
            "spec_hash": spec_hash,
        })
        if image_digest is not UNSET:
            field_dict["image_digest"] = image_digest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        image = d.pop("image")

        spec_hash = d.pop("spec_hash")

        def _parse_image_digest(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        image_digest = _parse_image_digest(d.pop("image_digest", UNSET))

        environment_model = cls(
            image=image,
            spec_hash=spec_hash,
            image_digest=image_digest,
        )

        environment_model.additional_properties = d
        return environment_model

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
