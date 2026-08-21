from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.biomodel_info_metadata import BiomodelInfoMetadata


T = TypeVar("T", bound="BiomodelInfo")


@_attrs_define
class BiomodelInfo:
    """
    Attributes:
        biomodel_id (str):
        metadata (BiomodelInfoMetadata):
    """

    biomodel_id: str
    metadata: "BiomodelInfoMetadata"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        biomodel_id = self.biomodel_id

        metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "biomodel_id": biomodel_id,
            "metadata": metadata,
        })

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.biomodel_info_metadata import BiomodelInfoMetadata

        d = dict(src_dict)
        biomodel_id = d.pop("biomodel_id")

        metadata = BiomodelInfoMetadata.from_dict(d.pop("metadata"))

        biomodel_info = cls(
            biomodel_id=biomodel_id,
            metadata=metadata,
        )

        biomodel_info.additional_properties = d
        return biomodel_info

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
