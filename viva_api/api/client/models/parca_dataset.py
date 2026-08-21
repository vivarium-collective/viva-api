from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.parca_dataset_request import ParcaDatasetRequest


T = TypeVar("T", bound="ParcaDataset")


@_attrs_define
class ParcaDataset:
    """
    Attributes:
        database_id (int):
        parca_dataset_request (ParcaDatasetRequest):
        remote_archive_path (Union[None, Unset, str]):
    """

    database_id: int
    parca_dataset_request: "ParcaDatasetRequest"
    remote_archive_path: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        parca_dataset_request = self.parca_dataset_request.to_dict()

        remote_archive_path: Union[None, Unset, str]
        if isinstance(self.remote_archive_path, Unset):
            remote_archive_path = UNSET
        else:
            remote_archive_path = self.remote_archive_path

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "database_id": database_id,
            "parca_dataset_request": parca_dataset_request,
        })
        if remote_archive_path is not UNSET:
            field_dict["remote_archive_path"] = remote_archive_path

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.parca_dataset_request import ParcaDatasetRequest

        d = dict(src_dict)
        database_id = d.pop("database_id")

        parca_dataset_request = ParcaDatasetRequest.from_dict(d.pop("parca_dataset_request"))

        def _parse_remote_archive_path(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        remote_archive_path = _parse_remote_archive_path(d.pop("remote_archive_path", UNSET))

        parca_dataset = cls(
            database_id=database_id,
            parca_dataset_request=parca_dataset_request,
            remote_archive_path=remote_archive_path,
        )

        parca_dataset.additional_properties = d
        return parca_dataset

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
