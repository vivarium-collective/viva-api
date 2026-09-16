from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dataset_dto import DatasetDTO


T = TypeVar("T", bound="DatasetListDTO")


@_attrs_define
class DatasetListDTO:
    """A page of datasets. ``next_offset`` is ``None`` on the last page.

    ``total`` is how many rows match the filters, not how many this page holds, so a client can
    say "1-100 of 43,182" and decide whether to narrow instead of paging. It counts with the
    SAME clauses as the page (``count_datasets``), so the two can never disagree. It defaults to
    0 only so a hand-built page in a test need not supply it; every route sets it.

        Attributes:
            datasets (list['DatasetDTO']):
            limit (int):
            offset (int):
            next_offset (Union[None, Unset, int]):
            total (Union[Unset, int]):  Default: 0.
    """

    datasets: list["DatasetDTO"]
    limit: int
    offset: int
    next_offset: Union[None, Unset, int] = UNSET
    total: Union[Unset, int] = 0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        datasets = []
        for datasets_item_data in self.datasets:
            datasets_item = datasets_item_data.to_dict()
            datasets.append(datasets_item)

        limit = self.limit

        offset = self.offset

        next_offset: Union[None, Unset, int]
        if isinstance(self.next_offset, Unset):
            next_offset = UNSET
        else:
            next_offset = self.next_offset

        total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "datasets": datasets,
            "limit": limit,
            "offset": offset,
        })
        if next_offset is not UNSET:
            field_dict["next_offset"] = next_offset
        if total is not UNSET:
            field_dict["total"] = total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dataset_dto import DatasetDTO

        d = dict(src_dict)
        datasets = []
        _datasets = d.pop("datasets")
        for datasets_item_data in _datasets:
            datasets_item = DatasetDTO.from_dict(datasets_item_data)

            datasets.append(datasets_item)

        limit = d.pop("limit")

        offset = d.pop("offset")

        def _parse_next_offset(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        next_offset = _parse_next_offset(d.pop("next_offset", UNSET))

        total = d.pop("total", UNSET)

        dataset_list_dto = cls(
            datasets=datasets,
            limit=limit,
            offset=offset,
            next_offset=next_offset,
            total=total,
        )

        dataset_list_dto.additional_properties = d
        return dataset_list_dto

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
