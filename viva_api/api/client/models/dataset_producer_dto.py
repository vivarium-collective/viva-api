from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dataset_producer_dto_source_type_0 import DatasetProducerDTOSourceType0


T = TypeVar("T", bound="DatasetProducerDTO")


@_attrs_define
class DatasetProducerDTO:
    """The run that wrote a dataset. ``kind`` is ``simulation``, ``analysis`` or ``parca``;
    ``hpcrun_id``/``trace_id``/``correlation_id`` are set when the run has a run row (a
    walk-created analysis has none).

        Attributes:
            kind (str):
            id (int):
            name (Union[None, Unset, str]):
            status (Union[None, Unset, str]):
            source (Union['DatasetProducerDTOSourceType0', None, Unset]):
            tags (Union[Unset, list[str]]):
            hpcrun_id (Union[None, Unset, int]):
            trace_id (Union[None, Unset, str]):
            correlation_id (Union[None, Unset, str]):
    """

    kind: str
    id: int
    name: Union[None, Unset, str] = UNSET
    status: Union[None, Unset, str] = UNSET
    source: Union["DatasetProducerDTOSourceType0", None, Unset] = UNSET
    tags: Union[Unset, list[str]] = UNSET
    hpcrun_id: Union[None, Unset, int] = UNSET
    trace_id: Union[None, Unset, str] = UNSET
    correlation_id: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.dataset_producer_dto_source_type_0 import DatasetProducerDTOSourceType0

        kind = self.kind

        id = self.id

        name: Union[None, Unset, str]
        if isinstance(self.name, Unset):
            name = UNSET
        else:
            name = self.name

        status: Union[None, Unset, str]
        if isinstance(self.status, Unset):
            status = UNSET
        else:
            status = self.status

        source: Union[None, Unset, dict[str, Any]]
        if isinstance(self.source, Unset):
            source = UNSET
        elif isinstance(self.source, DatasetProducerDTOSourceType0):
            source = self.source.to_dict()
        else:
            source = self.source

        tags: Union[Unset, list[str]] = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        hpcrun_id: Union[None, Unset, int]
        if isinstance(self.hpcrun_id, Unset):
            hpcrun_id = UNSET
        else:
            hpcrun_id = self.hpcrun_id

        trace_id: Union[None, Unset, str]
        if isinstance(self.trace_id, Unset):
            trace_id = UNSET
        else:
            trace_id = self.trace_id

        correlation_id: Union[None, Unset, str]
        if isinstance(self.correlation_id, Unset):
            correlation_id = UNSET
        else:
            correlation_id = self.correlation_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "kind": kind,
            "id": id,
        })
        if name is not UNSET:
            field_dict["name"] = name
        if status is not UNSET:
            field_dict["status"] = status
        if source is not UNSET:
            field_dict["source"] = source
        if tags is not UNSET:
            field_dict["tags"] = tags
        if hpcrun_id is not UNSET:
            field_dict["hpcrun_id"] = hpcrun_id
        if trace_id is not UNSET:
            field_dict["trace_id"] = trace_id
        if correlation_id is not UNSET:
            field_dict["correlation_id"] = correlation_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dataset_producer_dto_source_type_0 import DatasetProducerDTOSourceType0

        d = dict(src_dict)
        kind = d.pop("kind")

        id = d.pop("id")

        def _parse_name(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        name = _parse_name(d.pop("name", UNSET))

        def _parse_status(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        status = _parse_status(d.pop("status", UNSET))

        def _parse_source(data: object) -> Union["DatasetProducerDTOSourceType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                source_type_0 = DatasetProducerDTOSourceType0.from_dict(data)

                return source_type_0
            except:  # noqa: E722
                pass
            return cast(Union["DatasetProducerDTOSourceType0", None, Unset], data)

        source = _parse_source(d.pop("source", UNSET))

        tags = cast(list[str], d.pop("tags", UNSET))

        def _parse_hpcrun_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        hpcrun_id = _parse_hpcrun_id(d.pop("hpcrun_id", UNSET))

        def _parse_trace_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        trace_id = _parse_trace_id(d.pop("trace_id", UNSET))

        def _parse_correlation_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        correlation_id = _parse_correlation_id(d.pop("correlation_id", UNSET))

        dataset_producer_dto = cls(
            kind=kind,
            id=id,
            name=name,
            status=status,
            source=source,
            tags=tags,
            hpcrun_id=hpcrun_id,
            trace_id=trace_id,
            correlation_id=correlation_id,
        )

        dataset_producer_dto.additional_properties = d
        return dataset_producer_dto

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
