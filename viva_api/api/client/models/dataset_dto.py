from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dataset_dto_attributes import DatasetDTOAttributes
    from ..models.dataset_dto_source_type_0 import DatasetDTOSourceType0


T = TypeVar("T", bound="DatasetDTO")


@_attrs_define
class DatasetDTO:
    """One consumable file set a run actually wrote (a ``dataset`` row).

    Never pre-created: it exists only once a run's trace was scraped or the walk found
    the object. Exactly one producer id is set by the registry. ``available`` is false
    once the object is gone; the row itself is kept so provenance never dangles.

        Attributes:
            database_id (int):
            kind (str):
            uri (str):
            simulation_id (Union[None, Unset, int]):
            parca_dataset_id (Union[None, Unset, int]):
            analysis_id (Union[None, Unset, int]):
            view (Union[None, Unset, str]):
            display_name (Union[None, Unset, str]):
            size_bytes (Union[None, Unset, int]):
            sha256 (Union[None, Unset, str]):
            attributes (Union[Unset, DatasetDTOAttributes]):
            tags (Union[Unset, list[str]]):
            source (Union['DatasetDTOSourceType0', None, Unset]):
            available (Union[Unset, bool]):  Default: True.
            created_at (Union[None, Unset, str]):
            updated_at (Union[None, Unset, str]):
    """

    database_id: int
    kind: str
    uri: str
    simulation_id: Union[None, Unset, int] = UNSET
    parca_dataset_id: Union[None, Unset, int] = UNSET
    analysis_id: Union[None, Unset, int] = UNSET
    view: Union[None, Unset, str] = UNSET
    display_name: Union[None, Unset, str] = UNSET
    size_bytes: Union[None, Unset, int] = UNSET
    sha256: Union[None, Unset, str] = UNSET
    attributes: Union[Unset, "DatasetDTOAttributes"] = UNSET
    tags: Union[Unset, list[str]] = UNSET
    source: Union["DatasetDTOSourceType0", None, Unset] = UNSET
    available: Union[Unset, bool] = True
    created_at: Union[None, Unset, str] = UNSET
    updated_at: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.dataset_dto_source_type_0 import DatasetDTOSourceType0

        database_id = self.database_id

        kind = self.kind

        uri = self.uri

        simulation_id: Union[None, Unset, int]
        if isinstance(self.simulation_id, Unset):
            simulation_id = UNSET
        else:
            simulation_id = self.simulation_id

        parca_dataset_id: Union[None, Unset, int]
        if isinstance(self.parca_dataset_id, Unset):
            parca_dataset_id = UNSET
        else:
            parca_dataset_id = self.parca_dataset_id

        analysis_id: Union[None, Unset, int]
        if isinstance(self.analysis_id, Unset):
            analysis_id = UNSET
        else:
            analysis_id = self.analysis_id

        view: Union[None, Unset, str]
        if isinstance(self.view, Unset):
            view = UNSET
        else:
            view = self.view

        display_name: Union[None, Unset, str]
        if isinstance(self.display_name, Unset):
            display_name = UNSET
        else:
            display_name = self.display_name

        size_bytes: Union[None, Unset, int]
        if isinstance(self.size_bytes, Unset):
            size_bytes = UNSET
        else:
            size_bytes = self.size_bytes

        sha256: Union[None, Unset, str]
        if isinstance(self.sha256, Unset):
            sha256 = UNSET
        else:
            sha256 = self.sha256

        attributes: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.attributes, Unset):
            attributes = self.attributes.to_dict()

        tags: Union[Unset, list[str]] = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        source: Union[None, Unset, dict[str, Any]]
        if isinstance(self.source, Unset):
            source = UNSET
        elif isinstance(self.source, DatasetDTOSourceType0):
            source = self.source.to_dict()
        else:
            source = self.source

        available = self.available

        created_at: Union[None, Unset, str]
        if isinstance(self.created_at, Unset):
            created_at = UNSET
        else:
            created_at = self.created_at

        updated_at: Union[None, Unset, str]
        if isinstance(self.updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "database_id": database_id,
            "kind": kind,
            "uri": uri,
        })
        if simulation_id is not UNSET:
            field_dict["simulation_id"] = simulation_id
        if parca_dataset_id is not UNSET:
            field_dict["parca_dataset_id"] = parca_dataset_id
        if analysis_id is not UNSET:
            field_dict["analysis_id"] = analysis_id
        if view is not UNSET:
            field_dict["view"] = view
        if display_name is not UNSET:
            field_dict["display_name"] = display_name
        if size_bytes is not UNSET:
            field_dict["size_bytes"] = size_bytes
        if sha256 is not UNSET:
            field_dict["sha256"] = sha256
        if attributes is not UNSET:
            field_dict["attributes"] = attributes
        if tags is not UNSET:
            field_dict["tags"] = tags
        if source is not UNSET:
            field_dict["source"] = source
        if available is not UNSET:
            field_dict["available"] = available
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dataset_dto_attributes import DatasetDTOAttributes
        from ..models.dataset_dto_source_type_0 import DatasetDTOSourceType0

        d = dict(src_dict)
        database_id = d.pop("database_id")

        kind = d.pop("kind")

        uri = d.pop("uri")

        def _parse_simulation_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        simulation_id = _parse_simulation_id(d.pop("simulation_id", UNSET))

        def _parse_parca_dataset_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        parca_dataset_id = _parse_parca_dataset_id(d.pop("parca_dataset_id", UNSET))

        def _parse_analysis_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        analysis_id = _parse_analysis_id(d.pop("analysis_id", UNSET))

        def _parse_view(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        view = _parse_view(d.pop("view", UNSET))

        def _parse_display_name(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        display_name = _parse_display_name(d.pop("display_name", UNSET))

        def _parse_size_bytes(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        size_bytes = _parse_size_bytes(d.pop("size_bytes", UNSET))

        def _parse_sha256(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        sha256 = _parse_sha256(d.pop("sha256", UNSET))

        _attributes = d.pop("attributes", UNSET)
        attributes: Union[Unset, DatasetDTOAttributes]
        if isinstance(_attributes, Unset):
            attributes = UNSET
        else:
            attributes = DatasetDTOAttributes.from_dict(_attributes)

        tags = cast(list[str], d.pop("tags", UNSET))

        def _parse_source(data: object) -> Union["DatasetDTOSourceType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                source_type_0 = DatasetDTOSourceType0.from_dict(data)

                return source_type_0
            except:  # noqa: E722
                pass
            return cast(Union["DatasetDTOSourceType0", None, Unset], data)

        source = _parse_source(d.pop("source", UNSET))

        available = d.pop("available", UNSET)

        def _parse_created_at(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        created_at = _parse_created_at(d.pop("created_at", UNSET))

        def _parse_updated_at(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        updated_at = _parse_updated_at(d.pop("updated_at", UNSET))

        dataset_dto = cls(
            database_id=database_id,
            kind=kind,
            uri=uri,
            simulation_id=simulation_id,
            parca_dataset_id=parca_dataset_id,
            analysis_id=analysis_id,
            view=view,
            display_name=display_name,
            size_bytes=size_bytes,
            sha256=sha256,
            attributes=attributes,
            tags=tags,
            source=source,
            available=available,
            created_at=created_at,
            updated_at=updated_at,
        )

        dataset_dto.additional_properties = d
        return dataset_dto

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
