from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OutputFileMetadata")


@_attrs_define
class OutputFileMetadata:
    """
    Attributes:
        filename (str):
        variant (Union[Unset, int]):  Default: 0.
        lineage_seed (Union[None, Unset, int]):
        generation (Union[None, Unset, int]):
        agent_id (Union[None, Unset, str]):
        content (Union[None, Unset, str]):
    """

    filename: str
    variant: Union[Unset, int] = 0
    lineage_seed: Union[None, Unset, int] = UNSET
    generation: Union[None, Unset, int] = UNSET
    agent_id: Union[None, Unset, str] = UNSET
    content: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        filename = self.filename

        variant = self.variant

        lineage_seed: Union[None, Unset, int]
        if isinstance(self.lineage_seed, Unset):
            lineage_seed = UNSET
        else:
            lineage_seed = self.lineage_seed

        generation: Union[None, Unset, int]
        if isinstance(self.generation, Unset):
            generation = UNSET
        else:
            generation = self.generation

        agent_id: Union[None, Unset, str]
        if isinstance(self.agent_id, Unset):
            agent_id = UNSET
        else:
            agent_id = self.agent_id

        content: Union[None, Unset, str]
        if isinstance(self.content, Unset):
            content = UNSET
        else:
            content = self.content

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "filename": filename,
            }
        )
        if variant is not UNSET:
            field_dict["variant"] = variant
        if lineage_seed is not UNSET:
            field_dict["lineage_seed"] = lineage_seed
        if generation is not UNSET:
            field_dict["generation"] = generation
        if agent_id is not UNSET:
            field_dict["agent_id"] = agent_id
        if content is not UNSET:
            field_dict["content"] = content

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        filename = d.pop("filename")

        variant = d.pop("variant", UNSET)

        def _parse_lineage_seed(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        lineage_seed = _parse_lineage_seed(d.pop("lineage_seed", UNSET))

        def _parse_generation(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation = _parse_generation(d.pop("generation", UNSET))

        def _parse_agent_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        agent_id = _parse_agent_id(d.pop("agent_id", UNSET))

        def _parse_content(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        content = _parse_content(d.pop("content", UNSET))

        output_file_metadata = cls(
            filename=filename,
            variant=variant,
            lineage_seed=lineage_seed,
            generation=generation,
            agent_id=agent_id,
            content=content,
        )

        output_file_metadata.additional_properties = d
        return output_file_metadata

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
