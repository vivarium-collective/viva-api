from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.output_file_metadata import OutputFileMetadata


T = TypeVar("T", bound="AnalysisModuleConfig")


@_attrs_define
class AnalysisModuleConfig:
    """
    Attributes:
        name (str):
        files (Union[None, Unset, list['OutputFileMetadata']]):
    """

    name: str
    files: Union[None, Unset, list["OutputFileMetadata"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        files: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.files, Unset):
            files = UNSET
        elif isinstance(self.files, list):
            files = []
            for files_type_0_item_data in self.files:
                files_type_0_item = files_type_0_item_data.to_dict()
                files.append(files_type_0_item)

        else:
            files = self.files

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "name": name,
        })
        if files is not UNSET:
            field_dict["files"] = files

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.output_file_metadata import OutputFileMetadata

        d = dict(src_dict)
        name = d.pop("name")

        def _parse_files(data: object) -> Union[None, Unset, list["OutputFileMetadata"]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                files_type_0 = []
                _files_type_0 = data
                for files_type_0_item_data in _files_type_0:
                    files_type_0_item = OutputFileMetadata.from_dict(files_type_0_item_data)

                    files_type_0.append(files_type_0_item)

                return files_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["OutputFileMetadata"]], data)

        files = _parse_files(d.pop("files", UNSET))

        analysis_module_config = cls(
            name=name,
            files=files,
        )

        analysis_module_config.additional_properties = d
        return analysis_module_config

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
