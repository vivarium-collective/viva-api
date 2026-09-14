from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AnalysisFigureFile")


@_attrs_define
class AnalysisFigureFile:
    """One rendered artifact under an analysis's ``viz/`` or ``ptools/`` subdir.

    ``path`` is relative to ``<out_uri>/analyses/<name>/`` (e.g.
    ``"viz/chromosome_state_view__variant=0_seed=0_gen=0_agent=0.html"``), so it
    round-trips straight back to the fetch endpoint's ``path`` query param.

        Attributes:
            path (str):
            size (int):
    """

    path: str
    size: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        path = self.path

        size = self.size

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "path": path,
            "size": size,
        })

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        path = d.pop("path")

        size = d.pop("size")

        analysis_figure_file = cls(
            path=path,
            size=size,
        )

        analysis_figure_file.additional_properties = d
        return analysis_figure_file

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
