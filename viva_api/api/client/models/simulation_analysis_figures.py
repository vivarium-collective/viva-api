from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analysis_figure_group import AnalysisFigureGroup


T = TypeVar("T", bound="SimulationAnalysisFigures")


@_attrs_define
class SimulationAnalysisFigures:
    """Every analysis's rendered figures+ptools for a simulation, unioning DB
    records with an S3-prefix walk so hand-dispatched fills (no DB row) surface.

    ``available`` is False (with a ``reason``) when nothing renderable was found,
    so a caller can fall back rather than show an empty tab.

        Attributes:
            available (bool):
            reason (str):
            analyses (Union[Unset, list['AnalysisFigureGroup']]):
    """

    available: bool
    reason: str
    analyses: Union[Unset, list["AnalysisFigureGroup"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        available = self.available

        reason = self.reason

        analyses: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.analyses, Unset):
            analyses = []
            for analyses_item_data in self.analyses:
                analyses_item = analyses_item_data.to_dict()
                analyses.append(analyses_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "available": available,
            "reason": reason,
        })
        if analyses is not UNSET:
            field_dict["analyses"] = analyses

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analysis_figure_group import AnalysisFigureGroup

        d = dict(src_dict)
        available = d.pop("available")

        reason = d.pop("reason")

        analyses = []
        _analyses = d.pop("analyses", UNSET)
        for analyses_item_data in _analyses or []:
            analyses_item = AnalysisFigureGroup.from_dict(analyses_item_data)

            analyses.append(analyses_item)

        simulation_analysis_figures = cls(
            available=available,
            reason=reason,
            analyses=analyses,
        )

        simulation_analysis_figures.additional_properties = d
        return simulation_analysis_figures

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
