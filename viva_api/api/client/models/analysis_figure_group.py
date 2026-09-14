from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analysis_figure_file import AnalysisFigureFile


T = TypeVar("T", bound="AnalysisFigureGroup")


@_attrs_define
class AnalysisFigureGroup:
    """A single analysis directory's rendered figures + ptools tables.

    ``source`` distinguishes a row backed by a DB ``analysis`` record
    (``"record"``) from one discovered only by walking S3 (``"s3"``) -- the
    latter is a hand-dispatched ("fill") analysis that never created a record.

        Attributes:
            name (str):
            status (str):
            source (str):
            result_uri (Union[None, Unset, str]):
            figures (Union[Unset, list['AnalysisFigureFile']]):
            ptools (Union[Unset, list['AnalysisFigureFile']]):
    """

    name: str
    status: str
    source: str
    result_uri: Union[None, Unset, str] = UNSET
    figures: Union[Unset, list["AnalysisFigureFile"]] = UNSET
    ptools: Union[Unset, list["AnalysisFigureFile"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        status = self.status

        source = self.source

        result_uri: Union[None, Unset, str]
        if isinstance(self.result_uri, Unset):
            result_uri = UNSET
        else:
            result_uri = self.result_uri

        figures: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.figures, Unset):
            figures = []
            for figures_item_data in self.figures:
                figures_item = figures_item_data.to_dict()
                figures.append(figures_item)

        ptools: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.ptools, Unset):
            ptools = []
            for ptools_item_data in self.ptools:
                ptools_item = ptools_item_data.to_dict()
                ptools.append(ptools_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "name": name,
            "status": status,
            "source": source,
        })
        if result_uri is not UNSET:
            field_dict["result_uri"] = result_uri
        if figures is not UNSET:
            field_dict["figures"] = figures
        if ptools is not UNSET:
            field_dict["ptools"] = ptools

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analysis_figure_file import AnalysisFigureFile

        d = dict(src_dict)
        name = d.pop("name")

        status = d.pop("status")

        source = d.pop("source")

        def _parse_result_uri(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        result_uri = _parse_result_uri(d.pop("result_uri", UNSET))

        figures = []
        _figures = d.pop("figures", UNSET)
        for figures_item_data in _figures or []:
            figures_item = AnalysisFigureFile.from_dict(figures_item_data)

            figures.append(figures_item)

        ptools = []
        _ptools = d.pop("ptools", UNSET)
        for ptools_item_data in _ptools or []:
            ptools_item = AnalysisFigureFile.from_dict(ptools_item_data)

            ptools.append(ptools_item)

        analysis_figure_group = cls(
            name=name,
            status=status,
            source=source,
            result_uri=result_uri,
            figures=figures,
            ptools=ptools,
        )

        analysis_figure_group.additional_properties = d
        return analysis_figure_group

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
