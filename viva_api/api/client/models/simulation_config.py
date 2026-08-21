from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analysis_options import AnalysisOptions
    from ..models.parca_options import ParcaOptions


T = TypeVar("T", bound="SimulationConfig")


@_attrs_define
class SimulationConfig:
    """
    Attributes:
        experiment_id (str):
        parca_options (Union[Unset, ParcaOptions]):
        analysis_options (Union[Unset, AnalysisOptions]):
        ecoli_sources_uri (Union[None, Unset, str]):
        ecoli_sources_overlays (Union[None, Unset, str]):
        generations (Union[Unset, int]):  Default: 1.
    """

    experiment_id: str
    parca_options: Union[Unset, "ParcaOptions"] = UNSET
    analysis_options: Union[Unset, "AnalysisOptions"] = UNSET
    ecoli_sources_uri: Union[None, Unset, str] = UNSET
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET
    generations: Union[Unset, int] = 1
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        experiment_id = self.experiment_id

        parca_options: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.parca_options, Unset):
            parca_options = self.parca_options.to_dict()

        analysis_options: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.analysis_options, Unset):
            analysis_options = self.analysis_options.to_dict()

        ecoli_sources_uri: Union[None, Unset, str]
        if isinstance(self.ecoli_sources_uri, Unset):
            ecoli_sources_uri = UNSET
        else:
            ecoli_sources_uri = self.ecoli_sources_uri

        ecoli_sources_overlays: Union[None, Unset, str]
        if isinstance(self.ecoli_sources_overlays, Unset):
            ecoli_sources_overlays = UNSET
        else:
            ecoli_sources_overlays = self.ecoli_sources_overlays

        generations = self.generations

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "experiment_id": experiment_id,
            }
        )
        if parca_options is not UNSET:
            field_dict["parca_options"] = parca_options
        if analysis_options is not UNSET:
            field_dict["analysis_options"] = analysis_options
        if ecoli_sources_uri is not UNSET:
            field_dict["ecoli_sources_uri"] = ecoli_sources_uri
        if ecoli_sources_overlays is not UNSET:
            field_dict["ecoli_sources_overlays"] = ecoli_sources_overlays
        if generations is not UNSET:
            field_dict["generations"] = generations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analysis_options import AnalysisOptions
        from ..models.parca_options import ParcaOptions

        d = dict(src_dict)
        experiment_id = d.pop("experiment_id")

        _parca_options = d.pop("parca_options", UNSET)
        parca_options: Union[Unset, ParcaOptions]
        if isinstance(_parca_options, Unset):
            parca_options = UNSET
        else:
            parca_options = ParcaOptions.from_dict(_parca_options)

        _analysis_options = d.pop("analysis_options", UNSET)
        analysis_options: Union[Unset, AnalysisOptions]
        if isinstance(_analysis_options, Unset):
            analysis_options = UNSET
        else:
            analysis_options = AnalysisOptions.from_dict(_analysis_options)

        def _parse_ecoli_sources_uri(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        ecoli_sources_uri = _parse_ecoli_sources_uri(d.pop("ecoli_sources_uri", UNSET))

        def _parse_ecoli_sources_overlays(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        ecoli_sources_overlays = _parse_ecoli_sources_overlays(d.pop("ecoli_sources_overlays", UNSET))

        generations = d.pop("generations", UNSET)

        simulation_config = cls(
            experiment_id=experiment_id,
            parca_options=parca_options,
            analysis_options=analysis_options,
            ecoli_sources_uri=ecoli_sources_uri,
            ecoli_sources_overlays=ecoli_sources_overlays,
            generations=generations,
        )

        simulation_config.additional_properties = d
        return simulation_config

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
