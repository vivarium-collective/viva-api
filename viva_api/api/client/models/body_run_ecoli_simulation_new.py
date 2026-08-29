from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analysis_options import AnalysisOptions
    from ..models.body_run_ecoli_simulation_new_extra_params_type_0 import BodyRunEcoliSimulationNewExtraParamsType0


T = TypeVar("T", bound="BodyRunEcoliSimulationNew")


@_attrs_define
class BodyRunEcoliSimulationNew:
    """
    Attributes:
        analysis_options (Union['AnalysisOptions', None, Unset]):
        extra_params (Union['BodyRunEcoliSimulationNewExtraParamsType0', None, Unset]): Additional composite-specific
            parameters not covered by the named params above (e.g. a composite's own
            `injected_processes`/`multi_node_dispatch` knobs). Merged into the resolved config without overriding any of the
            named params — a key here is ignored if the same key is already set by one of them.
    """

    analysis_options: Union["AnalysisOptions", None, Unset] = UNSET
    extra_params: Union["BodyRunEcoliSimulationNewExtraParamsType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.analysis_options import AnalysisOptions
        from ..models.body_run_ecoli_simulation_new_extra_params_type_0 import BodyRunEcoliSimulationNewExtraParamsType0

        analysis_options: Union[None, Unset, dict[str, Any]]
        if isinstance(self.analysis_options, Unset):
            analysis_options = UNSET
        elif isinstance(self.analysis_options, AnalysisOptions):
            analysis_options = self.analysis_options.to_dict()
        else:
            analysis_options = self.analysis_options

        extra_params: Union[None, Unset, dict[str, Any]]
        if isinstance(self.extra_params, Unset):
            extra_params = UNSET
        elif isinstance(self.extra_params, BodyRunEcoliSimulationNewExtraParamsType0):
            extra_params = self.extra_params.to_dict()
        else:
            extra_params = self.extra_params

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if analysis_options is not UNSET:
            field_dict["analysis_options"] = analysis_options
        if extra_params is not UNSET:
            field_dict["extra_params"] = extra_params

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analysis_options import AnalysisOptions
        from ..models.body_run_ecoli_simulation_new_extra_params_type_0 import BodyRunEcoliSimulationNewExtraParamsType0

        d = dict(src_dict)

        def _parse_analysis_options(data: object) -> Union["AnalysisOptions", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                analysis_options_type_0 = AnalysisOptions.from_dict(data)

                return analysis_options_type_0
            except:  # noqa: E722
                pass
            return cast(Union["AnalysisOptions", None, Unset], data)

        analysis_options = _parse_analysis_options(d.pop("analysis_options", UNSET))

        def _parse_extra_params(data: object) -> Union["BodyRunEcoliSimulationNewExtraParamsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                extra_params_type_0 = BodyRunEcoliSimulationNewExtraParamsType0.from_dict(data)

                return extra_params_type_0
            except:  # noqa: E722
                pass
            return cast(Union["BodyRunEcoliSimulationNewExtraParamsType0", None, Unset], data)

        extra_params = _parse_extra_params(d.pop("extra_params", UNSET))

        body_run_ecoli_simulation_new = cls(
            analysis_options=analysis_options,
            extra_params=extra_params,
        )

        body_run_ecoli_simulation_new.additional_properties = d
        return body_run_ecoli_simulation_new

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
