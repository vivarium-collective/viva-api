from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analysis_module_config import AnalysisModuleConfig
    from ..models.ptools_analysis_config import PtoolsAnalysisConfig


T = TypeVar("T", bound="ExperimentAnalysisRequest")


@_attrs_define
class ExperimentAnalysisRequest:
    """Request body for the ``POST /analyses`` (ptools) endpoint.

    Top-level ``generation_start``, ``generation_end``, and ``seeds`` are
    fully supported for ``single`` analyses — they restrict which simulation
    data rows are returned, with metadata identifying each partition.

    For aggregated types (``multigeneration``, ``multiseed``), the filters are
    passed to vEcoli but not currently applied to the per-subset data query
    (known vEcoli limitation).  Use ``single`` with filters and aggregate
    client-side as a workaround.

    Per-module params (``n_tp``, ``time_unit``, …) are set inside each
    ``PtoolsAnalysisConfig`` entry and only affect the module they belong to.

        Attributes:
            experiment_id (str):
            generation_start (Union[None, Unset, int]):
            generation_end (Union[None, Unset, int]):
            seeds (Union[None, Unset, list[int]]):
            single (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
            multidaughter (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
            multigeneration (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
            multiseed (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
            multivariant (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
            multiexperiment (Union[None, Unset, list[Union['AnalysisModuleConfig', 'PtoolsAnalysisConfig']]]):
    """

    experiment_id: str
    generation_start: Union[None, Unset, int] = UNSET
    generation_end: Union[None, Unset, int] = UNSET
    seeds: Union[None, Unset, list[int]] = UNSET
    single: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    multidaughter: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    multigeneration: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    multiseed: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    multivariant: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    multiexperiment: Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.analysis_module_config import AnalysisModuleConfig

        experiment_id = self.experiment_id

        generation_start: Union[None, Unset, int]
        if isinstance(self.generation_start, Unset):
            generation_start = UNSET
        else:
            generation_start = self.generation_start

        generation_end: Union[None, Unset, int]
        if isinstance(self.generation_end, Unset):
            generation_end = UNSET
        else:
            generation_end = self.generation_end

        seeds: Union[None, Unset, list[int]]
        if isinstance(self.seeds, Unset):
            seeds = UNSET
        elif isinstance(self.seeds, list):
            seeds = self.seeds

        else:
            seeds = self.seeds

        single: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.single, Unset):
            single = UNSET
        elif isinstance(self.single, list):
            single = []
            for single_type_0_item_data in self.single:
                single_type_0_item: dict[str, Any]
                if isinstance(single_type_0_item_data, AnalysisModuleConfig):
                    single_type_0_item = single_type_0_item_data.to_dict()
                else:
                    single_type_0_item = single_type_0_item_data.to_dict()

                single.append(single_type_0_item)

        else:
            single = self.single

        multidaughter: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.multidaughter, Unset):
            multidaughter = UNSET
        elif isinstance(self.multidaughter, list):
            multidaughter = []
            for multidaughter_type_0_item_data in self.multidaughter:
                multidaughter_type_0_item: dict[str, Any]
                if isinstance(multidaughter_type_0_item_data, AnalysisModuleConfig):
                    multidaughter_type_0_item = multidaughter_type_0_item_data.to_dict()
                else:
                    multidaughter_type_0_item = multidaughter_type_0_item_data.to_dict()

                multidaughter.append(multidaughter_type_0_item)

        else:
            multidaughter = self.multidaughter

        multigeneration: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.multigeneration, Unset):
            multigeneration = UNSET
        elif isinstance(self.multigeneration, list):
            multigeneration = []
            for multigeneration_type_0_item_data in self.multigeneration:
                multigeneration_type_0_item: dict[str, Any]
                if isinstance(multigeneration_type_0_item_data, AnalysisModuleConfig):
                    multigeneration_type_0_item = multigeneration_type_0_item_data.to_dict()
                else:
                    multigeneration_type_0_item = multigeneration_type_0_item_data.to_dict()

                multigeneration.append(multigeneration_type_0_item)

        else:
            multigeneration = self.multigeneration

        multiseed: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.multiseed, Unset):
            multiseed = UNSET
        elif isinstance(self.multiseed, list):
            multiseed = []
            for multiseed_type_0_item_data in self.multiseed:
                multiseed_type_0_item: dict[str, Any]
                if isinstance(multiseed_type_0_item_data, AnalysisModuleConfig):
                    multiseed_type_0_item = multiseed_type_0_item_data.to_dict()
                else:
                    multiseed_type_0_item = multiseed_type_0_item_data.to_dict()

                multiseed.append(multiseed_type_0_item)

        else:
            multiseed = self.multiseed

        multivariant: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.multivariant, Unset):
            multivariant = UNSET
        elif isinstance(self.multivariant, list):
            multivariant = []
            for multivariant_type_0_item_data in self.multivariant:
                multivariant_type_0_item: dict[str, Any]
                if isinstance(multivariant_type_0_item_data, AnalysisModuleConfig):
                    multivariant_type_0_item = multivariant_type_0_item_data.to_dict()
                else:
                    multivariant_type_0_item = multivariant_type_0_item_data.to_dict()

                multivariant.append(multivariant_type_0_item)

        else:
            multivariant = self.multivariant

        multiexperiment: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.multiexperiment, Unset):
            multiexperiment = UNSET
        elif isinstance(self.multiexperiment, list):
            multiexperiment = []
            for multiexperiment_type_0_item_data in self.multiexperiment:
                multiexperiment_type_0_item: dict[str, Any]
                if isinstance(multiexperiment_type_0_item_data, AnalysisModuleConfig):
                    multiexperiment_type_0_item = multiexperiment_type_0_item_data.to_dict()
                else:
                    multiexperiment_type_0_item = multiexperiment_type_0_item_data.to_dict()

                multiexperiment.append(multiexperiment_type_0_item)

        else:
            multiexperiment = self.multiexperiment

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "experiment_id": experiment_id,
            }
        )
        if generation_start is not UNSET:
            field_dict["generation_start"] = generation_start
        if generation_end is not UNSET:
            field_dict["generation_end"] = generation_end
        if seeds is not UNSET:
            field_dict["seeds"] = seeds
        if single is not UNSET:
            field_dict["single"] = single
        if multidaughter is not UNSET:
            field_dict["multidaughter"] = multidaughter
        if multigeneration is not UNSET:
            field_dict["multigeneration"] = multigeneration
        if multiseed is not UNSET:
            field_dict["multiseed"] = multiseed
        if multivariant is not UNSET:
            field_dict["multivariant"] = multivariant
        if multiexperiment is not UNSET:
            field_dict["multiexperiment"] = multiexperiment

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analysis_module_config import AnalysisModuleConfig
        from ..models.ptools_analysis_config import PtoolsAnalysisConfig

        d = dict(src_dict)
        experiment_id = d.pop("experiment_id")

        def _parse_generation_start(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation_start = _parse_generation_start(d.pop("generation_start", UNSET))

        def _parse_generation_end(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation_end = _parse_generation_end(d.pop("generation_end", UNSET))

        def _parse_seeds(data: object) -> Union[None, Unset, list[int]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                seeds_type_0 = cast(list[int], data)

                return seeds_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[int]], data)

        seeds = _parse_seeds(d.pop("seeds", UNSET))

        def _parse_single(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                single_type_0 = []
                _single_type_0 = data
                for single_type_0_item_data in _single_type_0:

                    def _parse_single_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            single_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return single_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        single_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return single_type_0_item_type_1

                    single_type_0_item = _parse_single_type_0_item(single_type_0_item_data)

                    single_type_0.append(single_type_0_item)

                return single_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        single = _parse_single(d.pop("single", UNSET))

        def _parse_multidaughter(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                multidaughter_type_0 = []
                _multidaughter_type_0 = data
                for multidaughter_type_0_item_data in _multidaughter_type_0:

                    def _parse_multidaughter_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            multidaughter_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return multidaughter_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        multidaughter_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return multidaughter_type_0_item_type_1

                    multidaughter_type_0_item = _parse_multidaughter_type_0_item(multidaughter_type_0_item_data)

                    multidaughter_type_0.append(multidaughter_type_0_item)

                return multidaughter_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        multidaughter = _parse_multidaughter(d.pop("multidaughter", UNSET))

        def _parse_multigeneration(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                multigeneration_type_0 = []
                _multigeneration_type_0 = data
                for multigeneration_type_0_item_data in _multigeneration_type_0:

                    def _parse_multigeneration_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            multigeneration_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return multigeneration_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        multigeneration_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return multigeneration_type_0_item_type_1

                    multigeneration_type_0_item = _parse_multigeneration_type_0_item(multigeneration_type_0_item_data)

                    multigeneration_type_0.append(multigeneration_type_0_item)

                return multigeneration_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        multigeneration = _parse_multigeneration(d.pop("multigeneration", UNSET))

        def _parse_multiseed(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                multiseed_type_0 = []
                _multiseed_type_0 = data
                for multiseed_type_0_item_data in _multiseed_type_0:

                    def _parse_multiseed_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            multiseed_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return multiseed_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        multiseed_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return multiseed_type_0_item_type_1

                    multiseed_type_0_item = _parse_multiseed_type_0_item(multiseed_type_0_item_data)

                    multiseed_type_0.append(multiseed_type_0_item)

                return multiseed_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        multiseed = _parse_multiseed(d.pop("multiseed", UNSET))

        def _parse_multivariant(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                multivariant_type_0 = []
                _multivariant_type_0 = data
                for multivariant_type_0_item_data in _multivariant_type_0:

                    def _parse_multivariant_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            multivariant_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return multivariant_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        multivariant_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return multivariant_type_0_item_type_1

                    multivariant_type_0_item = _parse_multivariant_type_0_item(multivariant_type_0_item_data)

                    multivariant_type_0.append(multivariant_type_0_item)

                return multivariant_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        multivariant = _parse_multivariant(d.pop("multivariant", UNSET))

        def _parse_multiexperiment(
            data: object,
        ) -> Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                multiexperiment_type_0 = []
                _multiexperiment_type_0 = data
                for multiexperiment_type_0_item_data in _multiexperiment_type_0:

                    def _parse_multiexperiment_type_0_item(
                        data: object,
                    ) -> Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            multiexperiment_type_0_item_type_0 = AnalysisModuleConfig.from_dict(data)

                            return multiexperiment_type_0_item_type_0
                        except:  # noqa: E722
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        multiexperiment_type_0_item_type_1 = PtoolsAnalysisConfig.from_dict(data)

                        return multiexperiment_type_0_item_type_1

                    multiexperiment_type_0_item = _parse_multiexperiment_type_0_item(multiexperiment_type_0_item_data)

                    multiexperiment_type_0.append(multiexperiment_type_0_item)

                return multiexperiment_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union["AnalysisModuleConfig", "PtoolsAnalysisConfig"]]], data)

        multiexperiment = _parse_multiexperiment(d.pop("multiexperiment", UNSET))

        experiment_analysis_request = cls(
            experiment_id=experiment_id,
            generation_start=generation_start,
            generation_end=generation_end,
            seeds=seeds,
            single=single,
            multidaughter=multidaughter,
            multigeneration=multigeneration,
            multiseed=multiseed,
            multivariant=multivariant,
            multiexperiment=multiexperiment,
        )

        experiment_analysis_request.additional_properties = d
        return experiment_analysis_request

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
