from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.biomodel_simulator import BiomodelSimulator
from ..types import UNSET, Unset

T = TypeVar("T", bound="BiomodelsRunRequest")


@_attrs_define
class BiomodelsRunRequest:
    """One request shape for every BioModels run — subsumes the former
    single/batch/audit/regression endpoints. Cardinality comes from
    ``model_ids``/``n_models``; per-model cross-validation comes from listing
    more than one simulator.

        Attributes:
            model_ids (Union[None, Unset, list[str]]): Specific BioModel IDs to run. Mutually exclusive with n_models.
            n_models (Union[None, Unset, int]): Run the first N BioModels. Ignored if model_ids is set.
            simulators (Union[Unset, list[BiomodelSimulator]]): Simulators to run for each model. A single simulator runs
                the model on it; several are wired into one PB document per model for cross-validation (the former
                'audit'/'regression' behavior).
    """

    model_ids: Union[None, Unset, list[str]] = UNSET
    n_models: Union[None, Unset, int] = UNSET
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_ids: Union[None, Unset, list[str]]
        if isinstance(self.model_ids, Unset):
            model_ids = UNSET
        elif isinstance(self.model_ids, list):
            model_ids = self.model_ids

        else:
            model_ids = self.model_ids

        n_models: Union[None, Unset, int]
        if isinstance(self.n_models, Unset):
            n_models = UNSET
        else:
            n_models = self.n_models

        simulators: Union[Unset, list[str]] = UNSET
        if not isinstance(self.simulators, Unset):
            simulators = []
            for simulators_item_data in self.simulators:
                simulators_item = simulators_item_data.value
                simulators.append(simulators_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model_ids is not UNSET:
            field_dict["model_ids"] = model_ids
        if n_models is not UNSET:
            field_dict["n_models"] = n_models
        if simulators is not UNSET:
            field_dict["simulators"] = simulators

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_model_ids(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                model_ids_type_0 = cast(list[str], data)

                return model_ids_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        model_ids = _parse_model_ids(d.pop("model_ids", UNSET))

        def _parse_n_models(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        n_models = _parse_n_models(d.pop("n_models", UNSET))

        simulators = []
        _simulators = d.pop("simulators", UNSET)
        for simulators_item_data in _simulators or []:
            simulators_item = BiomodelSimulator(simulators_item_data)

            simulators.append(simulators_item)

        biomodels_run_request = cls(
            model_ids=model_ids,
            n_models=n_models,
            simulators=simulators,
        )

        biomodels_run_request.additional_properties = d
        return biomodels_run_request

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
