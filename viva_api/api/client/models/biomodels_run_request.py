from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.biomodel_simulator import BiomodelSimulator
from ..types import UNSET, Unset

T = TypeVar("T", bound="BiomodelsRunRequest")


@_attrs_define
class BiomodelsRunRequest:
    """
    Attributes:
        model_ids (Union[None, Unset, list[str]]): Specific BioModel IDs to run. Mutually exclusive with n_models.
        n_models (Union[None, Unset, int]): Run the first N BioModels. Ignored if model_ids is set.
        simulator (Union[Unset, BiomodelSimulator]):
    """

    model_ids: Union[None, Unset, list[str]] = UNSET
    n_models: Union[None, Unset, int] = UNSET
    simulator: Union[Unset, BiomodelSimulator] = UNSET
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

        simulator: Union[Unset, str] = UNSET
        if not isinstance(self.simulator, Unset):
            simulator = self.simulator.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model_ids is not UNSET:
            field_dict["model_ids"] = model_ids
        if n_models is not UNSET:
            field_dict["n_models"] = n_models
        if simulator is not UNSET:
            field_dict["simulator"] = simulator

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

        _simulator = d.pop("simulator", UNSET)
        simulator: Union[Unset, BiomodelSimulator]
        if isinstance(_simulator, Unset):
            simulator = UNSET
        else:
            simulator = BiomodelSimulator(_simulator)

        biomodels_run_request = cls(
            model_ids=model_ids,
            n_models=n_models,
            simulator=simulator,
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
