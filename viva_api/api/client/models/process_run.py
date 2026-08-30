from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.process_run_config_type_0 import ProcessRunConfigType0
    from ..models.process_run_inputs_type_0 import ProcessRunInputsType0


T = TypeVar("T", bound="ProcessRun")


@_attrs_define
class ProcessRun:
    """One `update()` -- a probe, not a job. `env_worker._run_process` is
    deliberately NOT job-class: it builds one class, fills its ports and runs a
    single step, which is the Composite Explorer's "try this process" button.

        Attributes:
            address (str): Registry address of a Process or Step
            config (Union['ProcessRunConfigType0', None, Unset]):
            inputs (Union['ProcessRunInputsType0', None, Unset]):
            interval (Union[None, Unset, float]):
    """

    address: str
    config: Union["ProcessRunConfigType0", None, Unset] = UNSET
    inputs: Union["ProcessRunInputsType0", None, Unset] = UNSET
    interval: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.process_run_config_type_0 import ProcessRunConfigType0
        from ..models.process_run_inputs_type_0 import ProcessRunInputsType0

        address = self.address

        config: Union[None, Unset, dict[str, Any]]
        if isinstance(self.config, Unset):
            config = UNSET
        elif isinstance(self.config, ProcessRunConfigType0):
            config = self.config.to_dict()
        else:
            config = self.config

        inputs: Union[None, Unset, dict[str, Any]]
        if isinstance(self.inputs, Unset):
            inputs = UNSET
        elif isinstance(self.inputs, ProcessRunInputsType0):
            inputs = self.inputs.to_dict()
        else:
            inputs = self.inputs

        interval: Union[None, Unset, float]
        if isinstance(self.interval, Unset):
            interval = UNSET
        else:
            interval = self.interval

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "address": address,
        })
        if config is not UNSET:
            field_dict["config"] = config
        if inputs is not UNSET:
            field_dict["inputs"] = inputs
        if interval is not UNSET:
            field_dict["interval"] = interval

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.process_run_config_type_0 import ProcessRunConfigType0
        from ..models.process_run_inputs_type_0 import ProcessRunInputsType0

        d = dict(src_dict)
        address = d.pop("address")

        def _parse_config(data: object) -> Union["ProcessRunConfigType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_0 = ProcessRunConfigType0.from_dict(data)

                return config_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ProcessRunConfigType0", None, Unset], data)

        config = _parse_config(d.pop("config", UNSET))

        def _parse_inputs(data: object) -> Union["ProcessRunInputsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                inputs_type_0 = ProcessRunInputsType0.from_dict(data)

                return inputs_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ProcessRunInputsType0", None, Unset], data)

        inputs = _parse_inputs(d.pop("inputs", UNSET))

        def _parse_interval(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        interval = _parse_interval(d.pop("interval", UNSET))

        process_run = cls(
            address=address,
            config=config,
            inputs=inputs,
            interval=interval,
        )

        process_run.additional_properties = d
        return process_run

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
