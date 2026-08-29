from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.task_submit_request_params_type_0 import TaskSubmitRequestParamsType0


T = TypeVar("T", bound="TaskSubmitRequest")


@_attrs_define
class TaskSubmitRequest:
    """
    Attributes:
        job_name (str): Relayed worker Job to run this on
        method (str): Worker method (JSON-RPC)
        params (Union['TaskSubmitRequestParamsType0', None, Unset]): Method params
    """

    job_name: str
    method: str
    params: Union["TaskSubmitRequestParamsType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.task_submit_request_params_type_0 import TaskSubmitRequestParamsType0

        job_name = self.job_name

        method = self.method

        params: Union[None, Unset, dict[str, Any]]
        if isinstance(self.params, Unset):
            params = UNSET
        elif isinstance(self.params, TaskSubmitRequestParamsType0):
            params = self.params.to_dict()
        else:
            params = self.params

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "job_name": job_name,
            "method": method,
        })
        if params is not UNSET:
            field_dict["params"] = params

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.task_submit_request_params_type_0 import TaskSubmitRequestParamsType0

        d = dict(src_dict)
        job_name = d.pop("job_name")

        method = d.pop("method")

        def _parse_params(data: object) -> Union["TaskSubmitRequestParamsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                params_type_0 = TaskSubmitRequestParamsType0.from_dict(data)

                return params_type_0
            except:  # noqa: E722
                pass
            return cast(Union["TaskSubmitRequestParamsType0", None, Unset], data)

        params = _parse_params(d.pop("params", UNSET))

        task_submit_request = cls(
            job_name=job_name,
            method=method,
            params=params,
        )

        task_submit_request.additional_properties = d
        return task_submit_request

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
