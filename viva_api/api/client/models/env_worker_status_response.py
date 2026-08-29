from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnvWorkerStatusResponse")


@_attrs_define
class EnvWorkerStatusResponse:
    """
    Attributes:
        job_name (str):
        status (Union[None, Unset, str]):
        exists (Union[Unset, bool]):  Default: True.
        logs (Union[None, Unset, str]):
    """

    job_name: str
    status: Union[None, Unset, str] = UNSET
    exists: Union[Unset, bool] = True
    logs: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_name = self.job_name

        status: Union[None, Unset, str]
        if isinstance(self.status, Unset):
            status = UNSET
        else:
            status = self.status

        exists = self.exists

        logs: Union[None, Unset, str]
        if isinstance(self.logs, Unset):
            logs = UNSET
        else:
            logs = self.logs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "job_name": job_name,
        })
        if status is not UNSET:
            field_dict["status"] = status
        if exists is not UNSET:
            field_dict["exists"] = exists
        if logs is not UNSET:
            field_dict["logs"] = logs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        job_name = d.pop("job_name")

        def _parse_status(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        status = _parse_status(d.pop("status", UNSET))

        exists = d.pop("exists", UNSET)

        def _parse_logs(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        logs = _parse_logs(d.pop("logs", UNSET))

        env_worker_status_response = cls(
            job_name=job_name,
            status=status,
            exists=exists,
            logs=logs,
        )

        env_worker_status_response.additional_properties = d
        return env_worker_status_response

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
