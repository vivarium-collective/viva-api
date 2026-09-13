from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SimulationTask")


@_attrs_define
class SimulationTask:
    """One unit of work of a run as the backend saw it (``GET /simulations/{id}/tasks``):
    a Nextflow task (one ``trace.csv`` row: ``native_id`` = the Batch job id) or a
    chain-dispatch seed job (one Batch job).

        Attributes:
            name (str):
            status (str):
            job_id (Union[None, Unset, str]):
            task_hash (Union[None, Unset, str]):
            exit_code (Union[None, Unset, int]):
            attempt (Union[None, Unset, int]):
            status_reason (Union[None, Unset, str]):
    """

    name: str
    status: str
    job_id: Union[None, Unset, str] = UNSET
    task_hash: Union[None, Unset, str] = UNSET
    exit_code: Union[None, Unset, int] = UNSET
    attempt: Union[None, Unset, int] = UNSET
    status_reason: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        status = self.status

        job_id: Union[None, Unset, str]
        if isinstance(self.job_id, Unset):
            job_id = UNSET
        else:
            job_id = self.job_id

        task_hash: Union[None, Unset, str]
        if isinstance(self.task_hash, Unset):
            task_hash = UNSET
        else:
            task_hash = self.task_hash

        exit_code: Union[None, Unset, int]
        if isinstance(self.exit_code, Unset):
            exit_code = UNSET
        else:
            exit_code = self.exit_code

        attempt: Union[None, Unset, int]
        if isinstance(self.attempt, Unset):
            attempt = UNSET
        else:
            attempt = self.attempt

        status_reason: Union[None, Unset, str]
        if isinstance(self.status_reason, Unset):
            status_reason = UNSET
        else:
            status_reason = self.status_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "name": name,
            "status": status,
        })
        if job_id is not UNSET:
            field_dict["job_id"] = job_id
        if task_hash is not UNSET:
            field_dict["task_hash"] = task_hash
        if exit_code is not UNSET:
            field_dict["exit_code"] = exit_code
        if attempt is not UNSET:
            field_dict["attempt"] = attempt
        if status_reason is not UNSET:
            field_dict["status_reason"] = status_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        status = d.pop("status")

        def _parse_job_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        job_id = _parse_job_id(d.pop("job_id", UNSET))

        def _parse_task_hash(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        task_hash = _parse_task_hash(d.pop("task_hash", UNSET))

        def _parse_exit_code(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        exit_code = _parse_exit_code(d.pop("exit_code", UNSET))

        def _parse_attempt(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        attempt = _parse_attempt(d.pop("attempt", UNSET))

        def _parse_status_reason(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        status_reason = _parse_status_reason(d.pop("status_reason", UNSET))

        simulation_task = cls(
            name=name,
            status=status,
            job_id=job_id,
            task_hash=task_hash,
            exit_code=exit_code,
            attempt=attempt,
            status_reason=status_reason,
        )

        simulation_task.additional_properties = d
        return simulation_task

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
