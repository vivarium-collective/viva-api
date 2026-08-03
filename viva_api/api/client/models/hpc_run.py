from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.job_status import JobStatus
from ..models.job_type import JobType
from ..types import UNSET, Unset

T = TypeVar("T", bound="HpcRun")


@_attrs_define
class HpcRun:
    """
    Attributes:
        database_id (int):
        correlation_id (str):
        job_type (JobType):
        ref_id (int):
        job_id_ext (str):
        job_backend (str):
        status (Union[JobStatus, None, Unset]):
        start_time (Union[None, Unset, str]):
        end_time (Union[None, Unset, str]):
        error_message (Union[None, Unset, str]):
    """

    database_id: int
    correlation_id: str
    job_type: JobType
    ref_id: int
    job_id_ext: str
    job_backend: str
    status: Union[JobStatus, None, Unset] = UNSET
    start_time: Union[None, Unset, str] = UNSET
    end_time: Union[None, Unset, str] = UNSET
    error_message: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        correlation_id = self.correlation_id

        job_type = self.job_type.value

        ref_id = self.ref_id

        job_id_ext = self.job_id_ext

        job_backend = self.job_backend

        status: Union[None, Unset, str]
        if isinstance(self.status, Unset):
            status = UNSET
        elif isinstance(self.status, JobStatus):
            status = self.status.value
        else:
            status = self.status

        start_time: Union[None, Unset, str]
        if isinstance(self.start_time, Unset):
            start_time = UNSET
        else:
            start_time = self.start_time

        end_time: Union[None, Unset, str]
        if isinstance(self.end_time, Unset):
            end_time = UNSET
        else:
            end_time = self.end_time

        error_message: Union[None, Unset, str]
        if isinstance(self.error_message, Unset):
            error_message = UNSET
        else:
            error_message = self.error_message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "database_id": database_id,
            "correlation_id": correlation_id,
            "job_type": job_type,
            "ref_id": ref_id,
            "job_id_ext": job_id_ext,
            "job_backend": job_backend,
        })
        if status is not UNSET:
            field_dict["status"] = status
        if start_time is not UNSET:
            field_dict["start_time"] = start_time
        if end_time is not UNSET:
            field_dict["end_time"] = end_time
        if error_message is not UNSET:
            field_dict["error_message"] = error_message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database_id = d.pop("database_id")

        correlation_id = d.pop("correlation_id")

        job_type = JobType(d.pop("job_type"))

        ref_id = d.pop("ref_id")

        job_id_ext = d.pop("job_id_ext")

        job_backend = d.pop("job_backend")

        def _parse_status(data: object) -> Union[JobStatus, None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                status_type_0 = JobStatus(data)

                return status_type_0
            except:  # noqa: E722
                pass
            return cast(Union[JobStatus, None, Unset], data)

        status = _parse_status(d.pop("status", UNSET))

        def _parse_start_time(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        start_time = _parse_start_time(d.pop("start_time", UNSET))

        def _parse_end_time(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        end_time = _parse_end_time(d.pop("end_time", UNSET))

        def _parse_error_message(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        error_message = _parse_error_message(d.pop("error_message", UNSET))

        hpc_run = cls(
            database_id=database_id,
            correlation_id=correlation_id,
            job_type=job_type,
            ref_id=ref_id,
            job_id_ext=job_id_ext,
            job_backend=job_backend,
            status=status,
            start_time=start_time,
            end_time=end_time,
            error_message=error_message,
        )

        hpc_run.additional_properties = d
        return hpc_run

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
