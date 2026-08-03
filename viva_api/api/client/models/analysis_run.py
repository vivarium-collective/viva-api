from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.job_status import JobStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="AnalysisRun")


@_attrs_define
class AnalysisRun:
    """
    Attributes:
        id (int):
        status (JobStatus): Shared job status enum for simulations, analyses, and other HPC jobs.
        job_id (Union[None, Unset, int]):
        error_log (Union[None, Unset, str]):
    """

    id: int
    status: JobStatus
    job_id: Union[None, Unset, int] = UNSET
    error_log: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        status = self.status.value

        job_id: Union[None, Unset, int]
        if isinstance(self.job_id, Unset):
            job_id = UNSET
        else:
            job_id = self.job_id

        error_log: Union[None, Unset, str]
        if isinstance(self.error_log, Unset):
            error_log = UNSET
        else:
            error_log = self.error_log

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "id": id,
            "status": status,
        })
        if job_id is not UNSET:
            field_dict["job_id"] = job_id
        if error_log is not UNSET:
            field_dict["error_log"] = error_log

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        status = JobStatus(d.pop("status"))

        def _parse_job_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        job_id = _parse_job_id(d.pop("job_id", UNSET))

        def _parse_error_log(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        error_log = _parse_error_log(d.pop("error_log", UNSET))

        analysis_run = cls(
            id=id,
            status=status,
            job_id=job_id,
            error_log=error_log,
        )

        analysis_run.additional_properties = d
        return analysis_run

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
