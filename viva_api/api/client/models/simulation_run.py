from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.job_status import JobStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="SimulationRun")


@_attrs_define
class SimulationRun:
    """What ``GET /simulations/{id}/status`` answers.

    The first three fields are the vocabulary every client has relied on since
    the endpoint existed. The rest (observability plan D4d) are additive and
    optional: they are ``None`` until the scheduler has folded the run's event
    stream / trace into the row, so an old client keeps reading the same
    three fields and a new one gets the stage the run is in.

        Attributes:
            id (int):
            status (JobStatus): Shared job status enum for simulations, analyses, and other HPC jobs.
            error_message (Union[None, Unset, str]):
            stage (Union[None, Unset, str]):
            generation (Union[None, Unset, int]):
            last_event_at (Union[None, Unset, str]):
            attempt (Union[None, Unset, int]):
            exit_code (Union[None, Unset, int]):
            error_source (Union[None, Unset, str]):
            trace_id (Union[None, Unset, str]):
            open_spans (Union[None, Unset, list[str]]):
    """

    id: int
    status: JobStatus
    error_message: Union[None, Unset, str] = UNSET
    stage: Union[None, Unset, str] = UNSET
    generation: Union[None, Unset, int] = UNSET
    last_event_at: Union[None, Unset, str] = UNSET
    attempt: Union[None, Unset, int] = UNSET
    exit_code: Union[None, Unset, int] = UNSET
    error_source: Union[None, Unset, str] = UNSET
    trace_id: Union[None, Unset, str] = UNSET
    open_spans: Union[None, Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        status = self.status.value

        error_message: Union[None, Unset, str]
        if isinstance(self.error_message, Unset):
            error_message = UNSET
        else:
            error_message = self.error_message

        stage: Union[None, Unset, str]
        if isinstance(self.stage, Unset):
            stage = UNSET
        else:
            stage = self.stage

        generation: Union[None, Unset, int]
        if isinstance(self.generation, Unset):
            generation = UNSET
        else:
            generation = self.generation

        last_event_at: Union[None, Unset, str]
        if isinstance(self.last_event_at, Unset):
            last_event_at = UNSET
        else:
            last_event_at = self.last_event_at

        attempt: Union[None, Unset, int]
        if isinstance(self.attempt, Unset):
            attempt = UNSET
        else:
            attempt = self.attempt

        exit_code: Union[None, Unset, int]
        if isinstance(self.exit_code, Unset):
            exit_code = UNSET
        else:
            exit_code = self.exit_code

        error_source: Union[None, Unset, str]
        if isinstance(self.error_source, Unset):
            error_source = UNSET
        else:
            error_source = self.error_source

        trace_id: Union[None, Unset, str]
        if isinstance(self.trace_id, Unset):
            trace_id = UNSET
        else:
            trace_id = self.trace_id

        open_spans: Union[None, Unset, list[str]]
        if isinstance(self.open_spans, Unset):
            open_spans = UNSET
        elif isinstance(self.open_spans, list):
            open_spans = self.open_spans

        else:
            open_spans = self.open_spans

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "id": id,
            "status": status,
        })
        if error_message is not UNSET:
            field_dict["error_message"] = error_message
        if stage is not UNSET:
            field_dict["stage"] = stage
        if generation is not UNSET:
            field_dict["generation"] = generation
        if last_event_at is not UNSET:
            field_dict["last_event_at"] = last_event_at
        if attempt is not UNSET:
            field_dict["attempt"] = attempt
        if exit_code is not UNSET:
            field_dict["exit_code"] = exit_code
        if error_source is not UNSET:
            field_dict["error_source"] = error_source
        if trace_id is not UNSET:
            field_dict["trace_id"] = trace_id
        if open_spans is not UNSET:
            field_dict["open_spans"] = open_spans

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        status = JobStatus(d.pop("status"))

        def _parse_error_message(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        error_message = _parse_error_message(d.pop("error_message", UNSET))

        def _parse_stage(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        stage = _parse_stage(d.pop("stage", UNSET))

        def _parse_generation(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation = _parse_generation(d.pop("generation", UNSET))

        def _parse_last_event_at(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        last_event_at = _parse_last_event_at(d.pop("last_event_at", UNSET))

        def _parse_attempt(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        attempt = _parse_attempt(d.pop("attempt", UNSET))

        def _parse_exit_code(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        exit_code = _parse_exit_code(d.pop("exit_code", UNSET))

        def _parse_error_source(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        error_source = _parse_error_source(d.pop("error_source", UNSET))

        def _parse_trace_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        trace_id = _parse_trace_id(d.pop("trace_id", UNSET))

        def _parse_open_spans(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                open_spans_type_0 = cast(list[str], data)

                return open_spans_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        open_spans = _parse_open_spans(d.pop("open_spans", UNSET))

        simulation_run = cls(
            id=id,
            status=status,
            error_message=error_message,
            stage=stage,
            generation=generation,
            last_event_at=last_event_at,
            attempt=attempt,
            exit_code=exit_code,
            error_source=error_source,
            trace_id=trace_id,
            open_spans=open_spans,
        )

        simulation_run.additional_properties = d
        return simulation_run

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
