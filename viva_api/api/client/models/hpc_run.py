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
        chain_n_generations (Union[None, Unset, int]):
        chain_final_job_ids (Union[None, Unset, list[str]]):
        chain_current_job_ids (Union[None, Unset, list[Union[None, str]]]):
        chain_current_generation (Union[None, Unset, list[Union[None, int]]]):
        chain_parca_done (Union[None, Unset, bool]):
        multi_node_composite_id (Union[None, Unset, str]):
        external_job_ids (Union[None, Unset, list[str]]):
        exit_code (Union[None, Unset, int]):
        attempt (Union[None, Unset, int]):
        error_source (Union[None, Unset, str]):
        trace_id (Union[None, Unset, str]):
        campaign_span_id (Union[None, Unset, str]):
        events_s3_prefix (Union[None, Unset, str]):
        stage (Union[None, Unset, str]):
        generation (Union[None, Unset, int]):
        last_event_at (Union[None, Unset, str]):
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
    chain_n_generations: Union[None, Unset, int] = UNSET
    chain_final_job_ids: Union[None, Unset, list[str]] = UNSET
    chain_current_job_ids: Union[None, Unset, list[Union[None, str]]] = UNSET
    chain_current_generation: Union[None, Unset, list[Union[None, int]]] = UNSET
    chain_parca_done: Union[None, Unset, bool] = UNSET
    multi_node_composite_id: Union[None, Unset, str] = UNSET
    external_job_ids: Union[None, Unset, list[str]] = UNSET
    exit_code: Union[None, Unset, int] = UNSET
    attempt: Union[None, Unset, int] = UNSET
    error_source: Union[None, Unset, str] = UNSET
    trace_id: Union[None, Unset, str] = UNSET
    campaign_span_id: Union[None, Unset, str] = UNSET
    events_s3_prefix: Union[None, Unset, str] = UNSET
    stage: Union[None, Unset, str] = UNSET
    generation: Union[None, Unset, int] = UNSET
    last_event_at: Union[None, Unset, str] = UNSET
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

        chain_n_generations: Union[None, Unset, int]
        if isinstance(self.chain_n_generations, Unset):
            chain_n_generations = UNSET
        else:
            chain_n_generations = self.chain_n_generations

        chain_final_job_ids: Union[None, Unset, list[str]]
        if isinstance(self.chain_final_job_ids, Unset):
            chain_final_job_ids = UNSET
        elif isinstance(self.chain_final_job_ids, list):
            chain_final_job_ids = self.chain_final_job_ids

        else:
            chain_final_job_ids = self.chain_final_job_ids

        chain_current_job_ids: Union[None, Unset, list[Union[None, str]]]
        if isinstance(self.chain_current_job_ids, Unset):
            chain_current_job_ids = UNSET
        elif isinstance(self.chain_current_job_ids, list):
            chain_current_job_ids = []
            for chain_current_job_ids_type_0_item_data in self.chain_current_job_ids:
                chain_current_job_ids_type_0_item: Union[None, str]
                chain_current_job_ids_type_0_item = chain_current_job_ids_type_0_item_data
                chain_current_job_ids.append(chain_current_job_ids_type_0_item)

        else:
            chain_current_job_ids = self.chain_current_job_ids

        chain_current_generation: Union[None, Unset, list[Union[None, int]]]
        if isinstance(self.chain_current_generation, Unset):
            chain_current_generation = UNSET
        elif isinstance(self.chain_current_generation, list):
            chain_current_generation = []
            for chain_current_generation_type_0_item_data in self.chain_current_generation:
                chain_current_generation_type_0_item: Union[None, int]
                chain_current_generation_type_0_item = chain_current_generation_type_0_item_data
                chain_current_generation.append(chain_current_generation_type_0_item)

        else:
            chain_current_generation = self.chain_current_generation

        chain_parca_done: Union[None, Unset, bool]
        if isinstance(self.chain_parca_done, Unset):
            chain_parca_done = UNSET
        else:
            chain_parca_done = self.chain_parca_done

        multi_node_composite_id: Union[None, Unset, str]
        if isinstance(self.multi_node_composite_id, Unset):
            multi_node_composite_id = UNSET
        else:
            multi_node_composite_id = self.multi_node_composite_id

        external_job_ids: Union[None, Unset, list[str]]
        if isinstance(self.external_job_ids, Unset):
            external_job_ids = UNSET
        elif isinstance(self.external_job_ids, list):
            external_job_ids = self.external_job_ids

        else:
            external_job_ids = self.external_job_ids

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

        campaign_span_id: Union[None, Unset, str]
        if isinstance(self.campaign_span_id, Unset):
            campaign_span_id = UNSET
        else:
            campaign_span_id = self.campaign_span_id

        events_s3_prefix: Union[None, Unset, str]
        if isinstance(self.events_s3_prefix, Unset):
            events_s3_prefix = UNSET
        else:
            events_s3_prefix = self.events_s3_prefix

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
        if chain_n_generations is not UNSET:
            field_dict["chain_n_generations"] = chain_n_generations
        if chain_final_job_ids is not UNSET:
            field_dict["chain_final_job_ids"] = chain_final_job_ids
        if chain_current_job_ids is not UNSET:
            field_dict["chain_current_job_ids"] = chain_current_job_ids
        if chain_current_generation is not UNSET:
            field_dict["chain_current_generation"] = chain_current_generation
        if chain_parca_done is not UNSET:
            field_dict["chain_parca_done"] = chain_parca_done
        if multi_node_composite_id is not UNSET:
            field_dict["multi_node_composite_id"] = multi_node_composite_id
        if external_job_ids is not UNSET:
            field_dict["external_job_ids"] = external_job_ids
        if exit_code is not UNSET:
            field_dict["exit_code"] = exit_code
        if attempt is not UNSET:
            field_dict["attempt"] = attempt
        if error_source is not UNSET:
            field_dict["error_source"] = error_source
        if trace_id is not UNSET:
            field_dict["trace_id"] = trace_id
        if campaign_span_id is not UNSET:
            field_dict["campaign_span_id"] = campaign_span_id
        if events_s3_prefix is not UNSET:
            field_dict["events_s3_prefix"] = events_s3_prefix
        if stage is not UNSET:
            field_dict["stage"] = stage
        if generation is not UNSET:
            field_dict["generation"] = generation
        if last_event_at is not UNSET:
            field_dict["last_event_at"] = last_event_at

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

        def _parse_chain_n_generations(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        chain_n_generations = _parse_chain_n_generations(d.pop("chain_n_generations", UNSET))

        def _parse_chain_final_job_ids(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                chain_final_job_ids_type_0 = cast(list[str], data)

                return chain_final_job_ids_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        chain_final_job_ids = _parse_chain_final_job_ids(d.pop("chain_final_job_ids", UNSET))

        def _parse_chain_current_job_ids(data: object) -> Union[None, Unset, list[Union[None, str]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                chain_current_job_ids_type_0 = []
                _chain_current_job_ids_type_0 = data
                for chain_current_job_ids_type_0_item_data in _chain_current_job_ids_type_0:

                    def _parse_chain_current_job_ids_type_0_item(data: object) -> Union[None, str]:
                        if data is None:
                            return data
                        return cast(Union[None, str], data)

                    chain_current_job_ids_type_0_item = _parse_chain_current_job_ids_type_0_item(
                        chain_current_job_ids_type_0_item_data
                    )

                    chain_current_job_ids_type_0.append(chain_current_job_ids_type_0_item)

                return chain_current_job_ids_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union[None, str]]], data)

        chain_current_job_ids = _parse_chain_current_job_ids(d.pop("chain_current_job_ids", UNSET))

        def _parse_chain_current_generation(data: object) -> Union[None, Unset, list[Union[None, int]]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                chain_current_generation_type_0 = []
                _chain_current_generation_type_0 = data
                for chain_current_generation_type_0_item_data in _chain_current_generation_type_0:

                    def _parse_chain_current_generation_type_0_item(data: object) -> Union[None, int]:
                        if data is None:
                            return data
                        return cast(Union[None, int], data)

                    chain_current_generation_type_0_item = _parse_chain_current_generation_type_0_item(
                        chain_current_generation_type_0_item_data
                    )

                    chain_current_generation_type_0.append(chain_current_generation_type_0_item)

                return chain_current_generation_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[Union[None, int]]], data)

        chain_current_generation = _parse_chain_current_generation(d.pop("chain_current_generation", UNSET))

        def _parse_chain_parca_done(data: object) -> Union[None, Unset, bool]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, bool], data)

        chain_parca_done = _parse_chain_parca_done(d.pop("chain_parca_done", UNSET))

        def _parse_multi_node_composite_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        multi_node_composite_id = _parse_multi_node_composite_id(d.pop("multi_node_composite_id", UNSET))

        def _parse_external_job_ids(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                external_job_ids_type_0 = cast(list[str], data)

                return external_job_ids_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        external_job_ids = _parse_external_job_ids(d.pop("external_job_ids", UNSET))

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

        def _parse_campaign_span_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        campaign_span_id = _parse_campaign_span_id(d.pop("campaign_span_id", UNSET))

        def _parse_events_s3_prefix(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        events_s3_prefix = _parse_events_s3_prefix(d.pop("events_s3_prefix", UNSET))

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
            chain_n_generations=chain_n_generations,
            chain_final_job_ids=chain_final_job_ids,
            chain_current_job_ids=chain_current_job_ids,
            chain_current_generation=chain_current_generation,
            chain_parca_done=chain_parca_done,
            multi_node_composite_id=multi_node_composite_id,
            external_job_ids=external_job_ids,
            exit_code=exit_code,
            attempt=attempt,
            error_source=error_source,
            trace_id=trace_id,
            campaign_span_id=campaign_span_id,
            events_s3_prefix=events_s3_prefix,
            stage=stage,
            generation=generation,
            last_event_at=last_event_at,
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
