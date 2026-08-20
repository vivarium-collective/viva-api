from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.job_status import JobStatus

T = TypeVar("T", bound="ChainProgress")


@_attrs_define
class ChainProgress:
    """Backlog item 6: aggregate per-seed progress for a chain-dispatch campaign
    (backlog item 33) — the data ``get_simulation_status`` already computes via
    ``SimulationServiceRay.get_chain_campaign_result`` and then collapses into
    one coarse ``JobStatus``, exposed at its real granularity instead. Not a new
    data source — the SAME already-tracked ``HpcRun.chain_final_job_ids`` and
    the SAME ``describe_jobs`` polling ``JobScheduler.update_chain_campaigns``
    already runs, just returned unflattened.

        Attributes:
            id (int):
            seeds_total (int):
            seeds_succeeded (int):
            seeds_failed (int):
            seeds_in_progress (int):
            terminal (bool):
            status (JobStatus): Shared job status enum for simulations, analyses, and other HPC jobs.
    """

    id: int
    seeds_total: int
    seeds_succeeded: int
    seeds_failed: int
    seeds_in_progress: int
    terminal: bool
    status: JobStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        seeds_total = self.seeds_total

        seeds_succeeded = self.seeds_succeeded

        seeds_failed = self.seeds_failed

        seeds_in_progress = self.seeds_in_progress

        terminal = self.terminal

        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "id": id,
            "seeds_total": seeds_total,
            "seeds_succeeded": seeds_succeeded,
            "seeds_failed": seeds_failed,
            "seeds_in_progress": seeds_in_progress,
            "terminal": terminal,
            "status": status,
        })

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        seeds_total = d.pop("seeds_total")

        seeds_succeeded = d.pop("seeds_succeeded")

        seeds_failed = d.pop("seeds_failed")

        seeds_in_progress = d.pop("seeds_in_progress")

        terminal = d.pop("terminal")

        status = JobStatus(d.pop("status"))

        chain_progress = cls(
            id=id,
            seeds_total=seeds_total,
            seeds_succeeded=seeds_succeeded,
            seeds_failed=seeds_failed,
            seeds_in_progress=seeds_in_progress,
            terminal=terminal,
            status=status,
        )

        chain_progress.additional_properties = d
        return chain_progress

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
