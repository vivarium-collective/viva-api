from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.compose_job_status import ComposeJobStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="BatchProgress")


@_attrs_define
class BatchProgress:
    """Live progress of a batch (multiseed x multigeneration) compose run.

    Derived purely from the hive-partitioned output the Ray/Batch entrypoint syncs
    to S3 *as the run proceeds* (``…/lineage_seed=<N>/…/generation=<G>/`` partitions,
    written incrementally and s3-synced every ~30 s) — so it needs NO new writer in
    the workload and works for any running compose batch, not just one composite.

    ``lineages``/``generations`` are ``"current:total"`` strings a client renders
    verbatim; ``overall`` is the whole-sweep percent complete
    (``sum(generations_reached) / (n_seeds x n_generations)``), estimated from a bounded
    sample of lineages so the cost is constant regardless of sweep size.

        Attributes:
            lineages (str):
            generations (str):
            overall (float):
            time_elapsed (float):
            status (Union[ComposeJobStatus, None, Unset]):
    """

    lineages: str
    generations: str
    overall: float
    time_elapsed: float
    status: Union[ComposeJobStatus, None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        lineages = self.lineages

        generations = self.generations

        overall = self.overall

        time_elapsed = self.time_elapsed

        status: Union[None, Unset, str]
        if isinstance(self.status, Unset):
            status = UNSET
        elif isinstance(self.status, ComposeJobStatus):
            status = self.status.value
        else:
            status = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "lineages": lineages,
                "generations": generations,
                "overall": overall,
                "time_elapsed": time_elapsed,
            }
        )
        if status is not UNSET:
            field_dict["status"] = status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        lineages = d.pop("lineages")

        generations = d.pop("generations")

        overall = d.pop("overall")

        time_elapsed = d.pop("time_elapsed")

        def _parse_status(data: object) -> Union[ComposeJobStatus, None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                status_type_0 = ComposeJobStatus(data)

                return status_type_0
            except:  # noqa: E722
                pass
            return cast(Union[ComposeJobStatus, None, Unset], data)

        status = _parse_status(d.pop("status", UNSET))

        batch_progress = cls(
            lineages=lineages,
            generations=generations,
            overall=overall,
            time_elapsed=time_elapsed,
            status=status,
        )

        batch_progress.additional_properties = d
        return batch_progress

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
