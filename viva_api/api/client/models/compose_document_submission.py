from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.compose_document_submission_document import ComposeDocumentSubmissionDocument


T = TypeVar("T", bound="ComposeDocumentSubmission")


@_attrs_define
class ComposeDocumentSubmission:
    """A process-bigraph document submitted inline as JSON (POST body), the
    sibling of ComposeSimulationRequest's file-upload transport -- same
    downstream dispatch, different input shape. Field name/shape mirrors
    env_worker.py's own StateDocument.document for naming consistency across
    this repo's two JSON-body document-submission paths.

        Attributes:
            document (ComposeDocumentSubmissionDocument): The composite document itself, as JSON
            interval_time (Union[Unset, float]):  Default: 1.0.
            batch_submission (Union[Unset, bool]):  Default: False.
            simulator_id (Union[None, Unset, int]):
            extra_pip_deps (Union[None, Unset, list[str]]):
    """

    document: "ComposeDocumentSubmissionDocument"
    interval_time: Union[Unset, float] = 1.0
    batch_submission: Union[Unset, bool] = False
    simulator_id: Union[None, Unset, int] = UNSET
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document = self.document.to_dict()

        interval_time = self.interval_time

        batch_submission = self.batch_submission

        simulator_id: Union[None, Unset, int]
        if isinstance(self.simulator_id, Unset):
            simulator_id = UNSET
        else:
            simulator_id = self.simulator_id

        extra_pip_deps: Union[None, Unset, list[str]]
        if isinstance(self.extra_pip_deps, Unset):
            extra_pip_deps = UNSET
        elif isinstance(self.extra_pip_deps, list):
            extra_pip_deps = self.extra_pip_deps

        else:
            extra_pip_deps = self.extra_pip_deps

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document": document,
            }
        )
        if interval_time is not UNSET:
            field_dict["interval_time"] = interval_time
        if batch_submission is not UNSET:
            field_dict["batch_submission"] = batch_submission
        if simulator_id is not UNSET:
            field_dict["simulator_id"] = simulator_id
        if extra_pip_deps is not UNSET:
            field_dict["extra_pip_deps"] = extra_pip_deps

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.compose_document_submission_document import ComposeDocumentSubmissionDocument

        d = dict(src_dict)
        document = ComposeDocumentSubmissionDocument.from_dict(d.pop("document"))

        interval_time = d.pop("interval_time", UNSET)

        batch_submission = d.pop("batch_submission", UNSET)

        def _parse_simulator_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        simulator_id = _parse_simulator_id(d.pop("simulator_id", UNSET))

        def _parse_extra_pip_deps(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                extra_pip_deps_type_0 = cast(list[str], data)

                return extra_pip_deps_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        extra_pip_deps = _parse_extra_pip_deps(d.pop("extra_pip_deps", UNSET))

        compose_document_submission = cls(
            document=document,
            interval_time=interval_time,
            batch_submission=batch_submission,
            simulator_id=simulator_id,
            extra_pip_deps=extra_pip_deps,
        )

        compose_document_submission.additional_properties = d
        return compose_document_submission

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
