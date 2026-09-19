from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dataset_dto import DatasetDTO
    from ..models.dataset_producer_dto import DatasetProducerDTO
    from ..models.simulation_span import SimulationSpan


T = TypeVar("T", bound="DatasetProvenanceDTO")


@_attrs_define
class DatasetProvenanceDTO:
    """``GET /datasets/{id}/provenance``: one hop back from a dataset.

    Attributes:
        dataset (DatasetDTO): One consumable file set a run actually wrote (a ``dataset`` row).

            Never pre-created: it exists only once a run's trace was scraped or the walk found
            the object. Exactly one producer id is set by the registry. ``available`` is false
            once the object is gone; the row itself is kept so provenance never dangles.
        producer (Union['DatasetProducerDTO', None, Unset]):
        span (Union['SimulationSpan', None, Unset]):
        inputs (Union[Unset, list['DatasetDTO']]):
    """

    dataset: "DatasetDTO"
    producer: Union["DatasetProducerDTO", None, Unset] = UNSET
    span: Union["SimulationSpan", None, Unset] = UNSET
    inputs: Union[Unset, list["DatasetDTO"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.dataset_producer_dto import DatasetProducerDTO
        from ..models.simulation_span import SimulationSpan

        dataset = self.dataset.to_dict()

        producer: Union[None, Unset, dict[str, Any]]
        if isinstance(self.producer, Unset):
            producer = UNSET
        elif isinstance(self.producer, DatasetProducerDTO):
            producer = self.producer.to_dict()
        else:
            producer = self.producer

        span: Union[None, Unset, dict[str, Any]]
        if isinstance(self.span, Unset):
            span = UNSET
        elif isinstance(self.span, SimulationSpan):
            span = self.span.to_dict()
        else:
            span = self.span

        inputs: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = []
            for inputs_item_data in self.inputs:
                inputs_item = inputs_item_data.to_dict()
                inputs.append(inputs_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "dataset": dataset,
        })
        if producer is not UNSET:
            field_dict["producer"] = producer
        if span is not UNSET:
            field_dict["span"] = span
        if inputs is not UNSET:
            field_dict["inputs"] = inputs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dataset_dto import DatasetDTO
        from ..models.dataset_producer_dto import DatasetProducerDTO
        from ..models.simulation_span import SimulationSpan

        d = dict(src_dict)
        dataset = DatasetDTO.from_dict(d.pop("dataset"))

        def _parse_producer(data: object) -> Union["DatasetProducerDTO", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                producer_type_0 = DatasetProducerDTO.from_dict(data)

                return producer_type_0
            except:  # noqa: E722
                pass
            return cast(Union["DatasetProducerDTO", None, Unset], data)

        producer = _parse_producer(d.pop("producer", UNSET))

        def _parse_span(data: object) -> Union["SimulationSpan", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                span_type_0 = SimulationSpan.from_dict(data)

                return span_type_0
            except:  # noqa: E722
                pass
            return cast(Union["SimulationSpan", None, Unset], data)

        span = _parse_span(d.pop("span", UNSET))

        inputs = []
        _inputs = d.pop("inputs", UNSET)
        for inputs_item_data in _inputs or []:
            inputs_item = DatasetDTO.from_dict(inputs_item_data)

            inputs.append(inputs_item)

        dataset_provenance_dto = cls(
            dataset=dataset,
            producer=producer,
            span=span,
            inputs=inputs,
        )

        dataset_provenance_dto.additional_properties = d
        return dataset_provenance_dto

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
