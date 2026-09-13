from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.simulation_event_baggage_type_0 import SimulationEventBaggageType0
    from ..models.simulation_event_payload_type_0 import SimulationEventPayloadType0
    from ..models.simulation_event_tags_type_0 import SimulationEventTagsType0


T = TypeVar("T", bound="SimulationEvent")


@_attrs_define
class SimulationEvent:
    """One structured event of a run (``hpcrun_event`` row; observability plan D1).

    The identity block mirrors the engine's JSON-lines schema; infrastructure
    identifiers (Batch job ids, log streams) live in ``tags``/``payload``, never
    in the core fields.

        Attributes:
            seq (int):
            source (str):
            ts (str):
            component (str):
            event (str):
            cursor (Union[None, Unset, int]):
            level (Union[Unset, str]):  Default: 'info'.
            generation (Union[None, Unset, int]):
            variant (Union[None, Unset, int]):
            lineage_seed (Union[None, Unset, int]):
            baggage (Union['SimulationEventBaggageType0', None, Unset]):
            global_time (Union[None, Unset, float]):
            wall_time (Union[None, Unset, float]):
            span_id (Union[None, Unset, str]):
            parent_span_id (Union[None, Unset, str]):
            payload (Union['SimulationEventPayloadType0', None, Unset]):
            tags (Union['SimulationEventTagsType0', None, Unset]):
    """

    seq: int
    source: str
    ts: str
    component: str
    event: str
    cursor: Union[None, Unset, int] = UNSET
    level: Union[Unset, str] = "info"
    generation: Union[None, Unset, int] = UNSET
    variant: Union[None, Unset, int] = UNSET
    lineage_seed: Union[None, Unset, int] = UNSET
    baggage: Union["SimulationEventBaggageType0", None, Unset] = UNSET
    global_time: Union[None, Unset, float] = UNSET
    wall_time: Union[None, Unset, float] = UNSET
    span_id: Union[None, Unset, str] = UNSET
    parent_span_id: Union[None, Unset, str] = UNSET
    payload: Union["SimulationEventPayloadType0", None, Unset] = UNSET
    tags: Union["SimulationEventTagsType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.simulation_event_baggage_type_0 import SimulationEventBaggageType0
        from ..models.simulation_event_payload_type_0 import SimulationEventPayloadType0
        from ..models.simulation_event_tags_type_0 import SimulationEventTagsType0

        seq = self.seq

        source = self.source

        ts = self.ts

        component = self.component

        event = self.event

        cursor: Union[None, Unset, int]
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        level = self.level

        generation: Union[None, Unset, int]
        if isinstance(self.generation, Unset):
            generation = UNSET
        else:
            generation = self.generation

        variant: Union[None, Unset, int]
        if isinstance(self.variant, Unset):
            variant = UNSET
        else:
            variant = self.variant

        lineage_seed: Union[None, Unset, int]
        if isinstance(self.lineage_seed, Unset):
            lineage_seed = UNSET
        else:
            lineage_seed = self.lineage_seed

        baggage: Union[None, Unset, dict[str, Any]]
        if isinstance(self.baggage, Unset):
            baggage = UNSET
        elif isinstance(self.baggage, SimulationEventBaggageType0):
            baggage = self.baggage.to_dict()
        else:
            baggage = self.baggage

        global_time: Union[None, Unset, float]
        if isinstance(self.global_time, Unset):
            global_time = UNSET
        else:
            global_time = self.global_time

        wall_time: Union[None, Unset, float]
        if isinstance(self.wall_time, Unset):
            wall_time = UNSET
        else:
            wall_time = self.wall_time

        span_id: Union[None, Unset, str]
        if isinstance(self.span_id, Unset):
            span_id = UNSET
        else:
            span_id = self.span_id

        parent_span_id: Union[None, Unset, str]
        if isinstance(self.parent_span_id, Unset):
            parent_span_id = UNSET
        else:
            parent_span_id = self.parent_span_id

        payload: Union[None, Unset, dict[str, Any]]
        if isinstance(self.payload, Unset):
            payload = UNSET
        elif isinstance(self.payload, SimulationEventPayloadType0):
            payload = self.payload.to_dict()
        else:
            payload = self.payload

        tags: Union[None, Unset, dict[str, Any]]
        if isinstance(self.tags, Unset):
            tags = UNSET
        elif isinstance(self.tags, SimulationEventTagsType0):
            tags = self.tags.to_dict()
        else:
            tags = self.tags

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "seq": seq,
            "source": source,
            "ts": ts,
            "component": component,
            "event": event,
        })
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if level is not UNSET:
            field_dict["level"] = level
        if generation is not UNSET:
            field_dict["generation"] = generation
        if variant is not UNSET:
            field_dict["variant"] = variant
        if lineage_seed is not UNSET:
            field_dict["lineage_seed"] = lineage_seed
        if baggage is not UNSET:
            field_dict["baggage"] = baggage
        if global_time is not UNSET:
            field_dict["global_time"] = global_time
        if wall_time is not UNSET:
            field_dict["wall_time"] = wall_time
        if span_id is not UNSET:
            field_dict["span_id"] = span_id
        if parent_span_id is not UNSET:
            field_dict["parent_span_id"] = parent_span_id
        if payload is not UNSET:
            field_dict["payload"] = payload
        if tags is not UNSET:
            field_dict["tags"] = tags

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.simulation_event_baggage_type_0 import SimulationEventBaggageType0
        from ..models.simulation_event_payload_type_0 import SimulationEventPayloadType0
        from ..models.simulation_event_tags_type_0 import SimulationEventTagsType0

        d = dict(src_dict)
        seq = d.pop("seq")

        source = d.pop("source")

        ts = d.pop("ts")

        component = d.pop("component")

        event = d.pop("event")

        def _parse_cursor(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        level = d.pop("level", UNSET)

        def _parse_generation(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation = _parse_generation(d.pop("generation", UNSET))

        def _parse_variant(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        variant = _parse_variant(d.pop("variant", UNSET))

        def _parse_lineage_seed(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        lineage_seed = _parse_lineage_seed(d.pop("lineage_seed", UNSET))

        def _parse_baggage(data: object) -> Union["SimulationEventBaggageType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                baggage_type_0 = SimulationEventBaggageType0.from_dict(data)

                return baggage_type_0
            except:  # noqa: E722
                pass
            return cast(Union["SimulationEventBaggageType0", None, Unset], data)

        baggage = _parse_baggage(d.pop("baggage", UNSET))

        def _parse_global_time(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        global_time = _parse_global_time(d.pop("global_time", UNSET))

        def _parse_wall_time(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        wall_time = _parse_wall_time(d.pop("wall_time", UNSET))

        def _parse_span_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        span_id = _parse_span_id(d.pop("span_id", UNSET))

        def _parse_parent_span_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        parent_span_id = _parse_parent_span_id(d.pop("parent_span_id", UNSET))

        def _parse_payload(data: object) -> Union["SimulationEventPayloadType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_0 = SimulationEventPayloadType0.from_dict(data)

                return payload_type_0
            except:  # noqa: E722
                pass
            return cast(Union["SimulationEventPayloadType0", None, Unset], data)

        payload = _parse_payload(d.pop("payload", UNSET))

        def _parse_tags(data: object) -> Union["SimulationEventTagsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                tags_type_0 = SimulationEventTagsType0.from_dict(data)

                return tags_type_0
            except:  # noqa: E722
                pass
            return cast(Union["SimulationEventTagsType0", None, Unset], data)

        tags = _parse_tags(d.pop("tags", UNSET))

        simulation_event = cls(
            seq=seq,
            source=source,
            ts=ts,
            component=component,
            event=event,
            cursor=cursor,
            level=level,
            generation=generation,
            variant=variant,
            lineage_seed=lineage_seed,
            baggage=baggage,
            global_time=global_time,
            wall_time=wall_time,
            span_id=span_id,
            parent_span_id=parent_span_id,
            payload=payload,
            tags=tags,
        )

        simulation_event.additional_properties = d
        return simulation_event

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
