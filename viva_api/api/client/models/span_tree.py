from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.simulation_event import SimulationEvent
    from ..models.simulation_span import SimulationSpan


T = TypeVar("T", bound="SpanTree")


@_attrs_define
class SpanTree:
    """``GET /simulations/{id}/events?tree=true``: the span tree with each span's
    own events attached (events whose ``span_id`` matches; ``tick`` heartbeats are
    never stored, so they never appear here).

        Attributes:
            span (SimulationSpan): A node of a run's trace tree (``hpcrun_span`` row), materialised from
                ``span.start``/``span.end`` events. ``end_ts`` is ``None`` while open; a span
                still open when the run goes terminal is closed as ``status='unknown'``.
            events (Union[Unset, list['SimulationEvent']]):
            children (Union[Unset, list['SpanTree']]):
    """

    span: "SimulationSpan"
    events: Union[Unset, list["SimulationEvent"]] = UNSET
    children: Union[Unset, list["SpanTree"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        span = self.span.to_dict()

        events: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.events, Unset):
            events = []
            for events_item_data in self.events:
                events_item = events_item_data.to_dict()
                events.append(events_item)

        children: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.children, Unset):
            children = []
            for children_item_data in self.children:
                children_item = children_item_data.to_dict()
                children.append(children_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "span": span,
        })
        if events is not UNSET:
            field_dict["events"] = events
        if children is not UNSET:
            field_dict["children"] = children

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.simulation_event import SimulationEvent
        from ..models.simulation_span import SimulationSpan

        d = dict(src_dict)
        span = SimulationSpan.from_dict(d.pop("span"))

        events = []
        _events = d.pop("events", UNSET)
        for events_item_data in _events or []:
            events_item = SimulationEvent.from_dict(events_item_data)

            events.append(events_item)

        children = []
        _children = d.pop("children", UNSET)
        for children_item_data in _children or []:
            children_item = SpanTree.from_dict(children_item_data)

            children.append(children_item)

        span_tree = cls(
            span=span,
            events=events,
            children=children,
        )

        span_tree.additional_properties = d
        return span_tree

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
