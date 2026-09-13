from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.simulation_event import SimulationEvent
    from ..models.span_tree import SpanTree


T = TypeVar("T", bound="SimulationEvents")


@_attrs_define
class SimulationEvents:
    """``GET /simulations/{id}/events``: a page of events (flat) or the span tree.

    ``next`` is the cursor to pass back as ``after`` for the next page; ``None``
    when this page was not full. ``tree`` is filled only when ``?tree=true``.

        Attributes:
            id (int):
            trace_id (Union[None, Unset, str]):
            events (Union[Unset, list['SimulationEvent']]):
            tree (Union[None, Unset, list['SpanTree']]):
            next_ (Union[None, Unset, int]):
    """

    id: int
    trace_id: Union[None, Unset, str] = UNSET
    events: Union[Unset, list["SimulationEvent"]] = UNSET
    tree: Union[None, Unset, list["SpanTree"]] = UNSET
    next_: Union[None, Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        trace_id: Union[None, Unset, str]
        if isinstance(self.trace_id, Unset):
            trace_id = UNSET
        else:
            trace_id = self.trace_id

        events: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.events, Unset):
            events = []
            for events_item_data in self.events:
                events_item = events_item_data.to_dict()
                events.append(events_item)

        tree: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.tree, Unset):
            tree = UNSET
        elif isinstance(self.tree, list):
            tree = []
            for tree_type_0_item_data in self.tree:
                tree_type_0_item = tree_type_0_item_data.to_dict()
                tree.append(tree_type_0_item)

        else:
            tree = self.tree

        next_: Union[None, Unset, int]
        if isinstance(self.next_, Unset):
            next_ = UNSET
        else:
            next_ = self.next_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "id": id,
        })
        if trace_id is not UNSET:
            field_dict["trace_id"] = trace_id
        if events is not UNSET:
            field_dict["events"] = events
        if tree is not UNSET:
            field_dict["tree"] = tree
        if next_ is not UNSET:
            field_dict["next"] = next_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.simulation_event import SimulationEvent
        from ..models.span_tree import SpanTree

        d = dict(src_dict)
        id = d.pop("id")

        def _parse_trace_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        trace_id = _parse_trace_id(d.pop("trace_id", UNSET))

        events = []
        _events = d.pop("events", UNSET)
        for events_item_data in _events or []:
            events_item = SimulationEvent.from_dict(events_item_data)

            events.append(events_item)

        def _parse_tree(data: object) -> Union[None, Unset, list["SpanTree"]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                tree_type_0 = []
                _tree_type_0 = data
                for tree_type_0_item_data in _tree_type_0:
                    tree_type_0_item = SpanTree.from_dict(tree_type_0_item_data)

                    tree_type_0.append(tree_type_0_item)

                return tree_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["SpanTree"]], data)

        tree = _parse_tree(d.pop("tree", UNSET))

        def _parse_next_(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        next_ = _parse_next_(d.pop("next", UNSET))

        simulation_events = cls(
            id=id,
            trace_id=trace_id,
            events=events,
            tree=tree,
            next_=next_,
        )

        simulation_events.additional_properties = d
        return simulation_events

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
