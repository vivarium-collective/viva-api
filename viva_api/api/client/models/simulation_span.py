from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.simulation_span_attrs_type_0 import SimulationSpanAttrsType0


T = TypeVar("T", bound="SimulationSpan")


@_attrs_define
class SimulationSpan:
    """A node of a run's trace tree (``hpcrun_span`` row), materialised from
    ``span.start``/``span.end`` events. ``end_ts`` is ``None`` while open; a span
    still open when the run goes terminal is closed as ``status='unknown'``.

        Attributes:
            span_id (str):
            name (str):
            parent_span_id (Union[None, Unset, str]):
            attrs (Union['SimulationSpanAttrsType0', None, Unset]):
            start_ts (Union[None, Unset, str]):
            end_ts (Union[None, Unset, str]):
            duration_s (Union[None, Unset, float]):
            status (Union[None, Unset, str]):
            error (Union[None, Unset, str]):
    """

    span_id: str
    name: str
    parent_span_id: Union[None, Unset, str] = UNSET
    attrs: Union["SimulationSpanAttrsType0", None, Unset] = UNSET
    start_ts: Union[None, Unset, str] = UNSET
    end_ts: Union[None, Unset, str] = UNSET
    duration_s: Union[None, Unset, float] = UNSET
    status: Union[None, Unset, str] = UNSET
    error: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.simulation_span_attrs_type_0 import SimulationSpanAttrsType0

        span_id = self.span_id

        name = self.name

        parent_span_id: Union[None, Unset, str]
        if isinstance(self.parent_span_id, Unset):
            parent_span_id = UNSET
        else:
            parent_span_id = self.parent_span_id

        attrs: Union[None, Unset, dict[str, Any]]
        if isinstance(self.attrs, Unset):
            attrs = UNSET
        elif isinstance(self.attrs, SimulationSpanAttrsType0):
            attrs = self.attrs.to_dict()
        else:
            attrs = self.attrs

        start_ts: Union[None, Unset, str]
        if isinstance(self.start_ts, Unset):
            start_ts = UNSET
        else:
            start_ts = self.start_ts

        end_ts: Union[None, Unset, str]
        if isinstance(self.end_ts, Unset):
            end_ts = UNSET
        else:
            end_ts = self.end_ts

        duration_s: Union[None, Unset, float]
        if isinstance(self.duration_s, Unset):
            duration_s = UNSET
        else:
            duration_s = self.duration_s

        status: Union[None, Unset, str]
        if isinstance(self.status, Unset):
            status = UNSET
        else:
            status = self.status

        error: Union[None, Unset, str]
        if isinstance(self.error, Unset):
            error = UNSET
        else:
            error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "span_id": span_id,
            "name": name,
        })
        if parent_span_id is not UNSET:
            field_dict["parent_span_id"] = parent_span_id
        if attrs is not UNSET:
            field_dict["attrs"] = attrs
        if start_ts is not UNSET:
            field_dict["start_ts"] = start_ts
        if end_ts is not UNSET:
            field_dict["end_ts"] = end_ts
        if duration_s is not UNSET:
            field_dict["duration_s"] = duration_s
        if status is not UNSET:
            field_dict["status"] = status
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.simulation_span_attrs_type_0 import SimulationSpanAttrsType0

        d = dict(src_dict)
        span_id = d.pop("span_id")

        name = d.pop("name")

        def _parse_parent_span_id(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        parent_span_id = _parse_parent_span_id(d.pop("parent_span_id", UNSET))

        def _parse_attrs(data: object) -> Union["SimulationSpanAttrsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                attrs_type_0 = SimulationSpanAttrsType0.from_dict(data)

                return attrs_type_0
            except:  # noqa: E722
                pass
            return cast(Union["SimulationSpanAttrsType0", None, Unset], data)

        attrs = _parse_attrs(d.pop("attrs", UNSET))

        def _parse_start_ts(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        start_ts = _parse_start_ts(d.pop("start_ts", UNSET))

        def _parse_end_ts(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        end_ts = _parse_end_ts(d.pop("end_ts", UNSET))

        def _parse_duration_s(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        duration_s = _parse_duration_s(d.pop("duration_s", UNSET))

        def _parse_status(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        status = _parse_status(d.pop("status", UNSET))

        def _parse_error(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        error = _parse_error(d.pop("error", UNSET))

        simulation_span = cls(
            span_id=span_id,
            name=name,
            parent_span_id=parent_span_id,
            attrs=attrs,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_s=duration_s,
            status=status,
            error=error,
        )

        simulation_span.additional_properties = d
        return simulation_span

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
