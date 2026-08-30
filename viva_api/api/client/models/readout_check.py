from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.readout_check_schema_type_0 import ReadoutCheckSchemaType0
    from ..models.readout_check_spec import ReadoutCheckSpec
    from ..models.readout_check_state_type_0 import ReadoutCheckStateType0


T = TypeVar("T", bound="ReadoutCheck")


@_attrs_define
class ReadoutCheck:
    """
    Attributes:
        spec (ReadoutCheckSpec): The study spec whose readouts are checked
        ref (Union[None, Unset, str]):
        state (Union['ReadoutCheckStateType0', None, Unset]):
        schema (Union['ReadoutCheckSchemaType0', None, Unset]):
    """

    spec: "ReadoutCheckSpec"
    ref: Union[None, Unset, str] = UNSET
    state: Union["ReadoutCheckStateType0", None, Unset] = UNSET
    schema: Union["ReadoutCheckSchemaType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.readout_check_schema_type_0 import ReadoutCheckSchemaType0
        from ..models.readout_check_state_type_0 import ReadoutCheckStateType0

        spec = self.spec.to_dict()

        ref: Union[None, Unset, str]
        if isinstance(self.ref, Unset):
            ref = UNSET
        else:
            ref = self.ref

        state: Union[None, Unset, dict[str, Any]]
        if isinstance(self.state, Unset):
            state = UNSET
        elif isinstance(self.state, ReadoutCheckStateType0):
            state = self.state.to_dict()
        else:
            state = self.state

        schema: Union[None, Unset, dict[str, Any]]
        if isinstance(self.schema, Unset):
            schema = UNSET
        elif isinstance(self.schema, ReadoutCheckSchemaType0):
            schema = self.schema.to_dict()
        else:
            schema = self.schema

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "spec": spec,
        })
        if ref is not UNSET:
            field_dict["ref"] = ref
        if state is not UNSET:
            field_dict["state"] = state
        if schema is not UNSET:
            field_dict["schema"] = schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.readout_check_schema_type_0 import ReadoutCheckSchemaType0
        from ..models.readout_check_spec import ReadoutCheckSpec
        from ..models.readout_check_state_type_0 import ReadoutCheckStateType0

        d = dict(src_dict)
        spec = ReadoutCheckSpec.from_dict(d.pop("spec"))

        def _parse_ref(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        ref = _parse_ref(d.pop("ref", UNSET))

        def _parse_state(data: object) -> Union["ReadoutCheckStateType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                state_type_0 = ReadoutCheckStateType0.from_dict(data)

                return state_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ReadoutCheckStateType0", None, Unset], data)

        state = _parse_state(d.pop("state", UNSET))

        def _parse_schema(data: object) -> Union["ReadoutCheckSchemaType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                schema_type_0 = ReadoutCheckSchemaType0.from_dict(data)

                return schema_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ReadoutCheckSchemaType0", None, Unset], data)

        schema = _parse_schema(d.pop("schema", UNSET))

        readout_check = cls(
            spec=spec,
            ref=ref,
            state=state,
            schema=schema,
        )

        readout_check.additional_properties = d
        return readout_check

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
