from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_selector_schema_type_0 import CompositeSelectorSchemaType0
    from ..models.composite_selector_state_type_0 import CompositeSelectorStateType0


T = TypeVar("T", bound="CompositeSelector")


@_attrs_define
class CompositeSelector:
    """A composite given EITHER by `ref` OR inline as `{state, schema}`.

    Both forms are real and the worker accepts either; sending neither is the
    mistake worth catching here, because the worker answers it with
    `__not_registered__` -- which reads as "your ref is wrong" to a caller who
    sent no ref at all.

        Attributes:
            ref (Union[None, Unset, str]):
            state (Union['CompositeSelectorStateType0', None, Unset]):
            schema (Union['CompositeSelectorSchemaType0', None, Unset]):
    """

    ref: Union[None, Unset, str] = UNSET
    state: Union["CompositeSelectorStateType0", None, Unset] = UNSET
    schema: Union["CompositeSelectorSchemaType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.composite_selector_schema_type_0 import CompositeSelectorSchemaType0
        from ..models.composite_selector_state_type_0 import CompositeSelectorStateType0

        ref: Union[None, Unset, str]
        if isinstance(self.ref, Unset):
            ref = UNSET
        else:
            ref = self.ref

        state: Union[None, Unset, dict[str, Any]]
        if isinstance(self.state, Unset):
            state = UNSET
        elif isinstance(self.state, CompositeSelectorStateType0):
            state = self.state.to_dict()
        else:
            state = self.state

        schema: Union[None, Unset, dict[str, Any]]
        if isinstance(self.schema, Unset):
            schema = UNSET
        elif isinstance(self.schema, CompositeSelectorSchemaType0):
            schema = self.schema.to_dict()
        else:
            schema = self.schema

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if ref is not UNSET:
            field_dict["ref"] = ref
        if state is not UNSET:
            field_dict["state"] = state
        if schema is not UNSET:
            field_dict["schema"] = schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_selector_schema_type_0 import CompositeSelectorSchemaType0
        from ..models.composite_selector_state_type_0 import CompositeSelectorStateType0

        d = dict(src_dict)

        def _parse_ref(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        ref = _parse_ref(d.pop("ref", UNSET))

        def _parse_state(data: object) -> Union["CompositeSelectorStateType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                state_type_0 = CompositeSelectorStateType0.from_dict(data)

                return state_type_0
            except:  # noqa: E722
                pass
            return cast(Union["CompositeSelectorStateType0", None, Unset], data)

        state = _parse_state(d.pop("state", UNSET))

        def _parse_schema(data: object) -> Union["CompositeSelectorSchemaType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                schema_type_0 = CompositeSelectorSchemaType0.from_dict(data)

                return schema_type_0
            except:  # noqa: E722
                pass
            return cast(Union["CompositeSelectorSchemaType0", None, Unset], data)

        schema = _parse_schema(d.pop("schema", UNSET))

        composite_selector = cls(
            ref=ref,
            state=state,
            schema=schema,
        )

        composite_selector.additional_properties = d
        return composite_selector

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
