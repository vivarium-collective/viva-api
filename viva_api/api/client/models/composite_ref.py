from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_ref_overrides_type_0 import CompositeRefOverridesType0


T = TypeVar("T", bound="CompositeRef")


@_attrs_define
class CompositeRef:
    """A composite named by a registered generator, optionally with overrides.

    Attributes:
        ref (str): Registered @composite_generator name
        overrides (Union['CompositeRefOverridesType0', None, Unset]): Generator parameter overrides
    """

    ref: str
    overrides: Union["CompositeRefOverridesType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.composite_ref_overrides_type_0 import CompositeRefOverridesType0

        ref = self.ref

        overrides: Union[None, Unset, dict[str, Any]]
        if isinstance(self.overrides, Unset):
            overrides = UNSET
        elif isinstance(self.overrides, CompositeRefOverridesType0):
            overrides = self.overrides.to_dict()
        else:
            overrides = self.overrides

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "ref": ref,
        })
        if overrides is not UNSET:
            field_dict["overrides"] = overrides

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_ref_overrides_type_0 import CompositeRefOverridesType0

        d = dict(src_dict)
        ref = d.pop("ref")

        def _parse_overrides(data: object) -> Union["CompositeRefOverridesType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                overrides_type_0 = CompositeRefOverridesType0.from_dict(data)

                return overrides_type_0
            except:  # noqa: E722
                pass
            return cast(Union["CompositeRefOverridesType0", None, Unset], data)

        overrides = _parse_overrides(d.pop("overrides", UNSET))

        composite_ref = cls(
            ref=ref,
            overrides=overrides,
        )

        composite_ref.additional_properties = d
        return composite_ref

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
