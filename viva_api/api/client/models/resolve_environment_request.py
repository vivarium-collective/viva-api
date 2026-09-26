from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.resolve_environment_request_kind import ResolveEnvironmentRequestKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dependency_model import DependencyModel


T = TypeVar("T", bound="ResolveEnvironmentRequest")


@_attrs_define
class ResolveEnvironmentRequest:
    """What is needed. ``explicit``: the environment named ``key`` (a commit, or its own marked tag),
    optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an EMPTY list
    is a real request, for a composite that needs only the built-ins.

        Attributes:
            kind (ResolveEnvironmentRequestKind):
            key (Union[None, Unset, str]):
            variant (Union[Unset, str]):  Default: ''.
            dependencies (Union[Unset, list['DependencyModel']]):
    """

    kind: ResolveEnvironmentRequestKind
    key: Union[None, Unset, str] = UNSET
    variant: Union[Unset, str] = ""
    dependencies: Union[Unset, list["DependencyModel"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        key: Union[None, Unset, str]
        if isinstance(self.key, Unset):
            key = UNSET
        else:
            key = self.key

        variant = self.variant

        dependencies: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.dependencies, Unset):
            dependencies = []
            for dependencies_item_data in self.dependencies:
                dependencies_item = dependencies_item_data.to_dict()
                dependencies.append(dependencies_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "kind": kind,
        })
        if key is not UNSET:
            field_dict["key"] = key
        if variant is not UNSET:
            field_dict["variant"] = variant
        if dependencies is not UNSET:
            field_dict["dependencies"] = dependencies

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dependency_model import DependencyModel

        d = dict(src_dict)
        kind = ResolveEnvironmentRequestKind(d.pop("kind"))

        def _parse_key(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        key = _parse_key(d.pop("key", UNSET))

        variant = d.pop("variant", UNSET)

        dependencies = []
        _dependencies = d.pop("dependencies", UNSET)
        for dependencies_item_data in _dependencies or []:
            dependencies_item = DependencyModel.from_dict(dependencies_item_data)

            dependencies.append(dependencies_item)

        resolve_environment_request = cls(
            kind=kind,
            key=key,
            variant=variant,
            dependencies=dependencies,
        )

        resolve_environment_request.additional_properties = d
        return resolve_environment_request

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
