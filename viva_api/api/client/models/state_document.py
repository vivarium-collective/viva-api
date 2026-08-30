from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.state_document_document import StateDocumentDocument


T = TypeVar("T", bound="StateDocument")


@_attrs_define
class StateDocument:
    """
    Attributes:
        document (StateDocumentDocument): An already-resolved composite state
        ref (Union[None, Unset, str]): Generator whose core_extensions resolve bare addresses
    """

    document: "StateDocumentDocument"
    ref: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document = self.document.to_dict()

        ref: Union[None, Unset, str]
        if isinstance(self.ref, Unset):
            ref = UNSET
        else:
            ref = self.ref

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "document": document,
        })
        if ref is not UNSET:
            field_dict["ref"] = ref

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.state_document_document import StateDocumentDocument

        d = dict(src_dict)
        document = StateDocumentDocument.from_dict(d.pop("document"))

        def _parse_ref(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        ref = _parse_ref(d.pop("ref", UNSET))

        state_document = cls(
            document=document,
            ref=ref,
        )

        state_document.additional_properties = d
        return state_document

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
