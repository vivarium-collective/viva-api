from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.viz_preview_config_type_0 import VizPreviewConfigType0
    from ..models.viz_preview_investigation_inputs_store_type_0 import VizPreviewInvestigationInputsStoreType0


T = TypeVar("T", bound="VizPreview")


@_attrs_define
class VizPreview:
    """
    Attributes:
        address (str): Visualization class address
        config (Union['VizPreviewConfigType0', None, Unset]):
        source (Union[None, Unset, str]): demo | streaming | investigation
        note_prefix (Union[None, Unset, str]):
        investigation_inputs_store (Union['VizPreviewInvestigationInputsStoreType0', None, Unset]):
    """

    address: str
    config: Union["VizPreviewConfigType0", None, Unset] = UNSET
    source: Union[None, Unset, str] = UNSET
    note_prefix: Union[None, Unset, str] = UNSET
    investigation_inputs_store: Union["VizPreviewInvestigationInputsStoreType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.viz_preview_config_type_0 import VizPreviewConfigType0
        from ..models.viz_preview_investigation_inputs_store_type_0 import VizPreviewInvestigationInputsStoreType0

        address = self.address

        config: Union[None, Unset, dict[str, Any]]
        if isinstance(self.config, Unset):
            config = UNSET
        elif isinstance(self.config, VizPreviewConfigType0):
            config = self.config.to_dict()
        else:
            config = self.config

        source: Union[None, Unset, str]
        if isinstance(self.source, Unset):
            source = UNSET
        else:
            source = self.source

        note_prefix: Union[None, Unset, str]
        if isinstance(self.note_prefix, Unset):
            note_prefix = UNSET
        else:
            note_prefix = self.note_prefix

        investigation_inputs_store: Union[None, Unset, dict[str, Any]]
        if isinstance(self.investigation_inputs_store, Unset):
            investigation_inputs_store = UNSET
        elif isinstance(self.investigation_inputs_store, VizPreviewInvestigationInputsStoreType0):
            investigation_inputs_store = self.investigation_inputs_store.to_dict()
        else:
            investigation_inputs_store = self.investigation_inputs_store

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "address": address,
        })
        if config is not UNSET:
            field_dict["config"] = config
        if source is not UNSET:
            field_dict["source"] = source
        if note_prefix is not UNSET:
            field_dict["note_prefix"] = note_prefix
        if investigation_inputs_store is not UNSET:
            field_dict["investigation_inputs_store"] = investigation_inputs_store

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.viz_preview_config_type_0 import VizPreviewConfigType0
        from ..models.viz_preview_investigation_inputs_store_type_0 import VizPreviewInvestigationInputsStoreType0

        d = dict(src_dict)
        address = d.pop("address")

        def _parse_config(data: object) -> Union["VizPreviewConfigType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_0 = VizPreviewConfigType0.from_dict(data)

                return config_type_0
            except:  # noqa: E722
                pass
            return cast(Union["VizPreviewConfigType0", None, Unset], data)

        config = _parse_config(d.pop("config", UNSET))

        def _parse_source(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        source = _parse_source(d.pop("source", UNSET))

        def _parse_note_prefix(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        note_prefix = _parse_note_prefix(d.pop("note_prefix", UNSET))

        def _parse_investigation_inputs_store(
            data: object,
        ) -> Union["VizPreviewInvestigationInputsStoreType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                investigation_inputs_store_type_0 = VizPreviewInvestigationInputsStoreType0.from_dict(data)

                return investigation_inputs_store_type_0
            except:  # noqa: E722
                pass
            return cast(Union["VizPreviewInvestigationInputsStoreType0", None, Unset], data)

        investigation_inputs_store = _parse_investigation_inputs_store(d.pop("investigation_inputs_store", UNSET))

        viz_preview = cls(
            address=address,
            config=config,
            source=source,
            note_prefix=note_prefix,
            investigation_inputs_store=investigation_inputs_store,
        )

        viz_preview.additional_properties = d
        return viz_preview

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
