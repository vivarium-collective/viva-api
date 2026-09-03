from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="NewGeneCacheRequest")


@_attrs_define
class NewGeneCacheRequest:
    """Backlog item 105: stamp an induction level onto a COMPLETED ParCa
    dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
    ``new_genes`` presence/absence -- see ``SimulationServiceRay.
    submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
    own request must have set ``parca_options.new_genes`` (an all-zero-
    expression source has nothing to induce -- not re-validated here, same
    pure-passthrough philosophy as ``injected_processes``/``variants``).

        Attributes:
            parca_dataset_id (int):
            variant (str):
            expression (float):
            translation_efficiency (float):
            rel_exp_adj (Union[None, Unset, str]):
            rel_trl_eff_adj (Union[None, Unset, str]):
            seed (Union[Unset, int]):  Default: 0.
            media_condition (Union[None, Unset, str]):
            fixed_media (Union[None, Unset, str]):
    """

    parca_dataset_id: int
    variant: str
    expression: float
    translation_efficiency: float
    rel_exp_adj: Union[None, Unset, str] = UNSET
    rel_trl_eff_adj: Union[None, Unset, str] = UNSET
    seed: Union[Unset, int] = 0
    media_condition: Union[None, Unset, str] = UNSET
    fixed_media: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        parca_dataset_id = self.parca_dataset_id

        variant = self.variant

        expression = self.expression

        translation_efficiency = self.translation_efficiency

        rel_exp_adj: Union[None, Unset, str]
        if isinstance(self.rel_exp_adj, Unset):
            rel_exp_adj = UNSET
        else:
            rel_exp_adj = self.rel_exp_adj

        rel_trl_eff_adj: Union[None, Unset, str]
        if isinstance(self.rel_trl_eff_adj, Unset):
            rel_trl_eff_adj = UNSET
        else:
            rel_trl_eff_adj = self.rel_trl_eff_adj

        seed = self.seed

        media_condition: Union[None, Unset, str]
        if isinstance(self.media_condition, Unset):
            media_condition = UNSET
        else:
            media_condition = self.media_condition

        fixed_media: Union[None, Unset, str]
        if isinstance(self.fixed_media, Unset):
            fixed_media = UNSET
        else:
            fixed_media = self.fixed_media

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "parca_dataset_id": parca_dataset_id,
            "variant": variant,
            "expression": expression,
            "translation_efficiency": translation_efficiency,
        })
        if rel_exp_adj is not UNSET:
            field_dict["rel_exp_adj"] = rel_exp_adj
        if rel_trl_eff_adj is not UNSET:
            field_dict["rel_trl_eff_adj"] = rel_trl_eff_adj
        if seed is not UNSET:
            field_dict["seed"] = seed
        if media_condition is not UNSET:
            field_dict["media_condition"] = media_condition
        if fixed_media is not UNSET:
            field_dict["fixed_media"] = fixed_media

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        parca_dataset_id = d.pop("parca_dataset_id")

        variant = d.pop("variant")

        expression = d.pop("expression")

        translation_efficiency = d.pop("translation_efficiency")

        def _parse_rel_exp_adj(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        rel_exp_adj = _parse_rel_exp_adj(d.pop("rel_exp_adj", UNSET))

        def _parse_rel_trl_eff_adj(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        rel_trl_eff_adj = _parse_rel_trl_eff_adj(d.pop("rel_trl_eff_adj", UNSET))

        seed = d.pop("seed", UNSET)

        def _parse_media_condition(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        media_condition = _parse_media_condition(d.pop("media_condition", UNSET))

        def _parse_fixed_media(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        fixed_media = _parse_fixed_media(d.pop("fixed_media", UNSET))

        new_gene_cache_request = cls(
            parca_dataset_id=parca_dataset_id,
            variant=variant,
            expression=expression,
            translation_efficiency=translation_efficiency,
            rel_exp_adj=rel_exp_adj,
            rel_trl_eff_adj=rel_trl_eff_adj,
            seed=seed,
            media_condition=media_condition,
            fixed_media=fixed_media,
        )

        new_gene_cache_request.additional_properties = d
        return new_gene_cache_request

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
