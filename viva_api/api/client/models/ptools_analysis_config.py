from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.output_file_metadata import OutputFileMetadata


T = TypeVar("T", bound="PtoolsAnalysisConfig")


@_attrs_define
class PtoolsAnalysisConfig:
    """Configuration for a single ptools analysis module.

    :param name: Analysis module type name
        (one of ``"ptools_rxns"``, ``"ptools_rna"``, ``"ptools_proteins"``).
    :param n_tp: Number of timepoints/columns in the output TSV.
    :param files: Specification of files requested to be returned
        with the completion of the analysis.

    Generation/seed filtering is handled at the request level
    (``ExperimentAnalysisRequest.generation_start/end/seeds``), not per-module.
    Fully supported for ``single`` analyses.  Aggregated types
    (``multigeneration``, ``multiseed``) do not currently respect these
    filters due to a known vEcoli limitation.

        Attributes:
            name (Union[Unset, str]):  Default: 'ptools_rxns'.
            n_tp (Union[Unset, int]):  Default: 8.
            variant (Union[Unset, int]):  Default: 0.
            generation (Union[None, Unset, int]):
            lineage_seed (Union[None, Unset, int]):
            agent_id (Union[None, Unset, int]):
            files (Union[None, Unset, list['OutputFileMetadata']]):
    """

    name: Union[Unset, str] = "ptools_rxns"
    n_tp: Union[Unset, int] = 8
    variant: Union[Unset, int] = 0
    generation: Union[None, Unset, int] = UNSET
    lineage_seed: Union[None, Unset, int] = UNSET
    agent_id: Union[None, Unset, int] = UNSET
    files: Union[None, Unset, list["OutputFileMetadata"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        n_tp = self.n_tp

        variant = self.variant

        generation: Union[None, Unset, int]
        if isinstance(self.generation, Unset):
            generation = UNSET
        else:
            generation = self.generation

        lineage_seed: Union[None, Unset, int]
        if isinstance(self.lineage_seed, Unset):
            lineage_seed = UNSET
        else:
            lineage_seed = self.lineage_seed

        agent_id: Union[None, Unset, int]
        if isinstance(self.agent_id, Unset):
            agent_id = UNSET
        else:
            agent_id = self.agent_id

        files: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.files, Unset):
            files = UNSET
        elif isinstance(self.files, list):
            files = []
            for files_type_0_item_data in self.files:
                files_type_0_item = files_type_0_item_data.to_dict()
                files.append(files_type_0_item)

        else:
            files = self.files

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if n_tp is not UNSET:
            field_dict["n_tp"] = n_tp
        if variant is not UNSET:
            field_dict["variant"] = variant
        if generation is not UNSET:
            field_dict["generation"] = generation
        if lineage_seed is not UNSET:
            field_dict["lineage_seed"] = lineage_seed
        if agent_id is not UNSET:
            field_dict["agent_id"] = agent_id
        if files is not UNSET:
            field_dict["files"] = files

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.output_file_metadata import OutputFileMetadata

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        n_tp = d.pop("n_tp", UNSET)

        variant = d.pop("variant", UNSET)

        def _parse_generation(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        generation = _parse_generation(d.pop("generation", UNSET))

        def _parse_lineage_seed(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        lineage_seed = _parse_lineage_seed(d.pop("lineage_seed", UNSET))

        def _parse_agent_id(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        agent_id = _parse_agent_id(d.pop("agent_id", UNSET))

        def _parse_files(data: object) -> Union[None, Unset, list["OutputFileMetadata"]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                files_type_0 = []
                _files_type_0 = data
                for files_type_0_item_data in _files_type_0:
                    files_type_0_item = OutputFileMetadata.from_dict(files_type_0_item_data)

                    files_type_0.append(files_type_0_item)

                return files_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["OutputFileMetadata"]], data)

        files = _parse_files(d.pop("files", UNSET))

        ptools_analysis_config = cls(
            name=name,
            n_tp=n_tp,
            variant=variant,
            generation=generation,
            lineage_seed=lineage_seed,
            agent_id=agent_id,
            files=files,
        )

        ptools_analysis_config.additional_properties = d
        return ptools_analysis_config

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
