from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.repo_discovery_analysis_modules import RepoDiscoveryAnalysisModules


T = TypeVar("T", bound="RepoDiscovery")


@_attrs_define
class RepoDiscovery:
    """Available config filenames and analysis modules discovered from a simulator's repo.

    Attributes:
        simulator_id (int):
        git_repo_url (str):
        git_commit_hash (str):
        config_filenames (Union[Unset, list[str]]):
        analysis_modules (Union[Unset, RepoDiscoveryAnalysisModules]):
    """

    simulator_id: int
    git_repo_url: str
    git_commit_hash: str
    config_filenames: Union[Unset, list[str]] = UNSET
    analysis_modules: Union[Unset, "RepoDiscoveryAnalysisModules"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        simulator_id = self.simulator_id

        git_repo_url = self.git_repo_url

        git_commit_hash = self.git_commit_hash

        config_filenames: Union[Unset, list[str]] = UNSET
        if not isinstance(self.config_filenames, Unset):
            config_filenames = self.config_filenames

        analysis_modules: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.analysis_modules, Unset):
            analysis_modules = self.analysis_modules.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "simulator_id": simulator_id,
            "git_repo_url": git_repo_url,
            "git_commit_hash": git_commit_hash,
        })
        if config_filenames is not UNSET:
            field_dict["config_filenames"] = config_filenames
        if analysis_modules is not UNSET:
            field_dict["analysis_modules"] = analysis_modules

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.repo_discovery_analysis_modules import RepoDiscoveryAnalysisModules

        d = dict(src_dict)
        simulator_id = d.pop("simulator_id")

        git_repo_url = d.pop("git_repo_url")

        git_commit_hash = d.pop("git_commit_hash")

        config_filenames = cast(list[str], d.pop("config_filenames", UNSET))

        _analysis_modules = d.pop("analysis_modules", UNSET)
        analysis_modules: Union[Unset, RepoDiscoveryAnalysisModules]
        if isinstance(_analysis_modules, Unset):
            analysis_modules = UNSET
        else:
            analysis_modules = RepoDiscoveryAnalysisModules.from_dict(_analysis_modules)

        repo_discovery = cls(
            simulator_id=simulator_id,
            git_repo_url=git_repo_url,
            git_commit_hash=git_commit_hash,
            config_filenames=config_filenames,
            analysis_modules=analysis_modules,
        )

        repo_discovery.additional_properties = d
        return repo_discovery

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
