from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="Simulator")


@_attrs_define
class Simulator:
    """
    Attributes:
        git_commit_hash (str):
        git_repo_url (str):
        git_branch (str):
    """

    git_commit_hash: str
    git_repo_url: str
    git_branch: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        git_commit_hash = self.git_commit_hash

        git_repo_url = self.git_repo_url

        git_branch = self.git_branch

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "git_commit_hash": git_commit_hash,
                "git_repo_url": git_repo_url,
                "git_branch": git_branch,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        git_commit_hash = d.pop("git_commit_hash")

        git_repo_url = d.pop("git_repo_url")

        git_branch = d.pop("git_branch")

        simulator = cls(
            git_commit_hash=git_commit_hash,
            git_repo_url=git_repo_url,
            git_branch=git_branch,
        )

        simulator.additional_properties = d
        return simulator

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
