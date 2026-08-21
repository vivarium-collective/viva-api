import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="SimulatorVersion")


@_attrs_define
class SimulatorVersion:
    """
    Attributes:
        git_commit_hash (str):
        git_repo_url (str):
        git_branch (str):
        database_id (int):
        created_at (Union[None, Unset, datetime.datetime]):
    """

    git_commit_hash: str
    git_repo_url: str
    git_branch: str
    database_id: int
    created_at: Union[None, Unset, datetime.datetime] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        git_commit_hash = self.git_commit_hash

        git_repo_url = self.git_repo_url

        git_branch = self.git_branch

        database_id = self.database_id

        created_at: Union[None, Unset, str]
        if isinstance(self.created_at, Unset):
            created_at = UNSET
        elif isinstance(self.created_at, datetime.datetime):
            created_at = self.created_at.isoformat()
        else:
            created_at = self.created_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "git_commit_hash": git_commit_hash,
            "git_repo_url": git_repo_url,
            "git_branch": git_branch,
            "database_id": database_id,
        })
        if created_at is not UNSET:
            field_dict["created_at"] = created_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        git_commit_hash = d.pop("git_commit_hash")

        git_repo_url = d.pop("git_repo_url")

        git_branch = d.pop("git_branch")

        database_id = d.pop("database_id")

        def _parse_created_at(data: object) -> Union[None, Unset, datetime.datetime]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                created_at_type_0 = isoparse(data)

                return created_at_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, datetime.datetime], data)

        created_at = _parse_created_at(d.pop("created_at", UNSET))

        simulator_version = cls(
            git_commit_hash=git_commit_hash,
            git_repo_url=git_repo_url,
            git_branch=git_branch,
            database_id=database_id,
            created_at=created_at,
        )

        simulator_version.additional_properties = d
        return simulator_version

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
