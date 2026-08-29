from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelayStartRequest")


@_attrs_define
class RelayStartRequest:
    """Start a worker that dials back to *viva-api* rather than to the caller.

    Attributes:
        commit (str): Simulator commit; the prebuilt image tag to run
        workspace (Union[None, Unset, str]): Workspace path inside the worker container
        session_key (Union[None, Unset, str]): Owning session; makes the Job name unique per session
        accept_timeout (Union[Unset, float]): Seconds to wait for the worker to dial back (pod schedule + image pull)
            Default: 300.0.
    """

    commit: str
    workspace: Union[None, Unset, str] = UNSET
    session_key: Union[None, Unset, str] = UNSET
    accept_timeout: Union[Unset, float] = 300.0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        commit = self.commit

        workspace: Union[None, Unset, str]
        if isinstance(self.workspace, Unset):
            workspace = UNSET
        else:
            workspace = self.workspace

        session_key: Union[None, Unset, str]
        if isinstance(self.session_key, Unset):
            session_key = UNSET
        else:
            session_key = self.session_key

        accept_timeout = self.accept_timeout

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "commit": commit,
        })
        if workspace is not UNSET:
            field_dict["workspace"] = workspace
        if session_key is not UNSET:
            field_dict["session_key"] = session_key
        if accept_timeout is not UNSET:
            field_dict["accept_timeout"] = accept_timeout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        commit = d.pop("commit")

        def _parse_workspace(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        workspace = _parse_workspace(d.pop("workspace", UNSET))

        def _parse_session_key(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        session_key = _parse_session_key(d.pop("session_key", UNSET))

        accept_timeout = d.pop("accept_timeout", UNSET)

        relay_start_request = cls(
            commit=commit,
            workspace=workspace,
            session_key=session_key,
            accept_timeout=accept_timeout,
        )

        relay_start_request.additional_properties = d
        return relay_start_request

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
