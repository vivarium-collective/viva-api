from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnvWorkerStartRequest")


@_attrs_define
class EnvWorkerStartRequest:
    """Where to dial back, and which environment to run.

    ``commit`` selects the environment: it is the tag of the prebuilt simulator
    image, which under §2A.8 *is* the execution environment rather than a recipe
    for rebuilding one.

        Attributes:
            commit (str): Simulator commit; the prebuilt image tag to run
            callback_host (str): Host/IP the worker dials back to (the workbench pod IP)
            callback_port (int): Port the workbench is listening on
            token (str): One-time handshake token the worker must present
            workspace (Union[None, Unset, str]): Workspace path inside the worker container; defaults to the deployment's
                env_worker_workspace_path (the image's own checkout)
            session_key (Union[None, Unset, str]): Owning session; makes the Job name unique per session
    """

    commit: str
    callback_host: str
    callback_port: int
    token: str
    workspace: Union[None, Unset, str] = UNSET
    session_key: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        commit = self.commit

        callback_host = self.callback_host

        callback_port = self.callback_port

        token = self.token

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

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "commit": commit,
            "callback_host": callback_host,
            "callback_port": callback_port,
            "token": token,
        })
        if workspace is not UNSET:
            field_dict["workspace"] = workspace
        if session_key is not UNSET:
            field_dict["session_key"] = session_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        commit = d.pop("commit")

        callback_host = d.pop("callback_host")

        callback_port = d.pop("callback_port")

        token = d.pop("token")

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

        env_worker_start_request = cls(
            commit=commit,
            callback_host=callback_host,
            callback_port=callback_port,
            token=token,
            workspace=workspace,
            session_key=session_key,
        )

        env_worker_start_request.additional_properties = d
        return env_worker_start_request

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
