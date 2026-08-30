from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relay_call_request_params_type_0 import RelayCallRequestParamsType0


T = TypeVar("T", bound="RelayCallRequest")


@_attrs_define
class RelayCallRequest:
    """
    Attributes:
        method (str): Worker method name (JSON-RPC)
        params (Union['RelayCallRequestParamsType0', None, Unset]): Method params
        timeout (Union[Unset, float]): Seconds to wait for this call's reply Default: 300.0.
    """

    method: str
    params: Union["RelayCallRequestParamsType0", None, Unset] = UNSET
    timeout: Union[Unset, float] = 300.0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.relay_call_request_params_type_0 import RelayCallRequestParamsType0

        method = self.method

        params: Union[None, Unset, dict[str, Any]]
        if isinstance(self.params, Unset):
            params = UNSET
        elif isinstance(self.params, RelayCallRequestParamsType0):
            params = self.params.to_dict()
        else:
            params = self.params

        timeout = self.timeout

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "method": method,
        })
        if params is not UNSET:
            field_dict["params"] = params
        if timeout is not UNSET:
            field_dict["timeout"] = timeout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relay_call_request_params_type_0 import RelayCallRequestParamsType0

        d = dict(src_dict)
        method = d.pop("method")

        def _parse_params(data: object) -> Union["RelayCallRequestParamsType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                params_type_0 = RelayCallRequestParamsType0.from_dict(data)

                return params_type_0
            except:  # noqa: E722
                pass
            return cast(Union["RelayCallRequestParamsType0", None, Unset], data)

        params = _parse_params(d.pop("params", UNSET))

        timeout = d.pop("timeout", UNSET)

        relay_call_request = cls(
            method=method,
            params=params,
            timeout=timeout,
        )

        relay_call_request.additional_properties = d
        return relay_call_request

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
