from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.viewer_launch_ctx_type_0 import ViewerLaunchCtxType0


T = TypeVar("T", bound="ViewerLaunch")


@_attrs_define
class ViewerLaunch:
    """`analysis_viewers` carries two operations behind an `action` flag. They are
    split into two routes here: listing is a read, launching invokes a
    contributor's callable. One endpoint with a mode string would hide that.

        Attributes:
            uid (str): Viewer uid from the listing
            study (Union[None, Unset, str]):
            run (Union[None, Unset, str]):
            ctx (Union['ViewerLaunchCtxType0', None, Unset]):
    """

    uid: str
    study: Union[None, Unset, str] = UNSET
    run: Union[None, Unset, str] = UNSET
    ctx: Union["ViewerLaunchCtxType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.viewer_launch_ctx_type_0 import ViewerLaunchCtxType0

        uid = self.uid

        study: Union[None, Unset, str]
        if isinstance(self.study, Unset):
            study = UNSET
        else:
            study = self.study

        run: Union[None, Unset, str]
        if isinstance(self.run, Unset):
            run = UNSET
        else:
            run = self.run

        ctx: Union[None, Unset, dict[str, Any]]
        if isinstance(self.ctx, Unset):
            ctx = UNSET
        elif isinstance(self.ctx, ViewerLaunchCtxType0):
            ctx = self.ctx.to_dict()
        else:
            ctx = self.ctx

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({
            "uid": uid,
        })
        if study is not UNSET:
            field_dict["study"] = study
        if run is not UNSET:
            field_dict["run"] = run
        if ctx is not UNSET:
            field_dict["ctx"] = ctx

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.viewer_launch_ctx_type_0 import ViewerLaunchCtxType0

        d = dict(src_dict)
        uid = d.pop("uid")

        def _parse_study(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        study = _parse_study(d.pop("study", UNSET))

        def _parse_run(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        run = _parse_run(d.pop("run", UNSET))

        def _parse_ctx(data: object) -> Union["ViewerLaunchCtxType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                ctx_type_0 = ViewerLaunchCtxType0.from_dict(data)

                return ctx_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ViewerLaunchCtxType0", None, Unset], data)

        ctx = _parse_ctx(d.pop("ctx", UNSET))

        viewer_launch = cls(
            uid=uid,
            study=study,
            run=run,
            ctx=ctx,
        )

        viewer_launch.additional_properties = d
        return viewer_launch

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
