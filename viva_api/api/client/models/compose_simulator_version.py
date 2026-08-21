import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.containerization_file_repr import ContainerizationFileRepr
    from ..models.registered_package import RegisteredPackage


T = TypeVar("T", bound="ComposeSimulatorVersion")


@_attrs_define
class ComposeSimulatorVersion:
    """
    Attributes:
        singularity_def (ContainerizationFileRepr): A textual container-definition file (e.g. a Singularity/apptainer
            def).
        singularity_def_hash (str):
        packages (Union[None, list['RegisteredPackage']]):
        database_id (int):
        created_at (Union[None, Unset, datetime.datetime]):
    """

    singularity_def: "ContainerizationFileRepr"
    singularity_def_hash: str
    packages: Union[None, list["RegisteredPackage"]]
    database_id: int
    created_at: Union[None, Unset, datetime.datetime] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        singularity_def = self.singularity_def.to_dict()

        singularity_def_hash = self.singularity_def_hash

        packages: Union[None, list[dict[str, Any]]]
        if isinstance(self.packages, list):
            packages = []
            for packages_type_0_item_data in self.packages:
                packages_type_0_item = packages_type_0_item_data.to_dict()
                packages.append(packages_type_0_item)

        else:
            packages = self.packages

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
            "singularity_def": singularity_def,
            "singularity_def_hash": singularity_def_hash,
            "packages": packages,
            "database_id": database_id,
        })
        if created_at is not UNSET:
            field_dict["created_at"] = created_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.containerization_file_repr import ContainerizationFileRepr
        from ..models.registered_package import RegisteredPackage

        d = dict(src_dict)
        singularity_def = ContainerizationFileRepr.from_dict(d.pop("singularity_def"))

        singularity_def_hash = d.pop("singularity_def_hash")

        def _parse_packages(data: object) -> Union[None, list["RegisteredPackage"]]:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                packages_type_0 = []
                _packages_type_0 = data
                for packages_type_0_item_data in _packages_type_0:
                    packages_type_0_item = RegisteredPackage.from_dict(packages_type_0_item_data)

                    packages_type_0.append(packages_type_0_item)

                return packages_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, list["RegisteredPackage"]], data)

        packages = _parse_packages(d.pop("packages"))

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

        compose_simulator_version = cls(
            singularity_def=singularity_def,
            singularity_def_hash=singularity_def_hash,
            packages=packages,
            database_id=database_id,
            created_at=created_at,
        )

        compose_simulator_version.additional_properties = d
        return compose_simulator_version

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
