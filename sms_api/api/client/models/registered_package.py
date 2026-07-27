from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.package_type import PackageType

if TYPE_CHECKING:
    from ..models.bi_graph_process import BiGraphProcess
    from ..models.bi_graph_step import BiGraphStep


T = TypeVar("T", bound="RegisteredPackage")


@_attrs_define
class RegisteredPackage:
    """
    Attributes:
        database_id (int):
        package_type (PackageType):
        name (str):
        processes (list['BiGraphProcess']):
        steps (list['BiGraphStep']):
    """

    database_id: int
    package_type: PackageType
    name: str
    processes: list["BiGraphProcess"]
    steps: list["BiGraphStep"]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        package_type = self.package_type.value

        name = self.name

        processes = []
        for processes_item_data in self.processes:
            processes_item = processes_item_data.to_dict()
            processes.append(processes_item)

        steps = []
        for steps_item_data in self.steps:
            steps_item = steps_item_data.to_dict()
            steps.append(steps_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "database_id": database_id,
                "package_type": package_type,
                "name": name,
                "processes": processes,
                "steps": steps,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bi_graph_process import BiGraphProcess
        from ..models.bi_graph_step import BiGraphStep

        d = dict(src_dict)
        database_id = d.pop("database_id")

        package_type = PackageType(d.pop("package_type"))

        name = d.pop("name")

        processes = []
        _processes = d.pop("processes")
        for processes_item_data in _processes:
            processes_item = BiGraphProcess.from_dict(processes_item_data)

            processes.append(processes_item)

        steps = []
        _steps = d.pop("steps")
        for steps_item_data in _steps:
            steps_item = BiGraphStep.from_dict(steps_item_data)

            steps.append(steps_item)

        registered_package = cls(
            database_id=database_id,
            package_type=package_type,
            name=name,
            processes=processes,
            steps=steps,
        )

        registered_package.additional_properties = d
        return registered_package

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
