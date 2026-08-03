from enum import Enum


class PackageType(str, Enum):
    CONDA = "conda"
    PYPI = "pypi"

    def __str__(self) -> str:
        return str(self.value)
