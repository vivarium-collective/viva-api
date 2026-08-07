from enum import Enum


class ComposeJobType(str, Enum):
    BUILD_CONTAINER = "build_container"
    SIMULATION = "simulation"

    def __str__(self) -> str:
        return str(self.value)
