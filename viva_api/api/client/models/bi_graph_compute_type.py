from enum import Enum


class BiGraphComputeType(str, Enum):
    PROCESS = "process"
    STEP = "step"

    def __str__(self) -> str:
        return str(self.value)
