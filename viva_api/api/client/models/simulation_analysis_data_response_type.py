from enum import Enum


class SimulationAnalysisDataResponseType(str, Enum):
    FILE = "file"
    STREAMING = "streaming"

    def __str__(self) -> str:
        return str(self.value)
