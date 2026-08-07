from enum import Enum


class RunEcoliSimulationNewVecoliSourceType0(str, Enum):
    UPSTREAM = "upstream"
    VIVARIUM_PROCESS = "vivarium-process"

    def __str__(self) -> str:
        return str(self.value)
