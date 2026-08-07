from enum import Enum


class RunEcoliSimulationNewCompositeType0(str, Enum):
    V2ECOLI = "v2ecoli"
    VECOLI = "vecoli"

    def __str__(self) -> str:
        return str(self.value)
