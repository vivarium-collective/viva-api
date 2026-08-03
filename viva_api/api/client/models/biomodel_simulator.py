from enum import Enum


class BiomodelSimulator(str, Enum):
    COPASI = "copasi"
    TELLURIUM = "tellurium"

    def __str__(self) -> str:
        return str(self.value)
