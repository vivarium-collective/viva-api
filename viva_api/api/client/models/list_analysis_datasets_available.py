from enum import Enum


class ListAnalysisDatasetsAvailable(str, Enum):
    ANY = "any"
    FALSE = "false"
    TRUE = "true"

    def __str__(self) -> str:
        return str(self.value)
