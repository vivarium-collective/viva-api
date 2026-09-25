from enum import Enum


class ResolveEnvironmentRequestKind(str, Enum):
    DERIVED = "derived"
    EXPLICIT = "explicit"

    def __str__(self) -> str:
        return str(self.value)
