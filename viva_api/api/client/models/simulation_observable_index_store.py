from enum import Enum


class SimulationObservableIndexStore(str, Enum):
    PARQUET = "parquet"
    ZARR = "zarr"

    def __str__(self) -> str:
        return str(self.value)
