from enum import Enum


class SimulationObservablesStore(str, Enum):
    PARQUET = "parquet"
    ZARR = "zarr"

    def __str__(self) -> str:
        return str(self.value)
