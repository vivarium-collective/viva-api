import json
import os
import pathlib
from dataclasses import asdict, dataclass
from typing import Any

import numpy
import numpy as np
import orjson
from pydantic import BaseModel, Field

### -- biocyc -- ###


class BiocycComponentData(BaseModel):
    id: str
    orgid: str
    frameid: str
    detail: str
    parent: dict[str, dict[str, str]] = Field(default_factory=dict)


class BiocycCompound(BiocycComponentData):
    cml: dict[str, Any]
    cls: str | None = None
    # common_name: dict


class BiocycReaction(BiocycComponentData):
    ec_number: dict[str, Any]
    right: list[dict[str, Any]]
    enzymatic_reaction: dict[str, Any]
    left: list[dict[str, Any]]


class BiocycComponent(BaseModel):
    id: str  # loadedjson['obj_id']
    pgdb: dict[str, Any]  # ['data']['ptools-xml']['metadata']['PGDB']
    data: BiocycCompound | BiocycReaction  # ['data']['ptools-xml']['Compound'] FOR EXAMPLE


@dataclass
class BiocycData:
    obj_id: str
    org_id: str
    data: dict[str, Any]
    request: dict[str, Any]
    dest_dirpath: pathlib.Path | None = None

    @property
    def filepath(self) -> pathlib.Path:
        dest_fp = self.dest_dirpath or pathlib.Path("assets/biocyc")
        return dest_fp / f"{self.obj_id}.json"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_dto(self) -> BiocycComponent:
        data = self
        ptools_data = data.data["ptools-xml"]
        pgdb = ptools_data["metadata"]["PGDB"]
        data_key = next(
            iter([key for key in ptools_data if not key.startswith("@") and not key.startswith("metadata")])
        )
        raw_component = ptools_data[data_key]
        comp_data = {}
        for k, v in raw_component.items():
            if "common" in k or "subclass" in k or "synonym" in k:
                continue
            if "class" in k:
                k = "cls"
            if k.startswith("@"):
                k = k.replace("@", "").replace("-", "_")
            comp_data[k.lower()] = v
        data_type = BiocycCompound if "Compound" in data_key else BiocycReaction
        return BiocycComponent(id=data.obj_id, pgdb=pgdb, data=data_type(**comp_data))

    def export(self, fp: pathlib.Path | None = None) -> None:
        try:
            exp = self.to_dict()
            fp = fp or self.filepath
            with open(fp, "w") as f:
                json.dump(exp, f, indent=4)
            print(f"Successfully wrote: {fp}")
        except OSError:
            print(f"Could not write for {self.obj_id}")


@dataclass
class Credentials:
    username: str | None = None
    password: str | None = None
    config: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, str | None]:
        d = asdict(self)
        d.pop("config", None)
        return d


@dataclass
class BiocycCredentials(Credentials):
    def to_dict(self) -> dict[str, str | None]:
        return {"email": self.username, "password": self.password}

    @classmethod
    def from_env(cls, env_fp: pathlib.Path, config: dict[str, Any] | None = None) -> "BiocycCredentials":
        import dotenv

        dotenv.load_dotenv(env_fp)
        print("loading", env_fp)
        return cls(username=os.getenv("BIOCYC_EMAIL"), password=os.getenv("BIOCYC_PASSWORD"), config=config)


class SerializedArray:
    __slots__ = ("_value", "shape")

    def __init__(self, arr: numpy.ndarray) -> None:
        self._value = self.serialize(arr)

    def serialize(self, arr: numpy.ndarray) -> bytes:
        self.shape = arr.shape
        return orjson.dumps(numpy.ravel(arr, order="C").tolist())

    def deserialize(self) -> numpy.ndarray:
        arr: np.ndarray = np.array(orjson.loads(self._value))
        return arr.reshape(self.shape)

    @property
    def value(self) -> numpy.ndarray:
        return self.deserialize()

    @value.setter
    def value(self, value: np.ndarray) -> None:
        self._value = self.serialize(value)
