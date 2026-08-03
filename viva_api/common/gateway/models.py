import dataclasses as dc
from enum import StrEnum
from pathlib import Path
from typing import Any

import dotenv
import fastapi
from pydantic import BaseModel


@dc.dataclass
class RouterConfig:
    router: fastapi.APIRouter
    prefix: str
    dependencies: list[Any] | None = None

    @property
    def id(self) -> str | None:
        if len(self.prefix) > 1:
            return self.prefix.split("/")[-1]
        else:
            return None

    def include(self, app: fastapi.FastAPI) -> None:
        return app.include_router(self.router, prefix=self.prefix, dependencies=self.dependencies or [])


class ServiceType(StrEnum):
    SIMULATION = "simulation"
    MONGO = "mongo"
    POSTGRES = "postgres"
    AUTH = "auth"


class ServerMode(StrEnum):
    DEV = "http://localhost:8000"
    PROD = "https://sms.cam.uchc.edu"
    PORT_FORWARD_DEV = "http://localhost:8888"

    @classmethod
    def detect(cls, env_path: Path) -> str:
        return cls.PORT_FORWARD_DEV if dotenv.load_dotenv(env_path) else cls.PROD


class RouterType(StrEnum):
    CORE = "core"
    ANTIBIOTIC = "antibiotic"
    BIOMANUFACTURING = "biomanufacturing"


class LoginForm(BaseModel):
    username: str
    password: str
