import datetime
import json
import secrets
import typing
import uuid
from pathlib import Path
from typing import Any, Literal

import numpy as np

from viva_api.common.models import DataId, SSHTarget
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.models import SimulatorVersion

REPO_DIR = Path(__file__).parent.parent.parent.absolute()
PINNED_OUTDIR = REPO_DIR / "out" / "sms_single"
CURRENT_API_VERSION = "v1"
DEFAULT_DATA_ID_PREFIX = "sms"
# Directory for captured sbatch scripts (gitignored)
DEBUG_ARTIFACTS_DIR = REPO_DIR / "artifacts"


class DataString(str):
    def __init__(self, content: str):
        super().__init__()
        self.content = content

    @property
    def timestamp(self) -> str:
        return self.content.split("_")[-1]

    @property
    def identifier(self) -> str:
        return self.content.split("-")[0]


def get_uuid(scope: str | None = None, data_id: str | None = None, n_sections: int = 1) -> str:
    if not scope:
        scope = "smsapi"
    if not data_id:
        data_id = "-".join(list(map(lambda _: get_salt(scope), list(range(n_sections)))))
    else:
        data_id += f"-{get_salt(scope)}"

    item_id = DataId(scope=scope, label=data_id, timestamp=timestamp())
    return item_id.str()


def i_random(start: int = 0, stop: int = 100_000) -> int:
    return np.random.randint(start, stop)


def hashed(data: typing.Any, salt: str | None = None) -> int:
    if salt is None:
        salt = str(uuid.uuid4())
    return int(str(hash(salt + str(data)) & 0xFFFF)[:2])


def get_data_id(
    exp_id: str | None = None, scope: Literal["experiment", "analysis"] | None = None, prefix: str | None = None
) -> str:
    # return f"{prefix or DEFAULT_DATA_ID_PREFIX}_{scope}-{exp_id}-{new_token(exp_id)}"
    return get_uuid(scope=scope)


def get_salt(scope: str) -> str:
    def salt(scope: str) -> str:
        hextag = str(secrets.token_hex(8))[:2]
        return f"{hextag}{hashed(scope, hextag)}"

    return f"{str(secrets.token_hex(8))[:2]}{salt(scope)[:2]}"


def unique_id(data_id: str | None = None, scope: str | None = None) -> str:
    hextag = str(secrets.token_hex(8))[:2]
    item_id = f"{data_id or scope or 'smsapi'}_"
    tag = f"{hextag}{hashed(item_id, hextag)}"
    unique = f"{data_id}_" if data_id is not None else f"{scope or 'smsapi'}_"
    unique += f"{tag}_{timestamp()}"
    return unique


def timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d")


def capture_slurm_script(script: str, filename: str) -> None:
    """Capture generated sbatch script to disk for debugging/inspection.

    Writes the script content to the artifacts/ directory at repo root.
    This directory is gitignored and used for debugging purposes only.

    Args:
        script: The sbatch script content to write.
        filename: The filename to write to (e.g., "simulation.sbatch").
    """
    DEBUG_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(DEBUG_ARTIFACTS_DIR / filename, "w") as f:
        f.write(script)


async def complete_config_template(
    simulator: SimulatorVersion, experiment_id: str, config_filename: str
) -> dict[str, Any]:
    settings = get_settings()
    remote_config_path = (
        settings.hpc_repo_base_path.remote_path / simulator.git_commit_hash / "vEcoli" / "configs" / config_filename
    )
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        returncode, stdout, stderr = await ssh.run_command(f"cat {remote_config_path}")
        if returncode != 0:
            raise ValueError(f"Failed to read config file {remote_config_path}: {stderr}")

    # 3. Replace placeholders in the config template
    config_str = stdout

    config_str = config_str.replace("EXPERIMENT_ID_PLACEHOLDER", experiment_id)
    config_str = config_str.replace("HPC_SIM_BASE_PATH_PLACEHOLDER", str(settings.simulation_outdir))
    image_path = settings.hpc_image_base_path / f"vecoli-{simulator.git_commit_hash}.sif"
    config_str = config_str.replace("SIMULATOR_IMAGE_PATH_PLACEHOLDER", str(image_path))
    config_data = json.loads(config_str)

    return config_data  # type: ignore[no-any-return]
