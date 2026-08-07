from pathlib import Path

import pytest_asyncio

from viva_api.common.storage.file_paths import HPCFilePath


@pytest_asyncio.fixture(scope="function")
async def analysis_outdir() -> HPCFilePath:
    return HPCFilePath(remote_path=Path("/projects/SMS/viva_api/alex/sims/sms_multigeneration/analyses"))
