from pathlib import Path

import pytest

from viva_api.data.models import BiocycCredentials


@pytest.mark.asyncio
async def test_biocyc_credentials() -> None:
    fp = Path("assets/dev/config/.dev_env_TEMPLATE")
    creds = BiocycCredentials.from_env(fp)
    print(creds)
