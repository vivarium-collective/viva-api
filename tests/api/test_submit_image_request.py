"""Requesting the Nextflow HEAD image at build time.

viva-api#423 gave the Ray build path an `include_submit_image` branch, and then
nothing called it -- so the image it builds was unreachable. These pin the wiring
in both directions, because the failure mode of a flag nobody passes is silence.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from viva_api.common.handlers import simulators as handlers
from viva_api.common.models import JobId


def _services(*, supports_flag: bool) -> tuple[MagicMock, MagicMock, dict[str, Any]]:
    """A build service whose submit_build_image_job either accepts the flag (the
    Ray path) or does not (every other path). ``seen`` records what it received."""
    seen: dict[str, Any] = {"include_submit_image": "never-called"}

    async def submit_accepting(*, simulator_version: Any, include_submit_image: bool = False) -> JobId:
        seen["include_submit_image"] = include_submit_image
        return JobId.local("job-1")

    async def submit_rejecting(*, simulator_version: Any) -> JobId:
        seen["include_submit_image"] = None
        return JobId.local("job-1")

    svc = MagicMock()
    svc.submit_build_image_job = submit_accepting if supports_flag else submit_rejecting
    svc._local = MagicMock()

    db = MagicMock()
    db.list_simulators = AsyncMock(return_value=[])
    sim = MagicMock()
    sim.database_id = 7
    sim.git_repo_url = "https://github.com/CovertLabEcoli/sms-ecoli"
    sim.git_commit_hash = "abc1234"
    db.insert_simulator = AsyncMock(return_value=sim)
    db.get_hpcrun_by_ref = AsyncMock(return_value=None)
    db.insert_hpcrun = AsyncMock(return_value=MagicMock(database_id=1))
    return svc, db, seen


async def _upload(svc: MagicMock, db: MagicMock, **kwargs: Any) -> Any:
    with patch.object(handlers, "verify_simulator_payload", lambda *_a, **_k: None):
        return await handlers.upload_simulator(
            commit_hash="abc1234",
            git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli",
            git_branch="main",
            simulation_service_slurm=svc,
            database_service=db,
            **kwargs,
        )


@pytest.mark.asyncio
async def test_flag_reaches_the_build_when_requested() -> None:
    """The whole defect #423 left behind: the branch existed and no caller could
    reach it."""
    svc, db, seen = _services(supports_flag=True)
    await _upload(svc, db, include_submit_image=True)
    assert seen["include_submit_image"] is True


@pytest.mark.asyncio
async def test_default_build_does_not_pay_for_the_head_image() -> None:
    """Off by default -- every build would otherwise carry a JRE + nextflow layer
    it will never use."""
    svc, db, seen = _services(supports_flag=True)
    await _upload(svc, db)
    assert seen["include_submit_image"] is False


@pytest.mark.asyncio
async def test_unsupported_path_refuses_loudly_rather_than_silently_ignoring() -> None:
    """Asking a build path that cannot honour it must FAIL. Silently dropping the
    request would hand back a simulator with no head image and no indication --
    exactly the presence-vs-effect gap this project keeps paying for."""
    svc, db, seen = _services(supports_flag=False)
    with pytest.raises(HTTPException) as exc:
        await _upload(svc, db, include_submit_image=True)
    assert exc.value.status_code == 400
    assert "include_submit_image" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_unsupported_path_is_unaffected_when_not_asked() -> None:
    svc, db, seen = _services(supports_flag=False)
    await _upload(svc, db)
    assert seen["include_submit_image"] is None
