"""batch_build helpers added for viva-api#414: batched describe_jobs and the
deterministic-name lookup the orphan reconciler falls back to."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from viva_api.simulation import batch_build


def _settings() -> MagicMock:
    return MagicMock(batch_region="us-gov-west-1")


class TestJobNames:
    def test_names_are_deterministic_in_the_commit(self) -> None:
        assert batch_build.ray_build_job_name("abc1234") == "v2ecoli-ray-build-abc1234"
        assert batch_build.k8s_build_job_names("abc1234") == {
            "arm64": "build-arm64-abc1234",
            "amd64": "build-amd64-abc1234",
        }


@pytest.mark.asyncio
class TestDescribeBatchJobs:
    async def test_empty_input_makes_no_call(self) -> None:
        with patch("viva_api.simulation.batch_build.boto3.client") as client:
            assert await batch_build.describe_batch_jobs([]) == {}
        client.assert_not_called()

    async def test_chunks_at_100_and_reports_missing_ids_as_absent(self) -> None:
        mock_batch = MagicMock()

        def _describe(jobs: list[str]) -> dict[str, Any]:
            assert len(jobs) <= 100
            return {
                "jobs": [
                    {
                        "jobId": jid,
                        "jobName": f"name-{jid}",
                        "status": "SUCCEEDED",
                        "statusReason": None,
                        "createdAt": 1,
                        "stoppedAt": 2,
                    }
                    for jid in jobs
                    if jid != "missing"
                ]
            }

        mock_batch.describe_jobs.side_effect = _describe
        ids = [f"j{i}" for i in range(150)] + ["missing"]
        with (
            patch("viva_api.simulation.batch_build.get_settings", _settings),
            patch("viva_api.simulation.batch_build.boto3.client", return_value=mock_batch),
        ):
            states = await batch_build.describe_batch_jobs(ids)
        assert mock_batch.describe_jobs.call_count == 2
        assert len(states) == 150
        assert "missing" not in states
        assert states["j0"] == batch_build.BatchJobState(
            job_id="j0", job_name="name-j0", status="SUCCEEDED", status_reason=None, created_at_ms=1, stopped_at_ms=2
        )


@pytest.mark.asyncio
class TestFindBatchJobIdsByName:
    async def test_exact_name_newest_first_paginated_and_filtered_by_created_after(self) -> None:
        mock_batch = MagicMock()
        pages = [
            {
                "jobSummaryList": [
                    {"jobId": "old", "jobName": "v2ecoli-ray-build-abc", "createdAt": 1_000},
                    {"jobId": "prefix-match", "jobName": "v2ecoli-ray-build-abcdef", "createdAt": 9_000},
                ],
                "nextToken": "t1",
            },
            {
                "jobSummaryList": [
                    {"jobId": "newest", "jobName": "V2ECOLI-RAY-BUILD-ABC", "createdAt": 5_000},
                    {"jobId": "middle", "jobName": "v2ecoli-ray-build-abc", "createdAt": 3_000},
                ]
            },
        ]
        mock_batch.list_jobs.side_effect = pages
        with (
            patch("viva_api.simulation.batch_build.get_settings", _settings),
            patch("viva_api.simulation.batch_build.boto3.client", return_value=mock_batch),
        ):
            ids = await batch_build.find_batch_job_ids_by_name(
                "build-queue", "v2ecoli-ray-build-abc", created_after_ms=2_000
            )
        # newest first; the exact-name check drops the prefix match Batch's
        # filter may return; created_after drops the older build of the same commit
        assert ids == ["newest", "middle"]
        first_call = mock_batch.list_jobs.call_args_list[0].kwargs
        assert first_call["jobQueue"] == "build-queue"
        assert first_call["filters"] == [{"name": "JOB_NAME", "values": ["v2ecoli-ray-build-abc"]}]
        assert mock_batch.list_jobs.call_args_list[1].kwargs["nextToken"] == "t1"

    async def test_nothing_found(self) -> None:
        mock_batch = MagicMock()
        mock_batch.list_jobs.return_value = {"jobSummaryList": []}
        with (
            patch("viva_api.simulation.batch_build.get_settings", _settings),
            patch("viva_api.simulation.batch_build.boto3.client", return_value=mock_batch),
        ):
            assert await batch_build.find_batch_job_ids_by_name("q", "name") == []
