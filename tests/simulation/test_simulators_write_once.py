"""Simulators are write-once; a temporary simulator is the marked exception (docs/plan-core.md D11).

A simulator record, its container image and its image tag are the provenance of every simulation
that ran on them. This file pins the three halves of the rule:

* an authoritative simulator is built once -- ``force`` cannot replace it; a FAILED build is retried;
* a temporary simulator is a NEW record with its own marked image tag every time, never reused,
  and never the answer to a request for an authoritative one;
* everything a temporary simulator writes -- image, job definitions, ParCa cache, build job -- is
  keyed by its marked tag, so it stays out of the authoritative simulator's namespace.
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from tests.simulation.test_ray_backend import _container_settings, _fake_container_batch, _ray_settings
from viva_api.common.handlers.simulators import SimulatorIsWriteOnce, temporary_image_tag, upload_simulator
from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobId, JobStatus
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.simulation.dispatch.build import ImageBuilder
from viva_api.simulation.models import JobType, Simulator, SimulatorVersion
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL

REPO = RepoUrl.V2ECOLI_REPO_URL.value


class RecordingRayService(SimulationServiceRay):
    """The real Ray service, with the build submission recorded instead of run."""

    def __init__(self) -> None:
        super().__init__()
        self.builds: list[SimulatorVersion] = []

    async def submit_build_image_job(self, simulator_version: SimulatorVersion, **kwargs: Any) -> JobId:
        self.builds.append(simulator_version)
        return JobId.local(f"build-{len(self.builds)}")


# ------------------------------------------------------------------ the model


def test_a_temporary_simulator_must_say_who_made_it() -> None:
    with pytest.raises(ValidationError, match="needs a label"):
        Simulator(git_commit_hash="abc1234", git_repo_url=REPO, git_branch="main", temporary=True)
    with pytest.raises(ValidationError, match="label must be"):
        Simulator(git_commit_hash="abc1234", git_repo_url=REPO, git_branch="main", temporary=True, label="a;b")
    made = Simulator(git_commit_hash="abc1234", git_repo_url=REPO, git_branch="main", temporary=True, label="smoke 1")
    assert made.temporary is True


def test_the_environment_key_is_the_commit_unless_the_simulator_has_its_own_tag() -> None:
    authoritative = SimulatorVersion(database_id=1, git_commit_hash="abc1234", git_repo_url=REPO, git_branch="main")
    assert authoritative.environment_key == "abc1234"  # exactly what every call site used before

    tag = temporary_image_tag("abc1234")
    assert tag.startswith("tmp-abc1234-") and tag != temporary_image_tag("abc1234")
    temporary = SimulatorVersion(
        database_id=2,
        git_commit_hash="abc1234",
        git_repo_url=REPO,
        git_branch="main",
        temporary=True,
        label="smoke",
        image_tag=tag,
    )
    assert temporary.environment_key == tag
    assert temporary.git_commit_hash == "abc1234"  # git's, and unchanged


# ------------------------------------------------------------------ the table


@pytest.mark.asyncio
async def test_any_number_of_temporaries_may_share_a_commit_and_none_is_the_authoritative_one(
    database_service: "DatabaseServiceSQL",
) -> None:
    first = await database_service.insert_simulator(
        "f00d123", REPO, "main", temporary=True, label="smoke a", image_tag="tmp-f00d123-aaaaaa"
    )
    second = await database_service.insert_simulator(
        "f00d123", REPO, "main", temporary=True, label="smoke b", image_tag="tmp-f00d123-bbbbbb"
    )
    assert first.database_id != second.database_id
    assert await database_service.get_simulator_by_commit("f00d123") is None

    authoritative = await database_service.insert_simulator("f00d123", REPO, "main")
    assert (authoritative.temporary, authoritative.label, authoritative.image_tag) == (False, None, None)
    found = await database_service.get_simulator_by_commit("f00d123")
    assert found is not None and found.database_id == authoritative.database_id

    with pytest.raises(RuntimeError, match="already exists"):
        await database_service.insert_simulator("f00d123", REPO, "main")

    reread = await database_service.get_simulator(second.database_id)
    assert reread is not None
    assert (reread.temporary, reread.label, reread.image_tag) == (True, "smoke b", "tmp-f00d123-bbbbbb")


# ------------------------------------------------------------------ the handler


async def _upload(service: Any, database_service: Any, commit: str, **kwargs: Any) -> SimulatorVersion:
    return await upload_simulator(
        commit_hash=commit,
        git_repo_url=REPO,
        git_branch="main",
        simulation_service_slurm=service,
        database_service=database_service,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_force_cannot_replace_an_authoritative_simulator(database_service: "DatabaseServiceSQL") -> None:
    service = RecordingRayService()
    built = await _upload(service, database_service, "a11ce01")
    assert [s.database_id for s in service.builds] == [built.database_id]

    # the same request again is a lookup, not a build
    again = await _upload(service, database_service, "a11ce01")
    assert again.database_id == built.database_id and len(service.builds) == 1

    # ...and `force` is refused while it is building, and once it is built
    for status in (JobStatus.RUNNING, JobStatus.COMPLETED):
        run = await database_service.get_hpcrun_by_ref(ref_id=built.database_id, job_type=JobType.BUILD_IMAGE)
        assert run is not None
        await database_service.update_hpcrun_status(run.database_id, JobStatusUpdate(job_id=run.job_id, status=status))
        with pytest.raises(SimulatorIsWriteOnce, match="write-once"):
            await _upload(service, database_service, "a11ce01", force=True)
    assert len(service.builds) == 1


@pytest.mark.asyncio
async def test_a_failed_build_is_retried_because_there_is_no_image_to_replace(
    database_service: "DatabaseServiceSQL",
) -> None:
    service = RecordingRayService()
    built = await _upload(service, database_service, "fa11ed0")
    run = await database_service.get_hpcrun_by_ref(ref_id=built.database_id, job_type=JobType.BUILD_IMAGE)
    assert run is not None
    await database_service.update_hpcrun_status(
        run.database_id, JobStatusUpdate(job_id=run.job_id, status=JobStatus.FAILED)
    )

    retried = await _upload(service, database_service, "fa11ed0")
    assert retried.database_id == built.database_id  # the same record...
    assert len(service.builds) == 2  # ...built again


@pytest.mark.asyncio
async def test_every_temporary_upload_is_a_new_record_with_its_own_marked_tag(
    database_service: "DatabaseServiceSQL",
) -> None:
    service = RecordingRayService()
    authoritative = await _upload(service, database_service, "c0ffee1")

    one = await _upload(service, database_service, "c0ffee1", temporary=True, label="atlantis smoke 1")
    two = await _upload(service, database_service, "c0ffee1", temporary=True, label="atlantis smoke 2")

    assert len({authoritative.database_id, one.database_id, two.database_id}) == 3
    assert one.temporary and two.temporary and not authoritative.temporary
    assert one.image_tag and two.image_tag and one.image_tag != two.image_tag
    assert one.image_tag.startswith("tmp-c0ffee1-")
    assert [s.environment_key for s in service.builds] == ["c0ffee1", one.image_tag, two.image_tag]

    # and an authoritative request still gets the authoritative one, never a temporary
    assert (await _upload(service, database_service, "c0ffee1")).database_id == authoritative.database_id


@pytest.mark.asyncio
async def test_a_temporary_simulator_never_stands_in_for_the_authoritative_one(
    database_service: "DatabaseServiceSQL",
) -> None:
    service = RecordingRayService()
    temporary = await _upload(service, database_service, "0ddba11", temporary=True, label="smoke")
    authoritative = await _upload(service, database_service, "0ddba11")
    assert authoritative.database_id != temporary.database_id
    assert (authoritative.temporary, authoritative.image_tag) == (False, None)


@pytest.mark.asyncio
async def test_temporary_simulators_are_refused_where_the_tag_cannot_be_separated_from_the_commit(
    database_service: "DatabaseServiceSQL", simulation_service_mock_clone_and_build: Any
) -> None:
    with pytest.raises(ValueError, match="Ray / AWS Batch path only"):
        await _upload(
            simulation_service_mock_clone_and_build, database_service, "5a1ad00", temporary=True, label="smoke"
        )


# ------------------------------------------------------------------ what a temporary one writes


def _version(image_tag: str | None) -> SimulatorVersion:
    return SimulatorVersion(
        database_id=9,
        git_commit_hash="abc1234",
        git_repo_url="https://github.com/vivarium-collective/v2Ecoli",
        git_branch="main",
        temporary=image_tag is not None,
        label="smoke" if image_tag else None,
        image_tag=image_tag,
    )


def test_the_build_checks_out_the_commit_and_pushes_the_marked_tag() -> None:
    with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
        builder = ImageBuilder(local_task_service=None)  # type: ignore[arg-type]
        authoritative = builder.build_command(_version(None), include_submit_image=True)[2]
        temporary = builder.build_command(_version("tmp-abc1234-0a1b2c"), include_submit_image=True)[2]

    for script in (authoritative, temporary):
        assert "git checkout abc1234" in script  # the commit is git's, either way
    assert " -i abc1234 " in authoritative and ":abc1234-submit" in authoritative
    assert " -i tmp-abc1234-0a1b2c " in temporary and ":tmp-abc1234-0a1b2c-submit" in temporary
    # the temporary build never names the authoritative tag
    assert " -i abc1234 " not in temporary and ":abc1234-submit" not in temporary
    assert ":abc1234\n" not in temporary


def test_a_temporary_simulators_image_job_definition_and_cache_are_its_own() -> None:
    service = SimulationServiceRay()
    key = _version("tmp-abc1234-0a1b2c").environment_key
    with (
        patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
        patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=_fake_container_batch([])),
    ):
        assert service.batch.image_uri(key).endswith(":tmp-abc1234-0a1b2c")
        assert "tmp-abc1234-0a1b2c" in service.cache_s3_uri(key)
        assert service.cache_s3_uri(key) != service.cache_s3_uri("abc1234")
        assert "tmp-abc1234-0a1b2c" in service.batch.ensure_container_job_def(service.batch.image_uri(key), key)


# ------------------------------------------------------------------ the rule, as a guard


def test_no_route_deletes_or_rewrites_a_simulator() -> None:
    """Write-once is also the absence of a way to do otherwise. ``DatabaseService`` has a
    ``delete_simulator`` (tests use it to clean up); no HTTP route may reach it, and no route
    may PUT or PATCH a simulator."""
    from viva_api.api.main import app

    offenders = []
    for route in app.routes:
        path, methods = str(getattr(route, "path", "")), set(getattr(route, "methods", None) or ())
        if "simulator" in path and methods & {"DELETE", "PUT", "PATCH"}:
            offenders.append(f"{sorted(methods)} {path}")
    assert not offenders, f"a simulator can be deleted or rewritten over HTTP: {offenders}"
