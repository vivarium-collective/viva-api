"""Unit tests for compose subsystem models."""

from pathlib import Path

import pytest

from viva_api.compose.models import (
    BiGraphComputeType,
    BiGraphProcess,
    BiGraphStep,
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeJobType,
    ComposeSimulationExperiment,
    ComposeSimulationRequest,
    ComposeWorkerEvent,
    ComposeWorkerEventMessagePayload,
    PackageOutline,
    PackageType,
    PBAllowList,
    SimulationFileType,
    get_singularity_hash,
)


class TestSimulationFileType:
    def test_get_file_type_omex(self) -> None:
        assert SimulationFileType.get_file_type(".omex") == SimulationFileType.OMEX

    def test_get_file_type_pbg(self) -> None:
        assert SimulationFileType.get_file_type(".pbg") == SimulationFileType.PBG

    def test_get_file_type_sbml(self) -> None:
        assert SimulationFileType.get_file_type(".sbml") == SimulationFileType.SBML

    def test_get_file_type_unknown(self) -> None:
        with pytest.raises(ValueError, match="Unknown simulation file type"):
            SimulationFileType.get_file_type(".xyz")

    def test_suffix(self) -> None:
        assert SimulationFileType.OMEX.get_files_suffix() == "omex"


class TestComposeSimulationRequest:
    def test_creation(self) -> None:
        req = ComposeSimulationRequest(
            request_file_path=Path("/var/tmp/test.omex"),  # noqa: S108
            simulation_file_type=SimulationFileType.OMEX,
            end_time_point=5.0,
            is_batch=False,
        )
        assert req.end_time_point == 5.0
        assert req.is_batch is False


class TestComposeHpcRun:
    def test_serialization(self) -> None:
        run = ComposeHpcRun(
            database_id=1,
            slurmjobid=12345,
            correlation_id="simulation-abc123",
            job_type=ComposeJobType.SIMULATION,
            sim_id=10,
            simulator_id=None,
            status=ComposeJobStatus.RUNNING,
        )
        data = run.model_dump()
        assert data["slurmjobid"] == 12345
        assert data["status"] == "running"
        restored = ComposeHpcRun.model_validate(data)
        assert restored.status == ComposeJobStatus.RUNNING


class TestBiGraphCompute:
    def test_process(self) -> None:
        p = BiGraphProcess(
            database_id=1,
            module="ecoli.processes.metabolism",
            name="Metabolism",
            compute_type=BiGraphComputeType.PROCESS,
            inputs="glucose,oxygen",
            outputs="atp,co2",
        )
        assert p.compute_type == BiGraphComputeType.PROCESS

    def test_step(self) -> None:
        s = BiGraphStep(
            database_id=2,
            module="ecoli.steps.division",
            name="CellDivision",
            compute_type=BiGraphComputeType.STEP,
            inputs="mass",
            outputs="daughter_cells",
        )
        assert s.compute_type == BiGraphComputeType.STEP


class TestPackageOutline:
    def test_from_pb_outline(self) -> None:
        outline_json = {
            "processes": [{"module": "mod.a", "name": "ProcA", "inputs": "x", "outputs": "y"}],
            "steps": [{"module": "mod.b", "name": "StepB", "inputs": "a", "outputs": "b"}],
        }
        pkg = PackageOutline.from_pb_outline(outline_json, name="test-pkg", package_type=PackageType.PYPI)
        assert pkg.name == "test-pkg"
        assert len(pkg.compute) == 2
        assert pkg.compute[0].compute_type == BiGraphComputeType.PROCESS
        assert pkg.compute[1].compute_type == BiGraphComputeType.STEP


class TestComposeWorkerEvent:
    def test_from_message_payload(self) -> None:
        payload = ComposeWorkerEventMessagePayload(
            correlation_id="sim-abc",
            sequence_number=42,
            time=3.14,
            mass={"cell": 1.5e-12},
        )
        event = ComposeWorkerEvent.from_message_payload(payload)
        assert event.correlation_id == "sim-abc"
        assert event.sequence_number == 42
        assert event.mass["cell"] == pytest.approx(1.5e-12)


class TestComposeSimulationExperiment:
    def test_creation(self) -> None:
        exp = ComposeSimulationExperiment(
            simulation_database_id=1,
            simulator_database_id=2,
        )
        assert exp.simulation_database_id == 1
        assert exp.last_updated  # auto-generated


class TestGetSingularityHash:
    def test_deterministic(self) -> None:
        from viva_api.compose.container_def import ContainerizationEngine, ContainerizationFileRepr

        rep = ContainerizationFileRepr(
            representation="Bootstrap: docker\nFrom: python:3.13",
            containerization_engine=ContainerizationEngine.APPTAINER,
        )
        h1 = get_singularity_hash(rep)
        h2 = get_singularity_hash(rep)
        assert h1 == h2
        assert len(h1) == 32  # MD5 hex digest


class TestPBAllowList:
    def test_creation(self) -> None:
        al = PBAllowList(allow_list=["pypi::cobra", "conda::readdy"])
        assert len(al.allow_list) == 2
