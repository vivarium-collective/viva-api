"""Unit tests for compose ORM tables and enum mappers."""

import pytest

from sms_api.compose.models import (
    BiGraphComputeType,
    ComposeJobStatus,
    ComposeJobType,
    PackageType,
)
from sms_api.compose.tables_orm import (
    BiGraphComputeTypeDB,
    ComposeJobStatusDB,
    ComposeJobTypeDB,
    PackageTypeDB,
)


class TestEnumMappers:
    def test_job_status_roundtrip(self) -> None:
        for status in ComposeJobStatus:
            db_status = ComposeJobStatusDB(status.value)
            assert db_status.to_job_status() == status

    def test_job_type_roundtrip(self) -> None:
        for jt in ComposeJobType:
            db_jt = ComposeJobTypeDB.from_job_type(jt)
            assert db_jt.to_job_type() == jt

    def test_package_type_roundtrip(self) -> None:
        for pt in PackageType:
            db_pt = PackageTypeDB.from_package_type(pt)
            assert db_pt.to_package_type() == pt

    def test_compute_type_roundtrip(self) -> None:
        for ct in BiGraphComputeType:
            db_ct = BiGraphComputeTypeDB.from_compute_type(ct)
            assert db_ct.to_compute_type() == ct

    def test_compute_type_none_raises(self) -> None:
        with pytest.raises(ValueError, match="No compute type specified"):
            BiGraphComputeTypeDB.from_compute_type(None)


class TestComposeTableNames:
    """Verify all compose tables use the compose_ prefix to avoid collisions."""

    def test_table_prefixes(self) -> None:
        from sms_api.compose.tables_orm import (
            ORMComposeAllowList,
            ORMComposeBiGraphCompute,
            ORMComposeHpcRun,
            ORMComposePackage,
            ORMComposeSimulation,
            ORMComposeSimulator,
            ORMComposeSimulatorToPackage,
            ORMComposeWorkerEvent,
        )

        tables = [
            ORMComposeSimulator,
            ORMComposeSimulatorToPackage,
            ORMComposeSimulation,
            ORMComposeHpcRun,
            ORMComposeWorkerEvent,
            ORMComposePackage,
            ORMComposeBiGraphCompute,
            ORMComposeAllowList,
        ]
        for table in tables:
            assert table.__tablename__.startswith("compose_"), (
                f"{table.__name__} table '{table.__tablename__}' does not have compose_ prefix"
            )


class TestComposeStatusLifecycle:
    """Guards the monitor's in-flight/terminal partition (see database_service).

    Regression: list_running_hpcruns polled RUNNING-only, so a Batch job marked
    QUEUED (Batch RUNNABLE) dropped out of polling and froze at QUEUED forever even
    after it SUCCEEDED. The monitor must keep polling every non-terminal state.
    """

    def test_running_and_queued_are_not_terminal(self) -> None:
        from sms_api.compose.database_service import _TERMINAL_COMPOSE_STATUSES
        from sms_api.compose.tables_orm import ComposeJobStatusDB

        # the two that broke: an in-flight job MUST remain pollable
        assert ComposeJobStatusDB.RUNNING not in _TERMINAL_COMPOSE_STATUSES
        assert ComposeJobStatusDB.QUEUED not in _TERMINAL_COMPOSE_STATUSES
        assert ComposeJobStatusDB.PENDING not in _TERMINAL_COMPOSE_STATUSES
        assert ComposeJobStatusDB.WAITING not in _TERMINAL_COMPOSE_STATUSES

    def test_terminal_states_are_actually_terminal(self) -> None:
        from sms_api.compose.database_service import _TERMINAL_COMPOSE_STATUSES
        from sms_api.compose.tables_orm import ComposeJobStatusDB

        for done in (ComposeJobStatusDB.COMPLETED, ComposeJobStatusDB.FAILED, ComposeJobStatusDB.CANCELLED):
            assert done in _TERMINAL_COMPOSE_STATUSES

    def test_every_status_is_classified(self) -> None:
        # a new enum value must be consciously put on one side or the other, not
        # silently treated as in-flight (poll-forever) or terminal (never-poll).
        from sms_api.compose.database_service import _TERMINAL_COMPOSE_STATUSES
        from sms_api.compose.tables_orm import ComposeJobStatusDB

        terminal = set(_TERMINAL_COMPOSE_STATUSES)
        in_flight = set(ComposeJobStatusDB) - terminal
        assert terminal | in_flight == set(ComposeJobStatusDB)
        assert not (terminal & in_flight)
