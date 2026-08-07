"""Unit tests for the pure schema-diff logic (no database access)."""

from viva_api.simulation.schema_diff import DbSchema, diff_schemas


def _expected() -> DbSchema:
    return DbSchema(
        tables={
            "simulation": {"id", "experiment_id", "config", "tags"},
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
            "compose_run": {"id", "status"},  # a table create_all would add on boot
        },
        enums={
            "jobstatusdb": {"waiting", "running", "cancelled"},  # used by hpcrun (usually exists)
            "composejobstatusdb": {"queued", "done"},  # used only by compose_run
        },
        enum_tables={
            "jobstatusdb": {"hpcrun"},
            "composejobstatusdb": {"compose_run"},
        },
    )


def _actual(**overrides: object) -> DbSchema:
    base = DbSchema(
        tables={
            "simulation": {"id", "experiment_id", "config", "tags"},
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
            "compose_run": {"id", "status"},
        },
        enums={"jobstatusdb": {"waiting", "running", "cancelled"}, "composejobstatusdb": {"queued", "done"}},
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_no_drift_when_db_matches_orm() -> None:
    diff = diff_schemas(_expected(), _actual())
    assert not diff.has_blocking_drift
    assert diff.missing_tables == []
    assert diff.missing_columns == {}


def test_missing_column_on_existing_table_is_blocking() -> None:
    actual = _actual(
        tables={
            "simulation": {"id", "experiment_id", "config"},  # no 'tags'
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
            "compose_run": {"id", "status"},
        }
    )
    diff = diff_schemas(_expected(), actual)
    assert diff.has_blocking_drift
    assert diff.missing_columns == {"simulation": ["tags"]}


def test_missing_table_is_info_not_blocking() -> None:
    actual = _actual(
        tables={
            "simulation": {"id", "experiment_id", "config", "tags"},
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
        }
    )
    diff = diff_schemas(_expected(), actual)
    assert diff.missing_tables == ["compose_run"]
    assert not diff.has_blocking_drift


def test_missing_enum_value_on_used_enum_is_blocking() -> None:
    actual = _actual(enums={"jobstatusdb": {"waiting", "running"}, "composejobstatusdb": {"queued", "done"}})
    diff = diff_schemas(_expected(), actual)
    assert diff.has_blocking_drift
    assert diff.missing_enum_values == {"jobstatusdb": ["cancelled"]}


def test_missing_enum_type_used_by_existing_table_is_blocking() -> None:
    # jobstatusdb absent, but hpcrun (which uses it) exists -> blocking.
    actual = _actual(enums={"composejobstatusdb": {"queued", "done"}})
    diff = diff_schemas(_expected(), actual)
    assert diff.has_blocking_drift
    assert diff.missing_enum_types == ["jobstatusdb"]


def test_missing_enum_type_for_missing_table_is_pending_not_blocking() -> None:
    # composejobstatusdb absent AND its only table (compose_run) is absent ->
    # create_all creates the type with the table -> INFO, not blocking.
    actual = _actual(
        tables={
            "simulation": {"id", "experiment_id", "config", "tags"},
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
        },
        enums={"jobstatusdb": {"waiting", "running", "cancelled"}},
    )
    diff = diff_schemas(_expected(), actual)
    assert diff.pending_enum_types == ["composejobstatusdb"]
    assert "composejobstatusdb" not in diff.missing_enum_types
    assert not diff.has_blocking_drift


def test_alembic_version_table_is_ignored() -> None:
    actual = _actual(tables={**_expected().tables, "alembic_version": {"version_num"}})
    diff = diff_schemas(_expected(), actual)
    assert "alembic_version" not in diff.extra_tables
    assert not diff.has_blocking_drift


def test_extra_tables_and_columns_are_info_only() -> None:
    actual = _actual(
        tables={
            "simulation": {"id", "experiment_id", "config", "tags", "legacy_col"},
            "hpcrun": {"id", "job_id_ext", "job_backend", "status"},
            "compose_run": {"id", "status"},
            "old_orphan_table": {"id"},
        }
    )
    diff = diff_schemas(_expected(), actual)
    assert not diff.has_blocking_drift
    assert diff.extra_tables == ["old_orphan_table"]
    assert diff.extra_columns == {"simulation": ["legacy_col"]}
