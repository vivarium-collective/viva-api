"""Unit tests for the pure classification logic in db_reconcile.

These cover the state machine without touching a database. End-to-end
stamp/upgrade behavior is exercised against a real Postgres by the migration
Job in deployment; here we lock down the decision logic that drives it.

The fingerprint vectors below are length-6, matching LEGACY_FINGERPRINTS:
    [baseline, hpcrun-k8s, cancelled-enum, simulation.tags, analysis.n_tp,
     compose_hpcrun.job_id_ext]
"""

from sms_api.simulation.db_reconcile import DbState, classify

HEAD = "e5a7c9d10f21"
# Mirrors LEGACY_FINGERPRINTS ordering.
REVS = ["fb7621a73e24", "0f991fad32ba", "a1c3e5f7b9d2", "c1a2b3d4e5f6", "d3f9a1c72b84", "e5a7c9d10f21"]


def test_managed_database_takes_upgrade_path() -> None:
    diag = classify(
        alembic_revision="0f991fad32ba", fingerprint=[True, True, False, False, False, False], head_revision=HEAD
    )
    assert diag.state is DbState.MANAGED
    assert diag.current_revision == "0f991fad32ba"
    assert diag.matched_revision is None
    assert diag.needs_stamp is False
    assert diag.can_upgrade is True


def test_managed_takes_precedence_even_with_odd_fingerprint() -> None:
    diag = classify(alembic_revision=HEAD, fingerprint=[False, False, False, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.MANAGED


def test_fresh_database_when_no_tables_and_no_version() -> None:
    diag = classify(alembic_revision=None, fingerprint=[False, False, False, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.FRESH
    assert diag.matched_revision is None
    assert diag.can_upgrade is True


def test_legacy_matches_baseline_only() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, False, False, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "fb7621a73e24"


def test_legacy_matches_middle_revision() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, False, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "0f991fad32ba"


def test_legacy_matches_cancelled_revision() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, True, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "a1c3e5f7b9d2"


def test_legacy_matches_tags_revision() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, True, True, False, False], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "c1a2b3d4e5f6"


def test_legacy_matches_analysis_revision() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, True, True, True, False], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "d3f9a1c72b84"


def test_legacy_matches_head_when_all_markers_present() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, True, True, True, True], head_revision=HEAD)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "e5a7c9d10f21"


def test_inconsistent_when_later_marker_present_but_earlier_missing() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, False, True, False, False, False], head_revision=HEAD)
    assert diag.state is DbState.INCONSISTENT
    assert diag.matched_revision is None
    assert diag.can_upgrade is False


def test_inconsistent_when_baseline_missing_but_later_present() -> None:
    diag = classify(alembic_revision=None, fingerprint=[False, True, True, True, True, True], head_revision=HEAD)
    assert diag.state is DbState.INCONSISTENT
    assert diag.can_upgrade is False


def test_markers_are_reported_with_labels() -> None:
    diag = classify(alembic_revision=None, fingerprint=[True, True, False, False, False, False], head_revision=HEAD)
    labels = [label for label, _ in diag.markers]
    presence = [present for _, present in diag.markers]
    assert presence == [True, True, False, False, False, False]
    assert any("analysis.n_tp" in label for label in labels)
