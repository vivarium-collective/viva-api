"""Unit tests for the pure classification logic in db_reconcile.

These cover the state machine without touching a database. End-to-end
stamp/upgrade behavior is exercised against a real Postgres by the migration
Job in deployment; here we lock down the decision logic that drives it.

The fingerprint vectors below are length-12, matching LEGACY_FINGERPRINTS:
    [baseline, hpcrun-k8s, cancelled-enum, simulation.tags, analysis.n_tp,
     compose_hpcrun.job_id_ext, hpcrun.chain_final_job_ids,
     jobstatusdb-pending-and-cancelled-uppercase, hpcrun.chain_current_job_ids,
     hpcrun.multi_node_composite_id, env_worker_task, hpcrun.external_job_ids]

These vectors have to grow with every migration -- that is the fingerprint
maintenance contract in CLAUDE.md making itself felt, and it is deliberate: a
new marker that nobody notices is exactly how a create_all database gets
stamped stale.
"""

from viva_api.simulation.db_reconcile import DbState, classify

HEAD = "c7d1f3a9b2e4"
# Mirrors LEGACY_FINGERPRINTS ordering.
REVS = [
    "fb7621a73e24",
    "0f991fad32ba",
    "a1c3e5f7b9d2",
    "c1a2b3d4e5f6",
    "d3f9a1c72b84",
    "e5a7c9d10f21",
    "f2b8e4a6c9d1",
    "44335812e447",
    "71a5478673a8",
    "9c2e6b1f4a73",
    "b4d7e9c02a15",
    "c7d1f3a9b2e4",
]


def test_managed_database_takes_upgrade_path() -> None:
    diag = classify(
        alembic_revision="0f991fad32ba",
        fingerprint=[True, True, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.MANAGED
    assert diag.current_revision == "0f991fad32ba"
    assert diag.matched_revision is None
    assert diag.needs_stamp is False
    assert diag.can_upgrade is True


def test_managed_takes_precedence_even_with_odd_fingerprint() -> None:
    diag = classify(
        alembic_revision=HEAD,
        fingerprint=[False, False, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.MANAGED


def test_fresh_database_when_no_tables_and_no_version() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[False, False, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.FRESH
    assert diag.matched_revision is None
    assert diag.can_upgrade is True


def test_legacy_matches_baseline_only() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, False, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "fb7621a73e24"


def test_legacy_matches_middle_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "0f991fad32ba"


def test_legacy_matches_cancelled_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "a1c3e5f7b9d2"


def test_legacy_matches_tags_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "c1a2b3d4e5f6"


def test_legacy_matches_analysis_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "d3f9a1c72b84"


def test_legacy_matches_compose_hpcrun_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "e5a7c9d10f21"


def test_legacy_matches_chain_dispatch_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, True, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "f2b8e4a6c9d1"


def test_legacy_matches_pending_and_cancelled_uppercase_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, True, True, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "44335812e447"


def test_legacy_matches_chain_current_revision() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, True, True, True, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == "71a5478673a8"


def test_legacy_matches_head_when_all_markers_present() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, True, True, True, True, True, True],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    # HEAD, not a literal: this assertion named the then-current revision and so
    # had to be edited by the next migration anyway. The property under test is
    # "all markers present walks all the way to head", which HEAD states directly.
    assert diag.matched_revision == HEAD


def test_legacy_matches_fresh_create_all_database() -> None:
    """Regression for backlog item 40's reconciler-side blast radius.

    A fresh create_all-bootstrapped database (no literal a1c3e5f7b9d2 ever
    applied) has upper-case 'CANCELLED' from the Python enum's .name, never
    the lower-case 'cancelled' the OLD a1c3e5f7b9d2 marker checked alone --
    empirically confirmed to misclassify such a database as INCONSISTENT
    before the marker was corrected to accept either spelling. All markers
    from a1c3e5f7b9d2 onward are satisfied "for free" by create_all, so the
    walk must reach all the way to head, LEGACY, not refuse.
    """
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, True, True, True, True, True, True, True, True, True, True],
        head_revision=HEAD,
    )
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == HEAD


def test_inconsistent_when_later_marker_present_but_earlier_missing() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, False, True, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.INCONSISTENT
    assert diag.matched_revision is None
    assert diag.can_upgrade is False


def test_inconsistent_when_baseline_missing_but_later_present() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[False, True, True, True, True, True, True, True, True, True, False, False],
        head_revision=HEAD,
    )
    assert diag.state is DbState.INCONSISTENT
    assert diag.can_upgrade is False


def test_markers_are_reported_with_labels() -> None:
    diag = classify(
        alembic_revision=None,
        fingerprint=[True, True, False, False, False, False, False, False, False, False, False, False],
        head_revision=HEAD,
    )
    labels = [label for label, _ in diag.markers]
    presence = [present for _, present in diag.markers]
    assert presence == [True, True, False, False, False, False, False, False, False, False, False, False]
    assert any("analysis.n_tp" in label for label in labels)
    assert any("chain_final_job_ids" in label for label in labels)
    assert any("PENDING" in label and "CANCELLED" in label for label in labels)
    assert any("chain_current_job_ids" in label for label in labels)
    assert any("multi_node_composite_id" in label for label in labels)
    assert any("external_job_ids" in label for label in labels)
