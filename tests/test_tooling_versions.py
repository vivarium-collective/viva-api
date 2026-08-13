"""Keep the lint toolchain's two version declarations in lockstep.

`ruff` is declared twice in this repo, and both declarations are load-bearing:

* ``pyproject.toml``'s ``ruff==X.Y.Z`` (in the ``dev`` dependency group) — what
  ``uv run ruff`` and any developer's local invocation resolve to.
* ``.pre-commit-config.yaml``'s ``ruff-pre-commit`` ``rev`` — what pre-commit
  (and therefore CI's ``quality`` job, via ``make check``) actually runs.

When they drift, the two disagree about the *same file*, and because
``[tool.ruff] fix = true`` a local ``ruff check`` silently REWRITES source to
satisfy the local version — which CI then rejects, or which lands and gets
reverted later. This repo hit that loop repeatedly on one ``# noqa: S603`` in
``tests/api/app/test_cli_e2e.py``: ruff 0.11.5 required the suppression, ruff
0.12.x flagged it as unused and deleted it. Commits 921d532d, d134fd04 and
1561af54 are each "restore the noqa a newer ad-hoc ruff dropped", undoing the
previous one.

Now that `main` requires the `quality` status check to merge, that drift stops
being a nuisance commit and starts blocking merges outright. Hence this guard:
bump both declarations together, in one commit, or this test fails.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
PRE_COMMIT_CONFIG = REPO_ROOT / ".pre-commit-config.yaml"

# `"ruff==0.12.12",` wherever it is declared (currently the `dev` group).
_PYPROJECT_RUFF = re.compile(r'^\s*"ruff==(?P<version>[0-9][^"]*)"', re.MULTILINE)
# The `rev: "v0.12.12"` belonging to the ruff-pre-commit repo entry.
_PRE_COMMIT_RUFF = re.compile(
    r"repo:\s*https://github\.com/astral-sh/ruff-pre-commit\s*\n\s*rev:\s*\"?v?(?P<version>[0-9][^\"\s]*)\"?",
)


def _pyproject_ruff_version() -> str:
    match = _PYPROJECT_RUFF.search(PYPROJECT.read_text(encoding="utf-8"))
    assert match is not None, (
        "pyproject.toml must pin ruff EXACTLY as `ruff==X.Y.Z` so it cannot drift "
        "from .pre-commit-config.yaml's rev; a range (e.g. `ruff>=0.12.2,<0.16`) "
        "silently resolves to whatever is newest and reintroduces the drift"
    )
    return match.group("version")


def _pre_commit_ruff_version() -> str:
    match = _PRE_COMMIT_RUFF.search(PRE_COMMIT_CONFIG.read_text(encoding="utf-8"))
    assert match is not None, "could not find the ruff-pre-commit `rev` in .pre-commit-config.yaml"
    return match.group("version")


def test_ruff_version_matches_between_pyproject_and_pre_commit() -> None:
    """The pinned ruff and the pre-commit ruff must be the same version."""
    pyproject_version = _pyproject_ruff_version()
    pre_commit_version = _pre_commit_ruff_version()
    assert pyproject_version == pre_commit_version, (
        f"ruff version drift: pyproject.toml pins {pyproject_version!r} but "
        f".pre-commit-config.yaml's ruff-pre-commit rev is v{pre_commit_version!r}. "
        "`uv run ruff` and CI would lint the same files with different rules, and "
        "`[tool.ruff] fix = true` means the local one rewrites source to match "
        "itself. Bump both together."
    )


def test_pre_commit_uses_ruff_check_not_the_legacy_alias() -> None:
    """Use the `ruff-check` hook id; the bare `ruff` id is a deprecated alias
    that emits "ruff (legacy alias)" on every pre-commit run."""
    config = PRE_COMMIT_CONFIG.read_text(encoding="utf-8")
    assert "- id: ruff-check" in config, "expected the `ruff-check` hook id in .pre-commit-config.yaml"
    assert not re.search(r"^\s*-\s*id:\s*ruff\s*$", config, re.MULTILINE), (
        "`- id: ruff` is the deprecated alias for `ruff-check`; use `ruff-check`"
    )
