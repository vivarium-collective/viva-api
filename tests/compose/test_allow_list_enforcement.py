import pytest
from fastapi import HTTPException

from sms_api.compose.handlers import _check_allow_list
from sms_api.compose.models import PBAllowList


def test_allowed_deps_pass() -> None:
    allow_list = PBAllowList(allow_list=["pypi::cobra", "pypi::git+https://github.com/vivarium-collective/v2ecoli.git"])
    _check_allow_list(["cobra", "git+https://github.com/vivarium-collective/v2ecoli.git@abc123"], allow_list)


def test_no_extra_deps_is_a_noop() -> None:
    _check_allow_list(None, PBAllowList(allow_list=[]))
    _check_allow_list([], PBAllowList(allow_list=["pypi::cobra"]))


def test_disallowed_dep_is_rejected() -> None:
    allow_list = PBAllowList(allow_list=["pypi::cobra"])
    with pytest.raises(HTTPException) as exc_info:
        _check_allow_list(["git+https://github.com/some-untrusted/repo.git"], allow_list)
    assert exc_info.value.status_code == 403
