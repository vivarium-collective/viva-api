"""The caller-identity seam (viva_api/api/auth.py).

These pin the seam's DELIBERATE limits as much as its behaviour, because the
limits are the part most likely to be misremembered later as "we have auth":

* absent configuration means anonymous, and that is a legitimate steady state
  rather than a misconfiguration;
* reading identity can never break a request;
* only a DESTRUCTIVE operation asks who you are, and it says what would satisfy
  it rather than returning a bare 401.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException, Request

from viva_api.api import auth


def _Req(**headers: str) -> Request:
    """A REAL Starlette Request built from an ASGI scope.

    Not a duck-typed stand-in: Starlette's header lookup is case-INSENSITIVE and
    a plain dict is not, so a stand-in would pass while the real thing behaved
    differently for any proxy that cases its header another way -- which they do.
    """
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "method": "GET", "path": "/", "headers": raw})


@pytest.fixture(autouse=True)
def _no_identity_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test to the unconfigured state, which is what nearly every
    deployment is."""
    monkeypatch.setattr(auth, "identity_header_name", lambda: "")


def _configured(monkeypatch: pytest.MonkeyPatch, name: str = "X-Auth-Request-Email") -> None:
    monkeypatch.setattr(auth, "identity_header_name", lambda: name)


# --- resolve_caller ---------------------------------------------------------


def test_unconfigured_deployment_reports_anonymous() -> None:
    """Not an error. Most deployments have no identity-setting proxy, and
    inventing an identity there would be worse than admitting there is none."""
    assert auth.resolve_caller(_Req()) is None


def test_unconfigured_deployment_ignores_a_header_a_caller_invents() -> None:
    """A caller cannot opt itself into being identified. Only the DEPLOYMENT
    decides which header is trusted, because only it knows what sits in front."""
    req = _Req(**{"X-Auth-Request-Email": "someone@example.org"})
    assert auth.resolve_caller(req) is None


def test_configured_deployment_reads_the_named_header(monkeypatch: pytest.MonkeyPatch) -> None:
    _configured(monkeypatch)
    req = _Req(**{"X-Auth-Request-Email": "kr0@stanford.edu"})
    assert auth.resolve_caller(req) == "kr0@stanford.edu"


def test_header_matching_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proxies disagree about casing, and HTTP says they may. Configuring
    `X-Auth-Request-Email` must still match a proxy that sends
    `x-auth-request-email`, or identity silently vanishes on that deployment."""
    _configured(monkeypatch, "X-Auth-Request-Email")
    assert auth.resolve_caller(_Req(**{"x-auth-request-email": "a@b.c"})) == "a@b.c"


def test_only_the_configured_header_is_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """A deployment behind oauth2-proxy must not also honour some other header an
    attacker finds easier to set."""
    _configured(monkeypatch, "X-Auth-Request-Email")
    req = _Req(**{"X-Forwarded-User": "someone-else@example.org"})
    assert auth.resolve_caller(req) is None


def test_a_blank_header_is_anonymous_not_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty value is how a proxy says 'nobody', and `""` stored as an owner
    would be an identity that every anonymous caller matches."""
    _configured(monkeypatch)
    assert auth.resolve_caller(_Req(**{"X-Auth-Request-Email": "   "})) is None


def test_an_over_long_identity_is_dropped_not_truncated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Truncating would produce a DIFFERENT identity and silently store it, so
    ownership checks would compare against something nobody ever sent."""
    _configured(monkeypatch)
    huge = "a" * (auth.MAX_IDENTITY_LEN + 1)
    assert auth.resolve_caller(_Req(**{"X-Auth-Request-Email": huge})) is None


def test_a_value_at_the_limit_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    _configured(monkeypatch)
    ok = "a" * auth.MAX_IDENTITY_LEN
    assert auth.resolve_caller(_Req(**{"X-Auth-Request-Email": ok})) == ok


# --- require_caller ---------------------------------------------------------


def test_require_returns_the_caller_when_identified(monkeypatch: pytest.MonkeyPatch) -> None:
    _configured(monkeypatch)
    req = _Req(**{"X-Auth-Request-Email": "jcschaff@stanford.edu"})
    assert auth.require_caller(req) == "jcschaff@stanford.edu"


def test_require_401s_anonymously_and_names_the_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 401 that does not say what would satisfy it is a dead end."""
    _configured(monkeypatch, "X-Remote-User")
    with pytest.raises(HTTPException) as ei:
        auth.require_caller(_Req())
    assert ei.value.status_code == 401
    assert "X-Remote-User" in ei.value.detail


def test_require_401_on_an_unconfigured_deployment_explains_why(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hardest error to act on: nothing the CALLER can send would help, so
    the message has to say that rather than demand a header that is not read."""
    with pytest.raises(HTTPException) as ei:
        auth.require_caller(_Req())
    assert ei.value.status_code == 401
    assert "VIVA_API_IDENTITY_HEADER" in ei.value.detail
    assert "unset" in ei.value.detail


def test_reading_identity_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """resolve_caller is on the path of ordinary requests; it must not be able to
    break one. require_caller is the only place that refuses."""
    _configured(monkeypatch)
    for headers in ({}, {"X-Auth-Request-Email": ""}, {"X-Auth-Request-Email": "x" * 10_000}):
        assert auth.resolve_caller(_Req(**headers)) is None


def test_the_setting_defaults_to_unconfigured() -> None:
    """The default must be 'no identity', so adding this module changes the
    behaviour of exactly zero existing deployments."""
    from viva_api.config import get_settings

    assert get_settings().identity_header == ""
