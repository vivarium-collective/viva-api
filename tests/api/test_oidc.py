"""OIDC bearer-token validation (#337).

Signed with a REAL RSA keypair and served through a REAL JWKS document, because
the whole value of this module is cryptographic and a mocked verifier would
assert nothing. What is faked is the network: `httpx.get` for discovery and
PyJWKClient's fetch, so no test reaches an issuer.

The bias in what is pinned: **every way this can fail open**. A validator that
wrongly rejects is noisy and gets fixed in an afternoon. One that wrongly
accepts is silent, and on this codebase silence is the known failure mode --
anonymous is a supported state, so "not validating" and "validating" look
identical from outside unless a test tells them apart.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any, cast

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request

from viva_api.api import oidc

ISSUER = "https://keycloak.example.test/realms/viva"
AUDIENCE = "viva-api"


@pytest.fixture(scope="module")
def keypair() -> Any:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def jwks(keypair: Any) -> dict[str, Any]:
    """A real JWKS document for the real key, as an issuer would publish it."""
    from jwt.algorithms import RSAAlgorithm

    key = json.loads(RSAAlgorithm.to_jwk(keypair.public_key()))
    key.update({"kid": "test-key-1", "use": "sig", "alg": "RS256"})
    return {"keys": [key]}


def _token(keypair: Any, **overrides: Any) -> str:
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "a3f1c2d4-0000-4000-8000-000000000001",
        "email": "scientist@example.test",
        "exp": int(time.time()) + 300,
        "iat": int(time.time()) - 5,
    }
    claims.update(overrides)
    for k in [k for k, v in claims.items() if v is None]:
        del claims[k]
    headers = overrides.pop("_headers", {"kid": "test-key-1"})
    token: str = jwt.encode(claims, keypair, algorithm="RS256", headers=headers)
    return token


@pytest.fixture(autouse=True)
def _wire(monkeypatch: pytest.MonkeyPatch, jwks: dict[str, Any]) -> Iterator[None]:
    """Configure OIDC and stub only the NETWORK, never the crypto."""
    oidc.reset_cache()

    class _S:
        oidc_issuer = ISSUER
        oidc_audience = AUDIENCE
        oidc_algorithms = "RS256"
        oidc_jwks_cache_seconds = 300
        oidc_leeway_seconds = 30
        oidc_fetch_timeout_seconds = 5.0

    monkeypatch.setattr(oidc, "get_settings", lambda: _S())

    # Replace ONLY this module's network seam. Patching httpx.get itself would
    # reach every other caller in the process, which a test should not do.
    monkeypatch.setattr(
        oidc,
        "_fetch_json",
        lambda url, timeout: {"issuer": ISSUER, "jwks_uri": f"{ISSUER}/protocol/openid-connect/certs"},
    )
    monkeypatch.setattr("jwt.jwks_client.PyJWKClient.fetch_data", lambda self: jwks)
    yield
    oidc.reset_cache()


def _req(auth: str | None = None) -> Request:
    headers = [(b"authorization", auth.encode())] if auth else []
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


# --- the happy path ---------------------------------------------------------


def test_a_valid_token_yields_its_subject(keypair: Any) -> None:
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair)}")) == "scientist@example.test"


def test_a_readable_claim_is_preferred_over_the_opaque_sub(keypair: Any) -> None:
    """`sub` is guaranteed but is typically a uuid. An Owner column and a 403
    that has to name who started something both want something a human
    recognises, so email and preferred_username come first."""
    token = _token(keypair, email=None, preferred_username="scientist")
    assert oidc.subject_from_bearer(_req(f"Bearer {token}")) == "scientist"


def test_sub_is_the_fallback_when_nothing_readable_is_present(keypair: Any) -> None:
    token = _token(keypair, email=None)
    subject = oidc.subject_from_bearer(_req(f"Bearer {token}"))
    assert subject is not None and subject.startswith("a3f1c2d4")


def test_the_bearer_scheme_is_matched_case_insensitively(keypair: Any) -> None:
    """RFC 6750 says the scheme is case-insensitive, and clients send `bearer`."""
    assert oidc.subject_from_bearer(_req(f"bearer {_token(keypair)}")) is not None


# --- every way it must NOT fail open ----------------------------------------


def test_an_unsigned_token_is_rejected(keypair: Any) -> None:
    """alg=none is the original JWT attack. Must never authenticate anyone."""
    claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": "attacker", "exp": int(time.time()) + 300}
    token = jwt.encode(claims, cast(str, None), algorithm="none")
    assert oidc.subject_from_bearer(_req(f"Bearer {token}")) is None


def test_a_token_signed_by_a_different_key_is_rejected() -> None:
    """The core claim of the whole module."""
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _token(other)
    assert oidc.subject_from_bearer(_req(f"Bearer {token}")) is None


def test_an_expired_token_is_rejected(keypair: Any) -> None:
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair, exp=int(time.time()) - 3600)}")) is None


def test_a_not_yet_valid_token_is_rejected(keypair: Any) -> None:
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair, nbf=int(time.time()) + 3600)}")) is None


def test_a_token_for_a_different_audience_is_rejected(keypair: Any) -> None:
    """The confused-deputy hole: the SAME issuer minting a token for another
    relying party must not be usable here."""
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair, aud='some-other-service')}")) is None


def test_a_token_from_a_different_issuer_is_rejected(keypair: Any) -> None:
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair, iss='https://evil.example.test/')}")) is None


def test_a_token_with_no_expiry_is_rejected(keypair: Any) -> None:
    """`exp` is in the require list. A token that never expires is a password."""
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair, exp=None)}")) is None


def test_garbage_is_rejected_without_raising() -> None:
    for value in ("Bearer not-a-jwt", "Bearer ", "Basic abc123", "", "Bearer a.b.c"):
        assert oidc.subject_from_bearer(_req(value or None)) is None


def test_no_authorization_header_is_anonymous() -> None:
    assert oidc.subject_from_bearer(_req()) is None


# --- half-configured must not become weakly-configured ----------------------


def _settings(monkeypatch: pytest.MonkeyPatch, **over: Any) -> None:
    base = {
        "oidc_issuer": ISSUER,
        "oidc_audience": AUDIENCE,
        "oidc_algorithms": "RS256",
        "oidc_jwks_cache_seconds": 300,
        "oidc_leeway_seconds": 30,
        "oidc_fetch_timeout_seconds": 5.0,
    }
    base.update(over)
    monkeypatch.setattr(oidc, "get_settings", lambda: type("_S", (), base)())
    oidc.reset_cache()


def test_an_issuer_without_an_audience_refuses_to_validate_at_all(
    monkeypatch: pytest.MonkeyPatch, keypair: Any
) -> None:
    """Refusing beats skipping the aud check. A deployment that set OIDC_ISSUER
    meant to turn something on, so failing loudly is more useful than quietly
    validating less than it looks like."""
    _settings(monkeypatch, oidc_audience="")
    assert not oidc.oidc_configured()
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair)}")) is None


@pytest.mark.parametrize("alg", ["none", "HS256", "RS256,HS256"])
def test_forbidden_algorithms_disable_validation(monkeypatch: pytest.MonkeyPatch, alg: str) -> None:
    """`none` is the unsigned attack; HMAC is symmetric and would mean verifying
    against a public key as if it were a shared secret."""
    _settings(monkeypatch, oidc_algorithms=alg)
    assert not oidc.oidc_configured()


def test_an_http_issuer_is_refused_except_on_localhost(monkeypatch: pytest.MonkeyPatch) -> None:
    """OIDC Discovery requires https, and a token over plaintext is replayable."""
    _settings(monkeypatch, oidc_issuer="http://keycloak.example.test/realms/viva")
    assert not oidc.oidc_configured()
    _settings(monkeypatch, oidc_issuer="http://localhost:8080/realms/viva")
    assert oidc.oidc_configured(), "a local issuer must stay usable for dev and tests"


def test_unset_issuer_makes_everything_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default, and a legitimate steady state."""
    _settings(monkeypatch, oidc_issuer="")
    assert not oidc.oidc_configured()
    assert oidc.subject_from_bearer(_req("Bearer anything")) is None


def test_no_network_call_happens_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Off must mean OFF -- an unconfigured deployment should not be making
    outbound requests on its request path."""
    _settings(monkeypatch, oidc_issuer="")

    def _boom(*a: Any, **k: Any) -> dict[str, Any]:
        raise AssertionError("made a network call while unconfigured")

    monkeypatch.setattr(oidc, "_fetch_json", _boom)
    assert oidc.subject_from_bearer(_req("Bearer x")) is None


# --- failures in the plumbing degrade to anonymous, never to an exception ----


def test_a_discovery_failure_is_anonymous_not_an_exception(monkeypatch: pytest.MonkeyPatch, keypair: Any) -> None:
    """resolve_caller must never be able to break a request."""

    def _boom(*a: Any, **k: Any) -> dict[str, Any]:
        raise OSError("issuer unreachable")

    monkeypatch.setattr(oidc, "_fetch_json", _boom)
    oidc.reset_cache()
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair)}")) is None


def test_a_discovery_document_without_jwks_uri_is_anonymous(monkeypatch: pytest.MonkeyPatch, keypair: Any) -> None:
    monkeypatch.setattr(oidc, "_fetch_json", lambda url, timeout: {"issuer": ISSUER})
    oidc.reset_cache()
    assert oidc.subject_from_bearer(_req(f"Bearer {_token(keypair)}")) is None


def test_status_reports_usable_not_merely_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """The failure this repo has had twice is configuration that exists and is
    never read. `issuer_set` and `usable` are different questions and the status
    answers both."""
    _settings(monkeypatch, oidc_audience="")
    status = oidc.oidc_status()
    assert status["issuer_set"] is True
    assert status["usable"] is False
    assert status["audience_set"] is False


# --- the seam: which source wins --------------------------------------------


def _req_both(token: str, header_value: str) -> Request:
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (b"authorization", f"Bearer {token}".encode()),
            (b"x-auth-request-email", header_value.encode()),
        ],
    })


def test_a_verified_token_beats_the_unverified_header(monkeypatch: pytest.MonkeyPatch, keypair: Any) -> None:
    """The ordering that makes token validation worth anything.

    If an unverified header could override a verified token, the header would be
    a way to impersonate anyone -- which is exactly what this module was added to
    stop. So the source carrying evidence wins.
    """
    from viva_api.api import auth

    monkeypatch.setattr(auth, "identity_header_name", lambda: "X-Auth-Request-Email")
    caller = auth.resolve_caller(_req_both(_token(keypair), "impostor@example.test"))
    assert caller == "scientist@example.test"


def test_the_header_still_works_where_no_token_is_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Adding a second source must not remove the first. Most deployments have
    only the header, and nothing about them changes."""
    from viva_api.api import auth

    monkeypatch.setattr(auth, "identity_header_name", lambda: "X-Auth-Request-Email")
    request = Request({
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"x-auth-request-email", b"someone@example.test")],
    })
    assert auth.resolve_caller(request) == "someone@example.test"


def test_an_invalid_token_falls_through_to_the_header(monkeypatch: pytest.MonkeyPatch, keypair: Any) -> None:
    """A deliberate, and debatable, trade. An expired token becomes anonymous
    rather than a 401, because `resolve_caller` must never break a request --
    enforcement is not this module's job. The visible cost is that a caller with
    a stale token looks like a caller with none; the clients cover it from the
    other side, since `atlantis worker submit` warns when the server did not
    record the identity it was given."""
    from viva_api.api import auth

    monkeypatch.setattr(auth, "identity_header_name", lambda: "X-Auth-Request-Email")
    expired = _token(keypair, exp=int(time.time()) - 3600)
    assert auth.resolve_caller(_req_both(expired, "fallback@example.test")) == "fallback@example.test"


def test_an_absurdly_long_subject_is_capped_not_stored_whole(monkeypatch: pytest.MonkeyPatch, keypair: Any) -> None:
    """A verified subject still lands in a database column. The issuer is
    trusted for authenticity, not for restraint."""
    from viva_api.api import auth

    monkeypatch.setattr(auth, "identity_header_name", lambda: "")
    token = _token(keypair, email="x" * 5000)
    caller = auth.resolve_caller(_req(f"Bearer {token}"))
    assert caller is not None
    assert len(caller) == auth.MAX_IDENTITY_LEN
