"""OIDC bearer tokens — a *verified* source for the identity seam.

Companion to :mod:`viva_api.api.auth`, which reads an unverified header. This
module reads an ``Authorization: Bearer`` token and checks its signature against
the issuer's published keys, so the identity it returns is one the issuer
actually asserted rather than one the caller typed.

**What this is not.** It is not enforcement. Nothing here makes a token
*required*: viva-api has 79 routes and zero security schemes, and that is
unchanged. This makes an identity *readable* when a deployment configures an
issuer, so ``created_by`` and the cancel rule mean something. Deciding which
routes demand a token is a separate, much larger piece — see
``docs/plan-authentication.md`` Phase 4.

**A verified token over plaintext is still replayable.** The internal ALB
terminates HTTP on port 80, so anyone who can observe traffic inside the VPC can
lift a bearer token and reuse it until it expires. That is the argument in the
plan for why TLS is the precondition rather than a later phase. Shipping this
first is deliberate — it is reversible and unblocked — but it does not by itself
make anything secure.

**Off by default.** With ``OIDC_ISSUER`` unset, every function here is a no-op
returning ``None`` and no network call is ever made. That is the default and a
legitimate steady state.

**Why the JWKS fetch is synchronous.** ``resolve_caller`` is a plain function
called from async handlers, so a fetch on the request path blocks the event loop.
It happens only on a cache miss — a cold start, a lifespan expiry, or a key
rotation — and is bounded by ``OIDC_FETCH_TIMEOUT_SECONDS``. The honest
description is: one request occasionally waits up to that timeout. Making the
seam async would ripple through every call site for a cost paid a few times an
hour, and is the right change only if that stops being true.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx
import jwt
from fastapi import Request
from jwt import PyJWKClient

from viva_api.config import get_settings

__all__ = ["oidc_configured", "oidc_status", "reset_cache", "subject_from_bearer"]

logger = logging.getLogger(__name__)

#: Claims tried in order for the caller's identity. `sub` is the only one an
#: issuer must supply, but it is typically an opaque uuid; a human-readable
#: address is far more useful in an Owner column and in a 403 that has to name
#: who started something. Preference therefore runs readable-first, with `sub`
#: as the guaranteed fallback.
SUBJECT_CLAIMS = ("email", "preferred_username", "sub")

#: Algorithms this module refuses outright, whatever a deployment configures.
#: `none` is the unsigned-token attack; the HMAC family is symmetric and would
#: mean verifying an asymmetric-issuer token against a public key as if it were
#: a shared secret. Neither belongs in an OIDC deployment.
FORBIDDEN_ALGORITHMS = frozenset({"none", "HS256", "HS384", "HS512"})

_LOCK = threading.Lock()
_CLIENTS: dict[str, PyJWKClient] = {}
_JWKS_URIS: dict[str, str] = {}
_WARNED: set[str] = set()


@dataclass(frozen=True)
class OidcConfig:
    issuer: str
    audience: str
    algorithms: tuple[str, ...]
    cache_seconds: int
    leeway_seconds: int
    timeout_seconds: float


def _warn_once(key: str, message: str, *args: Any) -> None:
    """Log a configuration complaint once rather than per request.

    A misconfigured issuer is hit on every single call; logging each one buries
    the rest of the log and tells the reader nothing the first line did not.
    """
    if key in _WARNED:
        return
    _WARNED.add(key)
    logger.warning(message, *args)


def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    """One outbound GET, isolated so it is the single network seam in this module.

    Its own function rather than an inline ``httpx.get`` so a test can replace
    exactly this and nothing else -- patching ``httpx.get`` itself would reach
    every other caller in the process, which is a poor thing for a test to do.
    """
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    document: dict[str, Any] = response.json()
    return document


def _config() -> OidcConfig | None:
    """The validated OIDC settings, or ``None`` when this deployment has none.

    Returns ``None`` — rather than a partially valid config — when the settings
    are inconsistent, and says why once. Half-configured OIDC must not silently
    become weaker OIDC.
    """
    settings = get_settings()
    issuer = (settings.oidc_issuer or "").strip().rstrip("/")
    if not issuer:
        return None

    audience = (settings.oidc_audience or "").strip()
    if not audience:
        # Refusing beats skipping the check. An issuer-only validation accepts a
        # token the same IdP minted for a different relying party, and a
        # deployment that set OIDC_ISSUER plainly meant to turn something on --
        # so failing loudly is more useful than quietly validating less.
        _warn_once(
            "no-audience",
            "OIDC_ISSUER is set (%s) but OIDC_AUDIENCE is not; refusing to validate tokens, "
            "because an issuer-only check would accept tokens minted for other relying parties",
            issuer,
        )
        return None

    algorithms = tuple(a.strip() for a in (settings.oidc_algorithms or "").split(",") if a.strip())
    forbidden = sorted(set(algorithms) & FORBIDDEN_ALGORITHMS)
    if forbidden:
        _warn_once(
            "bad-alg",
            "OIDC_ALGORITHMS contains %s, which this module refuses; tokens will not be validated",
            forbidden,
        )
        return None
    if not algorithms:
        _warn_once("no-alg", "OIDC_ALGORITHMS is empty; tokens will not be validated")
        return None

    if not issuer.startswith("https://"):
        # OIDC Discovery requires an https issuer. Allowed here only for
        # localhost, which is how a test or a local Keycloak is reached.
        host = issuer.split("//", 1)[-1].split("/", 1)[0].split(":", 1)[0]
        if host not in ("localhost", "127.0.0.1"):
            _warn_once(
                "http-issuer",
                "OIDC_ISSUER %s is not https; OIDC Discovery requires it and a token over "
                "plaintext is replayable by anyone who can see the traffic",
                issuer,
            )
            return None

    return OidcConfig(
        issuer=issuer,
        audience=audience,
        algorithms=algorithms,
        cache_seconds=int(settings.oidc_jwks_cache_seconds),
        leeway_seconds=int(settings.oidc_leeway_seconds),
        timeout_seconds=float(settings.oidc_fetch_timeout_seconds),
    )


def oidc_configured() -> bool:
    """Whether this deployment validates bearer tokens at all."""
    return _config() is not None


def _jwks_uri(config: OidcConfig) -> str | None:
    """The issuer's ``jwks_uri``, from its discovery document.

    Discovery is fetched rather than assumed: the JWKS path is not fixed by the
    spec, and guessing ``/protocol/openid-connect/certs`` because Keycloak uses
    it would break the moment a deployment points at anything else — which is
    the substitutability this whole module exists for.
    """
    cached = _JWKS_URIS.get(config.issuer)
    if cached:
        return cached
    url = urljoin(config.issuer + "/", ".well-known/openid-configuration")
    try:
        document = _fetch_json(url, config.timeout_seconds)
    except Exception as e:
        _warn_once(f"discovery:{config.issuer}", "OIDC discovery failed for %s: %s", url, e)
        return None

    uri = document.get("jwks_uri")
    if not isinstance(uri, str) or not uri:
        _warn_once(f"nojwks:{config.issuer}", "OIDC discovery document at %s has no jwks_uri", url)
        return None
    # The document also carries the canonical issuer. If it disagrees with what
    # we were configured with, every `iss` check below would fail on every token
    # -- so say so here, where the cause is visible, rather than at each token.
    stated = str(document.get("issuer") or "").rstrip("/")
    if stated and stated != config.issuer:
        _warn_once(
            f"issmismatch:{config.issuer}",
            "OIDC discovery at %s declares issuer %r but OIDC_ISSUER is %r; tokens will fail the iss check",
            url,
            stated,
            config.issuer,
        )
    _JWKS_URIS[config.issuer] = uri
    return uri


def _client(config: OidcConfig) -> PyJWKClient | None:
    """The cached JWKS client for this issuer.

    ``PyJWKClient`` caches by ``kid`` and refetches when it sees an unknown one,
    which is what makes key rotation work without a restart.
    """
    with _LOCK:
        existing = _CLIENTS.get(config.issuer)
        if existing is not None:
            return existing
        uri = _jwks_uri(config)
        if uri is None:
            return None
        client = PyJWKClient(
            uri,
            cache_keys=True,
            cache_jwk_set=True,
            lifespan=config.cache_seconds,
            timeout=int(config.timeout_seconds),
        )
        _CLIENTS[config.issuer] = client
        return client


def reset_cache() -> None:
    """Drop discovery, key and warning caches. For tests and settings changes."""
    with _LOCK:
        _CLIENTS.clear()
        _JWKS_URIS.clear()
        _WARNED.clear()


def bearer_token(request: Request) -> str | None:
    """The ``Authorization: Bearer`` token, or ``None``.

    Scheme match is case-insensitive because RFC 6750 says it is, and clients
    send ``bearer`` more often than one would hope.
    """
    header = request.headers.get("Authorization") or ""
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer":
        return None
    return token.strip() or None


def subject_from_bearer(request: Request) -> str | None:
    """The verified subject of this request's bearer token, or ``None``.

    Never raises, matching :func:`viva_api.api.auth.resolve_caller`'s contract:
    reading identity must not be able to break a request. An invalid token is
    therefore anonymous rather than a 401 — enforcement is not this module's job.

    That trade has a visible cost worth naming: a caller with an expired token
    looks exactly like a caller with no token. The clients cover this from the
    other side — `atlantis worker submit` warns when the server did not record
    the identity it was given — and the rejection is logged here at INFO.
    """
    config = _config()
    if config is None:
        return None
    token = bearer_token(request)
    if token is None:
        return None
    client = _client(config)
    if client is None:
        return None

    try:
        signing_key = client.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=list(config.algorithms),
            audience=config.audience,
            issuer=config.issuer,
            leeway=config.leeway_seconds,
            options={
                "require": ["exp", "iss", "aud"],
                "verify_signature": True,
                "verify_exp": True,
                "verify_nbf": True,
                "verify_iat": True,
                "verify_aud": True,
                "verify_iss": True,
            },
        )
    except jwt.PyJWTError as e:
        # INFO, not WARNING: a rejected token is a normal event on a public
        # endpoint (expiry, a client pointed at the wrong realm), and warning on
        # each would make the log useless for the misconfiguration case above.
        logger.info("rejected a bearer token: %s: %s", type(e).__name__, e)
        return None
    except Exception as e:
        _warn_once(f"keyfetch:{config.issuer}", "could not fetch signing keys for %s: %s", config.issuer, e)
        return None

    for claim in SUBJECT_CLAIMS:
        value = claims.get(claim)
        if isinstance(value, str) and value.strip():
            return value.strip()
    logger.info("bearer token verified but carries none of %s", ", ".join(SUBJECT_CLAIMS))
    return None


def oidc_status() -> dict[str, Any]:
    """A description of the OIDC configuration, for startup logging.

    Deliberately says whether it is *usable*, not just whether it is *set*: the
    failure this repository has already had twice is configuration that exists
    and is never read, which looks identical to no configuration at all.
    """
    settings = get_settings()
    issuer = (settings.oidc_issuer or "").strip()
    config = _config()
    return {
        "issuer_set": bool(issuer),
        "usable": config is not None,
        "issuer": config.issuer if config else issuer,
        "audience_set": bool((settings.oidc_audience or "").strip()),
        "algorithms": list(config.algorithms) if config else [],
    }
