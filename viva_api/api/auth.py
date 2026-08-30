"""Caller identity — a seam, not a security boundary.

**Read this before using anything here.** viva-api performs no authentication.
There are no security schemes, no token verification, and CORS is currently
``allow_origins=["*"]``. This module does not change that. It answers one
question — *who is claiming to make this request* — so that work can be
attributed and so that one destructive operation can refuse an obvious accident.

**Why a header, and why a configurable one.** The deployments differ in a way
that rules out anything cleverer:

* **Stanford dev/prod** sit behind an internal ALB reached over an SSM tunnel, so
  the caller does hold AWS credentials — but nothing puts them in the request.
* **A customer installation** lands its ALB in the customer's own institutional
  network. Those users hold **no AWS credentials at all**, so every AWS-keyed
  scheme (SigV4, ``sts:GetCallerIdentity``, CloudTrail attribution) is
  unavailable there. Anything built on AWS identity would exclude them.
* **`sms.cam.uchc.edu`**, the UConn install, answers from the public internet
  with no credentials of any kind (verified 2026-08-29).

What every one of those *can* have is a proxy that sets a header —
institutional SSO, ``oauth2-proxy`` (``X-Auth-Request-Email``), an ALB OIDC
action (``X-Amzn-Oidc-Identity``). So the header's NAME is configuration
(``IDENTITY_HEADER``) and its absence is the default.

**The honest limits, stated once so they are not rediscovered as a surprise:**

* A header is exactly as trustworthy as the proxy that sets it. Where nothing
  sets one, any caller may claim to be anyone.
* This therefore prevents **accidents**, not adversaries — someone cancelling a
  colleague's six-hour study by mistake, not someone determined to.
* It is not a login, not a session, and not an authorization framework. The only
  rule keyed on it is "you cannot cancel a task you did not start".

If real authentication arrives, it replaces :func:`resolve_caller` and everything
above it keeps working — which is the point of putting the question behind one
function now rather than threading a header through call sites later.
"""

from __future__ import annotations

from fastapi import HTTPException, Request

from viva_api.config import get_settings

__all__ = ["identity_header_name", "require_caller", "resolve_caller"]

#: Cap on an accepted identity. A header is unverified input that lands in a
#: database column and in logs; something absurd is a mistake or an abuse, and
#: either way is better refused than stored.
MAX_IDENTITY_LEN = 256


def identity_header_name() -> str:
    """The header this deployment reads identity from, or ``""`` if none.

    Empty is the default and a legitimate steady state: a deployment with no
    identity-setting proxy in front of it has no identity to offer, and saying so
    is more useful than inventing one.
    """
    return (get_settings().identity_header or "").strip()


def resolve_caller(request: Request) -> str | None:
    """Who this request claims to be, or ``None`` for anonymous.

    Never raises and never refuses: reading identity must not be able to break a
    request, because on most deployments there is none to read and that is
    normal. Callers decide what to do with ``None``.
    """
    header = identity_header_name()
    if not header:
        return None
    value = (request.headers.get(header) or "").strip()
    if not value or len(value) > MAX_IDENTITY_LEN:
        # An over-long value is dropped rather than truncated: a truncated
        # identity is a DIFFERENT identity, and silently storing one would make
        # ownership checks compare against something the caller never sent.
        return None
    return value


def require_caller(request: Request) -> str:
    """Identity or 401 — for the few operations that DESTROY something.

    Used only where anonymity would make an ownership rule decorative: if an
    anonymous caller could cancel anyone's work, omitting the header would bypass
    the rule entirely. Reading and submitting stay open; destroying asks who you
    are.

    The 401 names the header when one is configured, because "unauthorized" with
    no indication of what would satisfy it is a dead end for whoever hits it.
    """
    caller = resolve_caller(request)
    if caller:
        return caller
    header = identity_header_name()
    if not header:
        raise HTTPException(
            401,
            "this deployment records no caller identity "
            "(IDENTITY_HEADER is unset), so ownership cannot be "
            "established; cancel it from the client that started it, or "
            "configure an identity header",
        )
    raise HTTPException(401, f"identify yourself to cancel: send the {header} header")
