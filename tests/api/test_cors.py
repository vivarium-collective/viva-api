"""CORS, pinned as a correctness property rather than a preference (#336).

The previous configuration was `allow_origins=["*"]` with
`allow_credentials=True`. That combination is INVALID per the CORS spec -- a
browser rejects `Access-Control-Allow-Origin: *` on any request made with
credentials -- so it did not mean "permissive". It meant "credentialed
cross-origin requests fail", which is a different thing and not what the code
read as. It survived because nothing viva-api serves sends a credential, so
nothing exercised the broken half.

That is the shape worth testing against: a CORS setting that is wrong is
invisible until the first credentialed caller, which by then is a browser
against a deployed environment.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.middleware.cors import CORSMiddleware

from viva_api.api.main import APP_ORIGINS, app


def _cors() -> dict[str, Any]:
    """The CORSMiddleware options as actually installed on the app.

    Compared by name rather than identity: Starlette types `mw.cls` as a
    `_MiddlewareFactory` protocol, so `is CORSMiddleware` is a non-overlapping
    identity check as far as mypy is concerned even though it holds at runtime.
    """
    for mw in app.user_middleware:
        if getattr(mw.cls, "__name__", "") == CORSMiddleware.__name__:
            options: dict[str, Any] = mw.kwargs
            return options
    raise AssertionError("CORSMiddleware is not installed")


def test_wildcard_origin_and_credentials_are_never_both_set() -> None:
    """The specific invalid combination. This is the regression guard: it is a
    one-word edit to reintroduce, and nothing else in the suite would notice."""
    opts = _cors()
    wildcard = "*" in (opts.get("allow_origins") or [])
    assert not (wildcard and opts.get("allow_credentials")), (
        "allow_origins=['*'] with allow_credentials=True is rejected by browsers "
        "per the CORS spec; pick an explicit origin list or drop credentials"
    )


def test_the_origin_list_is_the_one_defined_in_the_module() -> None:
    """APP_ORIGINS sat defined-and-unused above the middleware while `["*"]` was
    passed instead. An allowlist nothing reads is worse than no allowlist: it
    reads as a control that is in force."""
    assert _cors()["allow_origins"] == APP_ORIGINS


def test_credentials_are_off_while_nothing_sends_one() -> None:
    """Not a permanent claim -- a comment at the call site says to revisit when
    #337 lands. Today the API has no cookies, no session and no Authorization
    header, so asserting credential support is asserting an untested capability."""
    assert _cors()["allow_credentials"] is False


def test_the_allowlist_is_not_silently_empty() -> None:
    """An empty list is valid Python and blocks every cross-origin browser
    caller. If APP_ORIGINS is ever pruned to nothing, that should be a decision,
    not a side effect."""
    assert APP_ORIGINS, "APP_ORIGINS is empty"
    assert all(o.startswith(("http://", "https://")) for o in APP_ORIGINS)


def test_no_origin_carries_a_trailing_slash() -> None:
    """An Origin header never has a path or trailing slash, so
    `https://example.com/` matches nothing. It is a silent no-op, which is how a
    stale allowlist entry hides."""
    bad = [o for o in APP_ORIGINS if o.rstrip("/") != o]
    assert not bad, f"origins with a trailing slash never match: {bad}"


@pytest.mark.parametrize("origin", ["https://sms.cam.uchc.edu"])
def test_the_deployed_origin_is_present(origin: str) -> None:
    """The one non-localhost entry. If a deployment's own origin drops off the
    list, cross-origin browser calls there fail with a message that names CORS
    and not the list."""
    assert origin in APP_ORIGINS


# --- what a browser actually receives ---------------------------------------
#
# The assertions above read the installed configuration. These drive the real
# app, because the configuration being right and the middleware behaving right
# are different claims -- and the previous bug was precisely a configuration
# that looked permissive and was not.


async def _headers_for(origin: str) -> dict[str, str]:
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/version", headers={"Origin": origin})
    return {k.lower(): v for k, v in response.headers.items()}


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["https://sms.cam.uchc.edu", "http://localhost:4200"])
async def test_an_allowed_origin_is_echoed_back(origin: str) -> None:
    assert (await _headers_for(origin)).get("access-control-allow-origin") == origin


@pytest.mark.asyncio
async def test_an_unlisted_origin_gets_no_allow_origin_header() -> None:
    """The response still reaches the server -- CORS is enforced in the browser,
    not the server -- but without this header the browser refuses to hand the
    body to the page. That is the drive-by case: a developer with the SSM tunnel
    open on localhost, visiting a page that tries to read their internal API."""
    assert "access-control-allow-origin" not in await _headers_for("https://evil.example.com")


@pytest.mark.asyncio
async def test_credentials_are_never_advertised() -> None:
    """`Access-Control-Allow-Credentials` must be absent for every origin,
    allowed or not, while nothing sends a credential."""
    for origin in ("https://sms.cam.uchc.edu", "https://evil.example.com"):
        assert "access-control-allow-credentials" not in await _headers_for(origin)


@pytest.mark.asyncio
async def test_a_preflight_from_an_unlisted_origin_is_refused() -> None:
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.options(
            "/api/v1/simulations",
            headers={"Origin": "https://evil.example.com", "Access-Control-Request-Method": "POST"},
        )
    assert response.status_code == 400
    assert "access-control-allow-origin" not in {k.lower() for k in response.headers}
