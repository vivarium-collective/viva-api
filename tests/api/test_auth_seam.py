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
    assert "IDENTITY_HEADER" in ei.value.detail
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


# --- the env var name, which was wrong in production for a day ---------------


def test_the_setting_reads_the_env_var_its_docs_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deployed once as `VIVA_API_IDENTITY_HEADER` -- the name used in the plan
    and in every docstring around it -- which pydantic-settings silently ignored,
    because these Settings carry no `env_prefix`. The variable was on the pod,
    `settings.identity_header` was still `""`, and the entire seam was dead
    config: tasks recorded `created_by = NULL` and the cancel rule could never
    fire. Nothing failed; it just did nothing.

    So assert the binding itself, not the prose about it.
    """
    from viva_api.config import Settings

    monkeypatch.setenv("IDENTITY_HEADER", "X-Auth-Request-Email")
    assert Settings().identity_header == "X-Auth-Request-Email"


def test_the_prefixed_name_is_NOT_what_is_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """The other half, and the one that would have caught it: a prefixed name
    must not appear to work. If someone adds an env_prefix or an alias later,
    this fails and the overlays get revisited deliberately."""
    from viva_api.config import Settings

    monkeypatch.delenv("IDENTITY_HEADER", raising=False)
    monkeypatch.setenv("VIVA_API_IDENTITY_HEADER", "X-Auth-Request-Email")
    assert Settings().identity_header == "", (
        "a prefixed env var now sets this; kustomize/overlays/*/kustomization.yaml "
        "name the unprefixed one and must be updated together"
    )


def test_every_env_var_the_overlay_sets_is_actually_read() -> None:
    """Generalises it: an env var nobody reads is dead config, and dead config
    fails silently -- which is how both this bug and the earlier
    ENV_WORKER_WORKSPACE_PATH one survived a whole rollout.

    "Read" means EITHER a Settings field of that name (no env_prefix, so the
    field name uppercased) OR a direct `os.environ` lookup somewhere in
    viva_api. The second is not hypothetical: ENV_WORKER_RELAY_ADVERTISE_HOST is
    read straight from the environment in the env-worker router, deliberately,
    and this test found it while asserting the narrower rule. Two mechanisms is
    itself worth knowing about; the invariant that matters is that SOMETHING
    consumes the name.
    """
    import re
    from pathlib import Path

    from viva_api.config import Settings

    repo = Path(__file__).resolve().parents[2]
    fields = {name.upper() for name in Settings.model_fields}
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (repo / "viva_api").rglob("*.py")
        if "api/client/" not in path.as_posix()
    )
    overlay = (repo / "kustomize" / "overlays" / "sms-api-stanford-test" / "kustomization.yaml").read_text(
        encoding="utf-8"
    )
    # Capture the WHOLE name. The first version matched
    # `(IDENTITY_HEADER|ENV_WORKER_[A-Z_]+)` unanchored, so reintroducing the bug
    # -- `VIVA_API_IDENTITY_HEADER` -- still matched, on the substring, and the
    # test passed against the very mistake it was written for.
    named = set(re.findall(r"- name: ([A-Z][A-Z0-9_]*)", overlay))
    # Only the names this repo owns; VIVARIUM_* belongs to the workbench, and
    # K8s/AWS/Postgres inject plenty of their own.
    ours = {n for n in named if "IDENTITY_HEADER" in n or n.startswith("ENV_WORKER")}
    unread = sorted(n for n in ours if n not in fields and f'"{n}"' not in source)
    assert not unread, f"overlay sets env vars nothing reads: {unread}"
