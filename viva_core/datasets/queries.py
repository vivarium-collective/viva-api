"""Query-string parsing for the dataset reads: pure, and the application's routes share it.

The application's ``/api/v1/datasets`` parsed these the same way (``viva_api/common/handlers/
datasets.py``); the functions moved here verbatim (P4a-2, slice 3) and the application imports
them back. What did NOT move is the application's ``source`` shorthands (``sim:1002``): core takes
``<kind>:<ref>`` as given, an application maps its own abbreviations before calling.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Container
from dataclasses import dataclass
from typing import Literal

from pydantic import JsonValue

from viva_core.datasets.models import DatasetQuery, JsonDict, OwnerRef

Availability = Literal["true", "false", "any"]
AVAILABILITY: dict[str, bool | None] = {"true": True, "false": False, "any": None}


class DatasetQueryError(ValueError):
    """A malformed dataset filter (400)."""


def parse_tags(tag: str | None) -> list[str] | None:
    """Comma-separated tags, blanks dropped; ``None`` when there are none."""
    tags = [part.strip() for part in (tag or "").split(",") if part.strip()]
    return tags or None


def naive_utc(moment: datetime.datetime | None) -> datetime.datetime | None:
    """``updated_at`` columns are naive UTC: convert an aware ``since``, take a naive one as UTC."""
    if moment is None or moment.tzinfo is None:
        return moment
    return moment.astimezone(datetime.UTC).replace(tzinfo=None)


def parse_attribute_value(raw: str) -> JsonValue:
    """A filter value typed the way JSONB containment compares it.

    A JSON scalar keeps its type (``0`` is an integer, ``true`` a boolean, ``"0"`` a string);
    anything that is not JSON (``000``, ``ptools_rna``) is a string."""
    try:
        value = json.loads(raw)
    except ValueError:
        return raw
    return value if isinstance(value, str | int | float | bool) else raw


def parse_attribute_filters(pairs: list[str], query_items: list[tuple[str, str]]) -> JsonDict | None:
    """``attr=<key>=<value>`` pairs plus ``attr.<key>=<value>`` parameters, as one containment filter."""
    attributes: JsonDict = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep or not key.strip():
            raise DatasetQueryError(f"attr filter {pair!r} is not <key>=<value>")
        attributes[key.strip()] = parse_attribute_value(raw)
    for name, raw in query_items:
        key = name.removeprefix("attr.")
        if key != name and key:
            attributes[key] = parse_attribute_value(raw)
    return attributes or None


def parse_source_filter(source: str | None) -> JsonDict | None:
    """A ``source`` filter -- what the data is OF: ``<kind>:<ref>``, or a JSON fragment of the
    stored ``source`` object (matched by containment)."""
    text = (source or "").strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            value = json.loads(text)
        except ValueError as e:
            raise DatasetQueryError(f"source {source!r} is not valid JSON") from e
        if not isinstance(value, dict):
            raise DatasetQueryError(f"source {source!r} is not a JSON object")
        return value
    kind, sep, ref = text.partition(":")
    if not sep or not kind.strip() or not ref.strip():
        raise DatasetQueryError(f"source {source!r} is not <kind>:<ref> or a JSON object")
    return {"kind": kind.strip(), "ref": ref.strip()}


def parse_owner(owner: str | None) -> OwnerRef | None:
    """An ``owner`` filter: ``<owner_kind>:<owner_id>`` -- the owning record's kind (the
    application's table name) and its id."""
    text = (owner or "").strip()
    if not text:
        return None
    kind, sep, ref = text.partition(":")
    if not sep or not kind.strip() or not ref.strip():
        raise DatasetQueryError(f"owner {owner!r} is not <owner_kind>:<owner_id>")
    return {"owner_kind": kind.strip(), "owner_id": ref.strip()}


def check_kind(kind: str | None, kinds: Container[str]) -> None:
    if kind is not None and kind not in kinds:
        raise DatasetQueryError(f"unknown dataset kind {kind!r}")


@dataclass(frozen=True)
class DatasetListParams:
    """The filters every dataset listing shares, parsed from the query string."""

    kind: str | None
    view: str | None
    tags: list[str] | None
    available: bool | None
    since: datetime.datetime | None
    uri_prefix: str | None
    q: str | None
    limit: int
    offset: int

    def query(self) -> DatasetQuery:
        return {
            "kind": self.kind,
            "view": self.view,
            "tags": self.tags,
            "available": self.available,
            "since": self.since,
            "uri_prefix": self.uri_prefix,
            "q": self.q,
        }
