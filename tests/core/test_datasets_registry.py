"""The trace feeder in core: ``artifact.written`` events become dataset rows through the
``OwnerResolver`` and ``DatasetStore`` Protocols (plan P4a-2, slice 1).

Pure and fast: an in-memory store and a scripted resolver stand in for the application. The
application's own tests (``tests/simulation/test_dataset_registry.py``) run the same rules end to
end through the event ingester against Postgres with its resolver and store.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from viva_core.datasets.models import DatasetRecord, DatasetStore, DatasetWrite, JsonDict, OwnerRef, UpsertAction
from viva_core.datasets.registry import (
    ArtifactEvent,
    OwnerResolver,
    OwnerUnresolved,
    RunContext,
    dataset_fields,
    register_datasets,
)

KINDS = ("table", "figure", "store", "log")
OWNER: OwnerRef = {"owner_kind": "run", "owner_id": "7"}


@dataclass
class _Row:
    database_id: int
    uri: str
    available: bool
    write: DatasetWrite
    owner: OwnerRef


@dataclass
class _Store:
    """A ``DatasetStore`` keyed on uri; an identical rewrite is ``unchanged``; ``refuse`` names
    uris the store rejects with ``ValueError``."""

    rows: dict[str, _Row] = field(default_factory=dict)
    refuse: set[str] = field(default_factory=set)

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]:
        uri = write["uri"]
        if uri in self.refuse:
            raise ValueError("refused by the store")
        existing = self.rows.get(uri)
        if existing is None:
            row = _Row(len(self.rows) + 1, uri, write.get("available", True), write, owner)
            self.rows[uri] = row
            return row, "inserted"
        if existing.write == write and existing.owner == owner:
            return existing, "unchanged"
        existing.write, existing.owner, existing.available = write, owner, write.get("available", True)
        return existing, "updated"


class _Resolver:
    """An ``OwnerResolver`` that answers ``OWNER`` unless the kind is in ``unresolved``."""

    def __init__(self, unresolved: tuple[str, ...] = ()) -> None:
        self.unresolved = unresolved
        self.asked: list[tuple[str, int]] = []

    async def resolve(self, kind: str, event: ArtifactEvent) -> OwnerRef:
        self.asked.append((kind, event.seq))
        if kind in self.unresolved:
            raise OwnerUnresolved(f"seq {event.seq}: nothing owns a {kind}")
        return OWNER


def _event(seq: int, payload: JsonDict, **overrides: object) -> ArtifactEvent:
    kwargs: dict[str, object] = {"seq": seq, "payload": payload, "span_id": "aaaabbbbccccdddd"}
    kwargs.update(overrides)
    return ArtifactEvent(**kwargs)  # type: ignore[arg-type]


RUN = RunContext(run_id=42, label="exp-1", tags=["cd2"], subject={"kind": "subject", "ref": "12", "resolved_id": 12})


def test_the_protocols_are_satisfied_structurally() -> None:
    store: DatasetStore = _Store()
    resolver: OwnerResolver = _Resolver()
    assert store is not None and resolver is not None


@pytest.mark.asyncio
async def test_a_run_registers_what_it_wrote_with_its_coordinate_tags_and_subject() -> None:
    store, resolver = _Store(), _Resolver()
    events = [
        _event(
            1,
            {"uri": "s3://b/out/history/variant=0/gen=1/", "kind": "store", "attributes": {"table": "history"}},
            coordinate={"variant": 0, "generation": 1},
        ),
        _event(
            2,
            {
                "uri": "s3://b/out/a/view__variant=0.tsv",
                "kind": "table",
                "name": "view__variant=0.tsv",
                "view": "view",
                "bytes": 2048,
                "attributes": {"protocol": "multiseed", "variant": 0, "n_tp": 8, "tags": ["run3"]},
            },
        ),
    ]

    result = await register_datasets(events, run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)

    assert (result.registered, result.unchanged, result.skipped) == (2, 0, 0)
    assert resolver.asked == [("store", 1), ("table", 2)]
    first = store.rows["s3://b/out/history/variant=0/gen=1/"]
    assert first.owner == OWNER
    assert first.write["kind"] == "store" and first.write["origin"] == "event"
    attributes = first.write["attributes"]
    assert attributes["variant"] == 0 and attributes["generation"] == 1
    assert attributes["hpcrun_id"] == 42 and attributes["span_id"] == "aaaabbbbccccdddd"
    assert first.write["source"] == {
        "kind": "subject",
        "ref": "12",
        "resolved_id": 12,
        "coordinate": {"variant": 0, "generation": 1},
    }

    second = store.rows["s3://b/out/a/view__variant=0.tsv"]
    assert second.write["view"] == "view" and second.write["size_bytes"] == 2048
    assert second.write["attributes"]["n_tp"] == 8 and second.write["attributes"]["name"] == "view__variant=0.tsv"
    assert "tags" not in second.write["attributes"]
    assert second.write["tags"] == ["cd2", "run3"]
    assert second.write["display_name"] == "exp-1 · view · multiseed"


def test_explicit_attributes_win_over_the_promoted_coordinate() -> None:
    event = _event(
        1, {"uri": "s3://b/x", "kind": "table", "attributes": {"seed": 5}}, coordinate={"seed": 1, "variant": 2}
    )
    fields = dataset_fields(event, run=RUN, kinds=KINDS)
    assert fields["source"] is not None
    assert fields["source"]["coordinate"] == {"variant": 2, "seed": 5}
    assert fields["attributes"]["seed"] == 5 and fields["attributes"]["variant"] == 2


def test_display_name_falls_back_to_the_event_label_then_to_the_kind() -> None:
    bare = RunContext(run_id=1)
    labelled = dataset_fields(_event(1, {"uri": "s3://b/x", "kind": "figure"}, label="exp-9"), run=bare, kinds=KINDS)
    assert labelled["display_name"] == "exp-9 · figure"
    assert labelled["source"] is None and labelled["tags"] == []
    unlabelled = dataset_fields(_event(1, {"uri": "s3://b/x", "kind": "figure"}), run=bare, kinds=KINDS)
    assert unlabelled["display_name"] == "figure"
    explicit = dataset_fields(
        _event(1, {"uri": "s3://b/x", "kind": "figure", "display_name": "Fig 1"}), run=RUN, kinds=KINDS
    )
    assert explicit["display_name"] == "Fig 1"


@pytest.mark.asyncio
async def test_a_failed_artifact_registers_unavailable_and_a_later_success_flips_it() -> None:
    store, resolver = _Store(), _Resolver()
    uri = "s3://b/out/a/failed.tsv"
    failure = _event(1, {"uri": uri, "kind": "table", "error": "OOM at 41.0 GiB"})
    await register_datasets([failure], run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)
    assert store.rows[uri].available is False
    assert store.rows[uri].write["attributes"]["error"] == "OOM at 41.0 GiB"

    success = _event(2, {"uri": uri, "kind": "table"})
    result = await register_datasets([failure, success], run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)
    assert (result.registered, result.unchanged) == (1, 1)
    assert store.rows[uri].available is True


@pytest.mark.asyncio
async def test_invalid_artifacts_are_skipped_and_counted_without_stopping_the_rest() -> None:
    store, resolver = _Store(refuse={"s3://b/refused"}), _Resolver(unresolved=("log",))
    events = [
        _event(1, {"uri": "/local/path.tsv", "kind": "table"}),
        _event(2, {"uri": "s3://b/bad-kind", "kind": "spreadsheet"}),
        _event(3, {"uri": "s3://b/bad-attrs", "kind": "figure", "attributes": ["x"]}),
        _event(4, {"uri": "s3://b/unowned", "kind": "log"}),
        _event(5, {"uri": "s3://b/refused", "kind": "table"}),
        _event(6, {"uri": "s3://b/good", "kind": "figure"}),
    ]

    result = await register_datasets(events, run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)

    assert (result.registered, result.skipped) == (1, 5)
    assert list(store.rows) == ["s3://b/good"]
    assert [reason.split(":")[0] for reason in result.reasons] == ["seq 1", "seq 2", "seq 3", "seq 4", "seq 5"]
    assert "uri must be an s3:// URI" in result.reasons[0]
    assert "unknown kind 'spreadsheet'" in result.reasons[1]
    assert "attributes must be an object" in result.reasons[2]
    assert "nothing owns a log" in result.reasons[3]
    assert "refused by the store" in result.reasons[4]
    # The resolver is asked only for events whose payload validated.
    assert [seq for _kind, seq in resolver.asked] == [4, 5, 6]


@pytest.mark.asyncio
async def test_registering_the_same_events_twice_changes_nothing() -> None:
    store, resolver = _Store(), _Resolver()
    events = [_event(1, {"uri": "s3://b/idempotent", "kind": "table", "attributes": {"n_tp": 5}})]
    first = await register_datasets(events, run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)
    again = await register_datasets(events, run=RUN, owner_resolver=resolver, store=store, kinds=KINDS)
    assert (first.registered, again.registered, again.unchanged) == (1, 0, 1)


@pytest.mark.asyncio
async def test_a_store_failure_that_is_not_a_refusal_propagates() -> None:
    class _Broken(_Store):
        async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]:
            raise RuntimeError("database went away")

    with pytest.raises(RuntimeError, match="database went away"):
        await register_datasets(
            [_event(1, {"uri": "s3://b/x", "kind": "table"})],
            run=RUN,
            owner_resolver=_Resolver(),
            store=_Broken(),
            kinds=KINDS,
        )
