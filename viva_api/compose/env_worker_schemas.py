"""What the env-worker endpoints accept and return.

Lifted VERBATIM out of ``viva_api/api/routers/env_worker.py`` (``docs/plan-core.md`` P3b): that file
held 22 models beside its routes, and a router that holds models cannot be moved -- everything that
wants a model has to import the HTTP layer to get it. They live with the env-worker service now, and
move into core with it. The section comments that explain the ENDPOINTS stayed with the endpoints.
"""

from pydantic import BaseModel, Field


class EnvWorkerStartRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """Where to dial back, and which environment to run.

    ``commit`` selects the environment: it is the tag of the prebuilt simulator
    image, which under §2A.8 *is* the execution environment rather than a recipe
    for rebuilding one.
    """

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    callback_host: str = Field(..., description="Host/IP the worker dials back to (the workbench pod IP)")
    callback_port: int = Field(..., ge=1, le=65535, description="Port the workbench is listening on")
    token: str = Field(..., description="One-time handshake token the worker must present")
    workspace: str | None = Field(
        None,
        description="Workspace path inside the worker container; defaults to the "
        "deployment's env_worker_workspace_path (the image's own checkout)",
    )
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")


class EnvWorkerStartResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    job_name: str
    image: str
    namespace: str


class EnvWorkerStatusResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    job_name: str
    status: str | None = None
    exists: bool = True
    logs: str | None = None


class RelayStartRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """Start a worker that dials back to *viva-api* rather than to the caller."""

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    workspace: str | None = Field(None, description="Workspace path inside the worker container")
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")
    accept_timeout: float = Field(
        300.0, gt=0, le=1800, description="Seconds to wait for the worker to dial back (pod schedule + image pull)"
    )


class RelayStartResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    job_name: str
    image: str
    namespace: str
    connected: bool


class RelayCallRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    method: str = Field(..., description="Worker method name (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")
    timeout: float = Field(300.0, gt=0, le=3600, description="Seconds to wait for this call's reply")


class RelayCallResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    result: object | None = None


class _WorkerBody(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """Passthrough base: declared fields are validated, unknown ones forwarded."""

    model_config = {"extra": "allow"}

    def to_params(self) -> dict[str, object]:
        """Body -> worker params, dropping keys the caller did not send.

        `exclude_none` matters: several worker handlers branch on PRESENCE
        (`if ref is not None`), so forwarding an explicit null would take a
        different path than omitting the field.

        `by_alias` matters more, and less visibly. `CompositeSelector.schema_`
        carries `alias="schema"` because a bare `schema` shadows a BaseModel
        attribute; without `by_alias` the dump emits `schema_`, the worker never
        sees the `schema` it looks for, and `observables` silently takes its
        `ref` branch on a request that supplied an inline state. Nothing raises
        -- the answer is just about a different composite.
        """
        return self.model_dump(exclude_none=True, by_alias=True)


class CompositeRef(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A composite named by a registered generator, optionally with overrides."""

    ref: str = Field(..., min_length=1, description="Registered @composite_generator name")
    overrides: dict[str, object] | None = Field(None, description="Generator parameter overrides")


class InnerCompositeRef(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """`hops` is a LIST OF NODE PATHS, each itself a list of key segments -- which
    is why this is a POST and not the GET the plan first assumed."""

    ref: str = Field(..., min_length=1)
    hops: list[list[str]] = Field(..., min_length=1, description="One node path per drill level")


class ConfigDocument(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    config: dict[str, object] = Field(..., description="vEcoli-style config to translate")


class StateDocument(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    document: dict[str, object] = Field(..., description="An already-resolved composite state")
    ref: str | None = Field(None, description="Generator whose core_extensions resolve bare addresses")


class CompositeSelector(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A composite given EITHER by `ref` OR inline as `{state, schema}`.

    Both forms are real and the worker accepts either; sending neither is the
    mistake worth catching here, because the worker answers it with
    `__not_registered__` -- which reads as "your ref is wrong" to a caller who
    sent no ref at all.
    """

    ref: str | None = None
    state: dict[str, object] | None = None
    schema_: dict[str, object] | None = Field(None, alias="schema")

    def model_post_init(self, __context: object) -> None:
        if self.ref is None and self.state is None:
            raise ValueError("provide either 'ref' (a registered generator) or an inline 'state' (with 'schema')")


class ReadoutCheck(CompositeSelector):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    spec: dict[str, object] = Field(..., description="The study spec whose readouts are checked")


class ProcessAddress(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    address: str = Field(..., min_length=1, description="Registry address of a Process or Step")
    config: dict[str, object] | None = None


class ProcessRun(ProcessAddress):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """One `update()` -- a probe, not a job. `env_worker._run_process` is
    deliberately NOT job-class: it builds one class, fills its ports and runs a
    single step, which is the Composite Explorer's "try this process" button."""

    inputs: dict[str, object] | None = None
    interval: float | None = None


class VizDoc(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    viz_doc: dict[str, object] = Field(..., description="A visualization composite document")


class VizPreview(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    address: str = Field(..., min_length=1, description="Visualization class address")
    config: dict[str, object] | None = None
    source: str | None = Field(None, description="demo | streaming | investigation")
    note_prefix: str | None = None
    investigation_inputs_store: dict[str, object] | None = None


class ViewerLaunch(_WorkerBody):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """`analysis_viewers` carries two operations behind an `action` flag. They are
    split into two routes here: listing is a read, launching invokes a
    contributor's callable. One endpoint with a mode string would hide that."""

    uid: str = Field(..., min_length=1, description="Viewer uid from the listing")
    study: str | None = None
    run: str | None = None
    ctx: dict[str, object] | None = None


class TaskSubmitRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    job_name: str = Field(..., description="Relayed worker Job to run this on")
    method: str = Field(..., description="Worker method (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")


class TaskResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    task_id: int
    job_name: str
    method: str
    status: str
    result: object | None = None
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None


class TaskStatusResponse(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A task WITHOUT its result — the shape the batch endpoint returns.

    Measured on dev before this existed: five tasks came back as **1.19 MB**,
    of which 1.185 MB was result payloads. The endpoint exists so a campaign can
    be polled without N round trips; inlining every result defeats exactly that,
    and the cost grows with both the number of tasks and the size of what they
    returned. Polling twenty tasks every few seconds would have moved megabytes
    per cycle over an SSM tunnel.

    So a *status* endpoint returns status. Fetch the singular
    ``GET /tasks/{id}`` for the payload, which is one request at the one moment
    a caller actually wants it.
    """

    task_id: int
    job_name: str
    method: str
    status: str
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    #: Whether a result is waiting, without shipping it. Lets a poller know it is
    #: worth one GET rather than guessing from `status`.
    has_result: bool = False
