# Simulation Filtering

*Available since v0.8.3*

The `GET /api/v1/simulations` endpoint supports optional filtering parameters that let you restrict which simulation specs are returned. This is useful when you want to work with a specific subset of simulations — for example, isolating a particular experiment group or scientific study.

## Filter Parameters

There are two orthogonal filter parameters that can be used independently or combined:

| Parameter | Type | Description |
|-----------|------|-------------|
| `experiment_id` | `string` | Comma-separated list of experiment IDs to include. Example: `sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617` |
| `tag` | `string` | Predefined tag name that resolves to a bundle of experiment IDs. Example: `cd1`. Use `GET /api/v1/simulations/tags` to list available tags. |

**Behavior:**
- Both parameters are optional. When neither is provided, all simulations are returned (backwards compatible).
- If both are provided, the result is the **union** of both sets.
- Unknown tags return a `400 Bad Request` with a list of available tags.
- Experiment IDs that don't exist in the database are silently ignored (no error).

## Tags Discovery Endpoint

```http
GET /api/v1/simulations/tags
```

Returns a mapping of all available tag names to their experiment ID lists:

```json
{
  "cd1": [
    "sim31-baseline-60bb",
    "sim33-violacien-seeds1000-generations10-9617",
    "sim33-mecillinam-seeds84-generations10-036f"
  ]
}
```

Tags are defined in code at `viva_api/simulation/simulation_tags.py` and are zero-migration (no database schema changes needed).

## Examples

All examples below use the base URL `https://sms.cam.uchc.edu/api/v1` (the public academic API at UCONN CCAM).

### No filters (backwards compatible)

Returns all simulation specs in the database:

```bash
curl "https://sms.cam.uchc.edu/api/v1/simulations"
```

```json
[
  {
    "id": 1,
    "experiment_id": "sim31-baseline-60bb",
    "simulator_id": "vEcoli",
    "simulation_config_filename": "config/vEcoli/glc_37C.yaml",
    "num_generations": 10,
    "num_seeds": 100,
    "composite": "vEcoli",
    "condition": "glc_37C",
    "created_at": "2024-01-15T10:30:00Z"
  },
  ...
]
```

### Filter by experiment IDs

Return only the specified experiments:

```bash
curl "https://sms.cam.uchc.edu/api/v1/simulations?experiment_id=sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617"
```

### Filter by tag

Return the `cd1` bundle (three specific experiments available on the Stanford GovCloud deployment):

```bash
curl "https://sms.cam.uchc.edu/api/v1/simulations?tag=cd1"
```

```json
[
  {
    "id": 42,
    "experiment_id": "sim31-baseline-60bb",
    "simulator_id": "vEcoli",
    "simulation_config_filename": "config/vEcoli/glc_37C.yaml",
    "num_generations": 10,
    "num_seeds": 100,
    "composite": "vEcoli",
    "condition": "glc_37C",
    "created_at": "2024-01-15T10:30:00Z"
  },
  {
    "id": 43,
    "experiment_id": "sim33-violacien-seeds1000-generations10-9617",
    "simulator_id": "vEcoli",
    "simulation_config_filename": "config/vEcoli/glc_37C.yaml",
    "num_generations": 10,
    "num_seeds": 1000,
    "composite": "vEcoli",
    "condition": "glc_37C",
    "created_at": "2024-01-20T14:22:00Z"
  },
  {
    "id": 44,
    "experiment_id": "sim33-mecillinam-seeds84-generations10-036f",
    "simulator_id": "vEcoli",
    "simulation_config_filename": "config/vEcoli/glc_37C.yaml",
    "num_generations": 10,
    "num_seeds": 84,
    "composite": "vEcoli",
    "condition": "glc_37C",
    "created_at": "2024-01-22T09:15:00Z"
  }
]
```

### Combine both filters

Union of explicit experiment IDs and the `cd1` tag:

```bash
curl "https://sms.cam.uchc.edu/api/v1/simulations?experiment_id=sim99-custom-abc123&tag=cd1"
```

### List available tags

```bash
curl "https://sms.cam.uchc.edu/api/v1/simulations/tags"
```

```json
{
  "cd1": [
    "sim31-baseline-60bb",
    "sim33-violacien-seeds1000-generations10-9617",
    "sim33-mecillinam-seeds84-generations10-036f"
  ]
}
```

## CLI Usage

The Atlantis CLI exposes the same filters via the `simulation list` command:

```bash
# List all simulations (backwards compatible)
uv run atlantis simulation list

# Filter by experiment IDs
uv run atlantis simulation list --experiment-id sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617

# Filter by tag
uv run atlantis simulation list --tag cd1

# Combine both
uv run atlantis simulation list --experiment-id sim99-custom-abc123 --tag cd1

# List available tags
uv run atlantis simulation tags
```

Output format matches the REST API (JSON by default, or use `--format table` for a pretty table).

## Python Client Usage

Using the auto-generated OpenAPI client:

```python
from viva_api.api.client import Client
from viva_api.api.client.api.simulations import list_ecoli_simulations

client = Client(base_url="https://sms.cam.uchc.edu/api/v1")

# No filters
all_sims = list_ecoli_simulations.sync(client=client)

# By experiment IDs
filtered = list_ecoli_simulations.sync(
    client=client,
    experiment_id="sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617"
)

# By tag
cd1_sims = list_ecoli_simulations.sync(client=client, tag="cd1")

# List tags
from viva_api.api.client.api.simulations import list_simulation_tags
tags = list_simulation_tags.sync(client=client)
print(tags)  # {"cd1": ["sim31-baseline-60bb", ...]}
```

## Deployment Notes

- The `cd1` tag bundle contains experiments that **only exist on the `sms-api-stanford` (GovCloud/AWS Batch) deployment**. They will return empty results on the public `sms-api-rke` (UCONN CCAM/SLURM) deployment.
- Tags are defined in code (`viva_api/simulation/simulation_tags.py`). To add new tags, edit the `SIMULATION_TAGS` dict and redeploy — no database migration required.
- The filtering logic runs in the database layer (`DatabaseService.list_simulations_filtered`) for efficiency.

## Error Responses

| Status | Condition |
|--------|-----------|
| `400` | Unknown tag provided (response includes list of available tags) |
| `500` | Database service unavailable or query error |

Example 400 response for unknown tag:

```json
{
  "detail": "Unknown simulation tag 'unknown-tag'. Available tags: ['cd1']"
}
```
