"""Backend-agnostic S3 -> tar.gz streaming helpers.

Lifted out of ``viva_api/common/handlers/simulations.py`` so both the study
route (``sms.py`` -> ``get_simulation_outputs``) and the compose route
(``compose.py`` -> ``get_results``) can stream an S3 prefix as a ``.tar.gz``
without ``compose.py`` (which stays deliberately thin/backend-agnostic) taking
on ``simulations.py``'s much heavier sms-specific import surface (simulation
services, ORM tables, an eager ``get_settings()`` call at import time, etc.).

Behavior is byte-for-byte identical to what used to live in
``simulations.py`` -- that module now imports these same functions from here,
so the study path's ``get_simulation_outputs``/``_stream_s3_tar_gz`` is
unaffected.
"""

import logging
from collections.abc import AsyncIterator

from viva_api.common.storage import data_layout
from viva_api.dependencies import get_file_service
from viva_core.storage.file_service import FileService
from viva_core.storage.s3_streaming import fetch_s3_file_entries as _fetch_s3_file_entries
from viva_core.storage.s3_streaming import gzip_pipe_stream as _gzip_pipe_stream
from viva_core.storage.s3_streaming import stream_prefix_tar_gz
from viva_core.storage.s3_streaming import write_tar_entries as _write_tar_entries

logger = logging.getLogger(__name__)

# The mechanism is core's (``viva_core.storage.s3_streaming``, P3d-4d-2); what stays here is the
# application's layout and its file-service lookup, under the names this module has always had.
write_tar_entries = _write_tar_entries
gzip_pipe_stream = _gzip_pipe_stream


def _require_file_service() -> FileService:
    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")
    return file_service


async def fetch_s3_file_entries(
    experiment_id: str, download_keys: list[str], experiment_prefix: str
) -> list[tuple[str, bytes]]:
    """Fetch S3 objects in-memory as (arcname, content) pairs for tar creation."""
    return await _fetch_s3_file_entries(_require_file_service(), experiment_id, download_keys, experiment_prefix)


async def stream_s3_tar_gz_ray(experiment_id: str, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
    """Stream a Ray ensemble's S3 outputs (zarr stores + summaries) into a tar.gz.

    The Ray entrypoint syncs the whole ``.pbg/runs/phase0-xarray`` tree to
    ``s3://{bucket}/{s3_output_prefix}/{experiment_id}/`` -- for v2ecoli comparison
    runs that is ``v2ecoli_seed{NN}.zarr/`` per seed plus ``v2ecoli_build_config.json``.
    Every object under ``RayLayout.experiment_prefix(experiment_id)`` is streamed as-is; a
    compose run's chained analysis manifest lives under the same prefix and rides along.
    """
    async for chunk in stream_prefix_tar_gz(
        _require_file_service(), data_layout.RayLayout.experiment_prefix(experiment_id), experiment_id, chunk_size
    ):
        yield chunk
