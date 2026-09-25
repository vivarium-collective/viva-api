"""The one place a file service is chosen from settings (``docs/plan-core.md`` §4b, U2c).

Three object stores, one ``FileService``: which one a site runs is ``storage_backend`` on
``CoreSettings`` (``s3`` for Stanford, ``qumulo`` for UConn, ``gcs`` historically). The switch used
to live in the application's composition root only, so a standalone core had no way to build the
service its datasets and results routes stream from. Each implementation reads its own
``storage_*`` settings through ``get_core_settings()``; this only picks the class.
"""

from __future__ import annotations

from typing import assert_never

from viva_core.settings import StorageBackend, get_core_settings
from viva_core.storage.file_service import FileService
from viva_core.storage.file_service_gcs import FileServiceGCS
from viva_core.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_core.storage.file_service_s3 import FileServiceS3


def file_service_for(backend: StorageBackend) -> FileService:
    """The file service for a named backend. Exhaustive over ``StorageBackend``: a new store is a
    new branch here, and the type checker says so."""
    match backend:
        case "s3":
            return FileServiceS3()
        case "qumulo":
            return FileServiceQumuloS3()
        case "gcs":
            return FileServiceGCS()
        case _:
            assert_never(backend)


def file_service_from_settings() -> FileService:
    """The site's file service, per ``storage_backend`` in core's settings."""
    return file_service_for(get_core_settings().storage_backend)
