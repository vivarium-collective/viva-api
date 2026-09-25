"""The file-service factory picks the implementation ``storage_backend`` names (U2c)."""

from __future__ import annotations

from typing import cast

import pytest

from viva_core.settings import StorageBackend, get_core_settings
from viva_core.storage.factory import file_service_for, file_service_from_settings
from viva_core.storage.file_service_gcs import FileServiceGCS
from viva_core.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_core.storage.file_service_s3 import FileServiceS3


@pytest.mark.parametrize(
    ("backend", "expected"),
    [("s3", FileServiceS3), ("qumulo", FileServiceQumuloS3), ("gcs", FileServiceGCS)],
)
def test_each_named_backend_builds_its_service(backend: StorageBackend, expected: type[object]) -> None:
    assert type(file_service_for(backend)) is expected


def test_the_site_service_follows_the_setting() -> None:
    settings = get_core_settings()
    previous = settings.storage_backend
    try:
        settings.storage_backend = "qumulo"
        assert isinstance(file_service_from_settings(), FileServiceQumuloS3)
        settings.storage_backend = "s3"
        assert isinstance(file_service_from_settings(), FileServiceS3)
    finally:
        settings.storage_backend = previous


def test_a_backend_the_type_does_not_name_is_refused() -> None:
    """The match is exhaustive for the checker; at runtime a stray value must not fall through to
    some default store."""
    with pytest.raises(AssertionError):
        file_service_for(cast(StorageBackend, "nfs"))
