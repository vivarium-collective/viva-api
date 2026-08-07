# ruff: noqa: E402  (os.environ must be set before any viva_api imports)
import os
import socket

# Set mandatory config before any viva_api imports (module-level code in
# simulator_defaults.py reads these at import time).
os.environ.setdefault("COMPUTE_BACKEND", "slurm")
os.environ.setdefault("PUBLIC_MODE", "false")


def _hpc_reachable() -> bool:
    """Return True only if the HPC SSH host is reachable AND an SSH key is configured.

    Requiring the key file — not just host reachability — keeps the SLURM
    integration tests from running with a blank/unset slurm_submit_key_path.
    The UConn login node is publicly reachable on :22, so a host-only check
    lets those tests run and then crash at key load (an empty key path becomes
    Path("")==".", so asyncssh does open(".") -> IsADirectoryError). The
    per-test skipif has the same blind spot: Path("").exists() is True. Gating
    on os.path.isfile(key_path) (False for "") makes them skip cleanly instead.
    """
    try:
        from viva_api.config import get_settings

        settings = get_settings()
        host = settings.slurm_submit_host
        key_path = settings.slurm_submit_key_path
        if not host or not os.path.isfile(os.path.expanduser(key_path)):
            return False
        with socket.create_connection((host, 22), timeout=3):
            return True
    except OSError:
        return False


_HPC_REACHABLE: bool = _hpc_reachable()


def pytest_collection_modifyitems(items):  # type: ignore[no-untyped-def]
    """Skip HPC-requiring tests when the SLURM host is not reachable (no VPN)."""
    if _HPC_REACHABLE:
        return
    import pytest

    skip = pytest.mark.skip(reason="HPC not reachable — VPN required (set VPN on to run these tests)")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip)
            continue
        for marker in item.iter_markers("skipif"):
            reason = marker.kwargs.get("reason", "")
            if "slurm" in reason.lower() or "ssh" in reason.lower() or "hpc" in reason.lower():
                item.add_marker(skip)
                break


import pytest_asyncio  # noqa: F401

from tests.fixtures.api_fixtures import (  # noqa: F401
    SimulatorRepoInfo,
    analysis_config_path,
    # biocyc_service,
    analysis_request,
    analysis_request_base,
    analysis_request_config,
    analysis_request_ptools,
    base_router,
    ecoli_simulation,
    empty_simulation_id,
    expected_analysis_output_files,
    expected_analysis_output_files_incorrect,
    experiment_request,
    fastapi_app,
    in_memory_api_client,
    job_scheduler,
    large_simulation_mock,
    latest_commit_hash,
    local_base_url,
    parca_options,
    ptools_analysis_request,
    simulation_config,
    simulation_mock,
    simulator_repo_info,
    workflow_config,
    workflow_request_payload,
    workspace_image_hash,
)
from tests.fixtures.data_fixtures import analysis_service, data_fixture, simulation_data  # noqa: F401
from tests.fixtures.file_service_fixtures import (  # noqa: F401
    file_service_gcs,
    file_service_gcs_test_base_path,
    file_service_local,
    file_service_qumulo,
    file_service_qumulo_test_base_path,
    file_service_s3,
    file_service_s3_test_base_path,
    gcs_token,
    temp_test_data_dir,
)
from tests.fixtures.handlers_fixtures import analysis_outdir  # noqa: F401
from tests.fixtures.k8s_fixtures import (  # noqa: F401
    make_listing_item,
    mock_file_service,
    mock_k8s_job_service,
    simulation_service_k8s_mock,
)
from tests.fixtures.logging_fixtures import logger  # noqa: F401
from tests.fixtures.mongodb_fixtures import (  # noqa: F401
    mongo_test_client,
    mongo_test_collection,
    mongo_test_database,
    mongodb_container,
)
from tests.fixtures.postgres_fixtures import async_postgres_engine, database_service, postgres_url  # noqa: F401
from tests.fixtures.redis_fixtures import (  # noqa: F401
    redis_container_host_and_port,
    redis_producer_service,
    redis_subscriber_service,
)
from tests.fixtures.simulation_fixtures import (  # noqa: F401
    expected_build_job_id,
    expected_parca_database_id,
    mock_ssh_session_service,
    simulation_service_mock_clone_and_build,
    simulation_service_mock_parca,
    simulation_service_slurm,
)
from tests.fixtures.slurm_fixtures import (  # noqa: F401
    nextflow_config_local_executor,
    nextflow_config_slurm_executor,
    nextflow_script_hello,
    nextflow_script_hello_slurm,
    slurm_service,
    slurm_template_hello_1s,
    slurm_template_hello_10s,
    slurm_template_hello_TEMPLATE,
    slurm_template_nextflow,
    slurm_template_nextflow_slurm_executor,
    slurm_template_with_storage,
    ssh_session_service,
)
from tests.fixtures.workflow_fixtures import (  # noqa: F401
    slurm_template_workflow,
    workflow_inputs_dir,
    workflow_test_config_content,
)
