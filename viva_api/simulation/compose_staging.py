"""What SMS stages into a compose run: its simulator's ParCa cache (``docs/plan-core.md`` P3d-1).

This was ``ComposeSimulationServiceRay._parca_staging``. It names a ParCa cache and reads an SMS
setting, so it is SMS's; compose declares the hook it fills (``StageInputs``) and the composition root
hands this in. The body is the method's, verbatim.
"""

from viva_api.common.storage import data_layout
from viva_api.config import get_settings


def compose_parca_staging(commit: str | None = None) -> tuple[str | None, str | None]:
    """(stage_s3, stage_dir) for the commit-keyed ParCa cache, or (None, None).

    The ensemble path stages this cache by passing these same two args to ``submit_mnp`` -- the
    entrypoint turns them into RAY_STAGE_S3/RAY_STAGE_DIR and syncs S3 -> local on every node before
    the job command runs. The compose driver-swap replaced the command but must keep the staging, or
    a composite whose ``cache_dir`` expects a populated ParCa bundle (v2ecoli's ``baseline``) starts
    against an empty directory.

    Keyed by ``commit`` when a per-run build was resolved (item 98: a resolved ``simulator_id``
    implies a real, distinct commit -- staging the deploy-wide tag's cache instead would silently
    serve the wrong commit's data). Otherwise keyed by the deploy-wide image tag, since that IS the
    workspace commit in the static-image case. Disabled either way when no cache dir is configured.
    """
    settings = get_settings()
    if not settings.compose_parca_cache_dir:
        return None, None
    return (
        data_layout.RayLayout.parca_cache_uri(commit or settings.compose_ray_image_tag),
        settings.compose_parca_cache_dir,
    )
