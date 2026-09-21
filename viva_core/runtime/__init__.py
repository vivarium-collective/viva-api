"""What runs INSIDE a container core dispatches: the entrypoint that keeps the container contract.

Not imported by anything -- ``batch-container-entrypoint.sh`` is copied into the core runtime image
(``Dockerfile-core-runtime``) at ``/opt/batch-container-entrypoint.sh``, the path every container
job definition calls. Its other half is ``viva_core.backends.batch.stage_out_env``, which writes
the ``CONTAINER_*`` variables this script reads.
"""
