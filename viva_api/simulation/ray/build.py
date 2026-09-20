"""Building a simulator image: a Docker-out-of-Docker build on AWS Batch, run as a LOCAL
task that submits the Batch job and polls it to completion.

``RayImageBuilder`` is a SERVICE, not a mixin of ``SimulationServiceRay`` (it was one for a
single PR, P2.1 cut 4; ``docs/plan-core.md`` decision log, 2026-09-20). A build needs one
collaborator -- the ``LocalTaskService`` that owns the task -- and nothing else of the
service: not the Batch layer, not ParCa, not dispatch. Inheriting all of that to reach one
attribute was the wrong shape, and it was a dead end besides: a build's destination is
core (``viva_core/backends/build.py`` + a recipe SMS registers; plan P2.3 / P5), and a mixin
that inherits an SMS class cannot go there. A service that is handed its dependencies can.

What keeps it in SMS today is what it still reaches for: this application's settings
(through ``_seams``), and ``viva_api.simulation.batch_build``, which names this
application's jobs and is shared with ``SimulationServiceK8s``.

``SimulationServiceRay.submit_build_image_job`` remains -- it is part of the
``SimulationService`` interface the handlers call -- and delegates here.
"""

import logging

from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobId
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.simulation import batch_build
from viva_api.simulation.models import SimulatorVersion
from viva_api.simulation.ray import _seams

logger = logging.getLogger(__name__)


class RayImageBuilder:
    def __init__(self, local_task_service: LocalTaskService) -> None:
        self._local = local_task_service

    async def submit(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> JobId:
        """Build the self-contained v2ecoli Ray image via a DooD Batch job.

        Symmetric with SimulationServiceK8s.submit_build_image_job: a LOCAL task submits a
        DooD Batch build job that clones the workload repo at the commit and runs its own
        build-and-push recipe (v2ecoli/docker/build-and-push-ecr.sh) → v2ecoli:<commit>
        (plus the :latest deploy tag the Ray-MNP job def references). Returns immediately
        with a LOCAL JobId; ``run`` polls the Batch job to completion.

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        build to before this param existed. See ``build_command``'s own docstring.

        ``stage_private_fork``/``vecoli_private_commit``: False for every existing caller --
        identical build to before these params existed. See ``build_command``'s own
        docstring.
        """
        commit = simulator_version.git_commit_hash
        return self._local.submit(
            self.run(
                simulator_version,
                include_new_gene_data=include_new_gene_data,
                include_submit_image=include_submit_image,
                stage_private_fork=stage_private_fork,
                vecoli_private_commit=vecoli_private_commit,
            ),
            name=f"ray-build-{commit}",
        )

    def build_command(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> list[str]:
        """DooD build command: clone v2ecoli@commit, run its build-and-push recipe.

        Mirrors SimulationServiceK8s._build_command (apk deps, PAT clone, in-repo recipe),
        but the workload repo is v2ecoli and the recipe is the v2ecoli image's own
        docker/build-and-push-ecr.sh → v2ecoli:<sha> (+ :latest).

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        command to before this param existed (the outer clone's PAT is unset immediately,
        as before; ``-g`` is never passed). When True, the SAME PAT this method already
        fetches to clone the workload repo (both under the CovertLabEcoli org) stays
        exported for the build-and-push recipe's own ``-g`` flag, which threads it through
        as a Docker BuildKit secret (never a plain env/build-arg baked into a layer) so the
        image can stage private new-gene data for a ``--composite vecoli`` ParCa build that
        declares one. No new credential -- reuses this same Secrets Manager entry.

        ``stage_private_fork``/``vecoli_private_commit``: stage vEcoli-private -- not the
        Dockerfile's public default (``CovertLab/vEcoli@master``) -- as the image's own
        wrapped ``/app/vEcoli`` fork, via the SAME ``-s <spec>`` mechanism
        docker/build-and-push-ecr.sh already supports for any fork comparison. Every
        existing build reaches this recipe with no ``-s`` at all, so it silently falls
        through to that public default regardless of the simulator's own pinned commit --
        a raw ``!ParameterSerializer[...]`` tag whose value only exists in the private
        fork's own param_store (e.g. antibiotic-transport parameters) can never resolve on
        any remote dispatch today. The spec is generated INLINE in this same script (a
        heredoc, not a checked-in file) so there is nothing to go stale. Reuses the SAME
        PAT already fetched for the outer clone (vEcoli-private is private, same org) via
        the recipe's own ``--secret id=github_pat`` path -- no new credential.
        ``vecoli_private_commit`` is REQUIRED when ``stage_private_fork`` is True:
        deliberately no "latest" auto-resolution, so which commit gets staged is always an
        explicit, visible choice made by the caller, never a silent moving target.
        """
        if stage_private_fork and not vecoli_private_commit:
            raise ValueError("vecoli_private_commit is required when stage_private_fork is True")
        settings = _seams.get_settings()
        commit = simulator_version.git_commit_hash
        branch = simulator_version.git_branch
        repo_url = simulator_version.git_repo_url
        build_flags = " -g" if include_new_gene_data else ""
        if stage_private_fork:
            build_flags += " -s /tmp/vecoli-private-fork.yaml"
        keep_pat = include_new_gene_data or stage_private_fork
        unset_pat = "" if keep_pat else "unset GH_PAT\n"
        private_fork_spec_block = ""
        if stage_private_fork:
            private_fork_spec_block = f"""\
cat > /tmp/vecoli-private-fork.yaml <<'SPEC'
comparison:
  reference:
    repo: {RepoUrl.VECOLI_PRIVATE_REPO_URL}
    commit: {vecoli_private_commit}
    extra_deps:
      - jax
SPEC
"""
        script = f"""\
set -ex
export USER=${{USER:-sms-api}}
apk add --no-cache aws-cli git bash

# Docker daemon runs on the host (DooD) — verify the mounted socket.
docker info >/dev/null 2>&1 || {{ echo "ERROR: Docker socket not available"; exit 1; }}

# GitHub PAT (Secrets Manager) for the clone; x-access-token is GitHub's HTTPS convention.
# Disable xtrace around the secret so the PAT (and the clone URL embedding it) never lands
# in the build logs (CloudWatch). Re-enable tracing once the clone is done.
set +x
export GH_PAT=$(aws secretsmanager get-secret-value \
    --secret-id {settings.build_git_secret_arn} --query SecretString --output text)
CLONE_URL=$(echo "{repo_url}" | sed "s|https://github.com/|https://x-access-token:${{GH_PAT}}@github.com/|")
export GIT_TERMINAL_PROMPT=0
git clone --branch {branch} --single-branch "$CLONE_URL" /build/v2ecoli
unset CLONE_URL
{unset_pat}set -x
cd /build/v2ecoli
git checkout {commit}

{private_fork_spec_block}# The v2ecoli image is self-contained (bundles the AWS CLI + Ray entrypoint); its own
# recipe builds + pushes v2ecoli:<sha> and the :latest deploy tag the MNP job def uses.
bash docker/build-and-push-ecr.sh -i {commit} -r {settings.ray_ecr_repository} -R {settings.batch_region}{build_flags}
"""
        if include_submit_image:
            # The Nextflow HEAD image. Deliberately a thin derived layer, not a change to the
            # task image: on vEcoli's proven awsbatch profile the Batch tasks run
            # ``container = params.container_image`` -- the PLAIN science image, with no JVM
            # and no nextflow binary anywhere. Only the process that runs ``nextflow run``
            # needs Java. v2ecoli's own image already installs AWS CLI v2 (Dockerfile:149-156),
            # which is the one thing Nextflow *does* require inside a task container to stage
            # the S3 work dir, so nothing about the task side has to change.
            #
            # Mirrors SimulationServiceK8s._build_command(submit_image=True) rather than
            # inventing a second recipe; NEXTFLOW_VERSION is pinned to the same 25.10.2 that
            # image uses, so one Nextflow version spans the deployment. (Phase 0 of
            # docs/plan-nextflow-dispatch.md measured 25.04.3; both map `time` to Batch
            # attemptDurationSeconds -- see its §11.1 -- and the skew is resolved here in
            # favour of what already ships.)
            script += f"""
BASE_URI=$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}

cat > /tmp/Dockerfile-submit <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${{BASE_IMAGE}}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \\
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ARG NEXTFLOW_VERSION=25.10.2
RUN curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${{NEXTFLOW_VERSION}}/nextflow" \\
    -o /usr/local/bin/nextflow && chmod +x /usr/local/bin/nextflow
WORKDIR /app/v2ecoli
DOCKERFILE

docker build -t "$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit" \
    --build-arg BASE_IMAGE="$BASE_URI" \
    -f /tmp/Dockerfile-submit /tmp
docker push "$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit"
echo "Submit image pushed: $ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit"
"""
        return ["sh", "-c", script]

    async def run(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> None:
        """Submit the DooD v2ecoli image build to Batch (amd64 queue) and poll it."""
        settings = _seams.get_settings()
        commit = simulator_version.git_commit_hash
        job_id = await batch_build.submit_batch_build(
            job_name=batch_build.ray_build_job_name(commit),
            queue=settings.build_amd64_queue,
            command=self.build_command(
                simulator_version,
                include_new_gene_data=include_new_gene_data,
                include_submit_image=include_submit_image,
                stage_private_fork=stage_private_fork,
                vecoli_private_commit=vecoli_private_commit,
            ),
        )
        # viva-api#414: persist the Batch handle on this task's HpcRun row so
        # the build's outcome is recoverable from any process, not only the
        # one holding this asyncio.Task (which dies with the pod).
        await self._local.record_external_job_ids([job_id])
        await batch_build.poll_batch_jobs([job_id])
        logger.info("v2ecoli Ray image build complete: %s:%s", settings.ray_ecr_repository, commit)
