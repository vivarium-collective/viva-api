import logging
from pathlib import Path

from viva_api.common.hpc.models import SlurmJob
from viva_api.common.ssh.ssh_service import SSHSession
from viva_api.common.storage.file_paths import HPCFilePath

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SlurmService:
    verified_htc_dir: bool = False

    async def get_job_status_squeue(self, ssh: SSHSession, job_ids: list[int] | None = None) -> list[SlurmJob]:
        command = f'squeue -u $USER --noheader --format="{SlurmJob.get_squeue_format_string()}"'
        if job_ids is not None:
            job_ids_str = ",".join(map(str, job_ids)) if len(job_ids) > 1 else str(job_ids[0])
            command = command + f" -j {job_ids_str}"

        return_code, stdout, stderr = await ssh.run_command(command=command, check=False)

        if return_code != 0:
            # Invalid job id causes squeue to fail entirely, even if some jobs are valid
            # Fall back to querying each job individually to get partial results
            if "Invalid job id" in stderr and job_ids is not None:
                logger.debug("Batch squeue failed due to invalid job id(s), falling back to individual queries")
                return await self._get_job_status_squeue_individual(ssh, job_ids)
            raise Exception(
                f"failed to get job status with command {command} return code {return_code} stderr {stderr[:100]}"
            )
        slurm_jobs: list[SlurmJob] = []
        for line in stdout.splitlines():
            if not line.strip():
                continue
            slurm_jobs.append(SlurmJob.from_squeue_formatted_output(line.strip()))
        return slurm_jobs

    async def _get_job_status_squeue_individual(self, ssh: SSHSession, job_ids: list[int]) -> list[SlurmJob]:
        """Query squeue for each job individually to handle invalid job IDs gracefully.

        This is slower than batch querying but allows returning partial results
        when some job IDs are invalid (e.g., jobs that have completed and left squeue).
        """
        slurm_jobs: list[SlurmJob] = []
        for job_id in job_ids:
            command = f'squeue --noheader --format="{SlurmJob.get_squeue_format_string()}" -j {job_id}'
            return_code, stdout, stderr = await ssh.run_command(command=command, check=False)

            if return_code != 0:
                if "Invalid job id" in stderr:
                    logger.debug(f"Job {job_id} not found in squeue (may have completed)")
                    continue
                # Log warning but continue to next job
                logger.warning(f"squeue failed for job {job_id}: {stderr[:100]}")
                continue

            for line in stdout.splitlines():
                if not line.strip():
                    continue
                try:
                    slurm_jobs.append(SlurmJob.from_squeue_formatted_output(line.strip()))
                except Exception as e:
                    logger.warning(f"Failed to parse squeue output for job {job_id}: {e}")

        return slurm_jobs

    # this is deprecated for use in this project since GovCloud PCS doesn't support Slurm accounting
    async def _get_job_status_sacct(self, ssh: SSHSession, job_ids: list[int] | None = None) -> list[SlurmJob]:
        command = (
            f'sacct -u $USER --parsable --delimiter="|" --noheader --format="{SlurmJob.get_sacct_format_string()}"'
        )
        if job_ids is not None:
            job_ids_str = ",".join(map(str, job_ids)) if len(job_ids) > 1 else str(job_ids[0])
            command = command + f" -j {job_ids_str}"

        return_code, stdout, stderr = await ssh.run_command(command=command)

        if return_code != 0:
            raise Exception(
                f"failed to get job status with command {command} return code {return_code} stderr {stderr[:100]}"
            )
        slurm_jobs: list[SlurmJob] = []
        for line in stdout.splitlines():
            if not line.strip():
                continue
            # skip lines which are .batch and ?? jobs
            if line.split("|")[0].endswith(".batch"):
                continue
            if line.split("|")[0].endswith(".extern"):
                continue
            slurm_jobs.append(SlurmJob.from_sacct_formatted_output(line.strip()))
        return slurm_jobs

    async def get_job_status_scontrol(self, ssh: SSHSession, job_ids: list[int]) -> list[SlurmJob]:
        """Get job status using scontrol show job (alternative to sacct when accounting is disabled).

        Note: scontrol only shows jobs that are still in the scheduler's memory.
        Completed jobs may not be available after some time (typically minutes to hours
        depending on SLURM configuration). For historical job data, use sacct if available.

        Args:
            ssh: SSH session to use for the command
            job_ids: List of job IDs to query (required, unlike sacct)

        Returns:
            List of SlurmJob objects for jobs that were found
        """
        if not job_ids:
            return []

        slurm_jobs: list[SlurmJob] = []
        for job_id in job_ids:
            command = f"scontrol show job {job_id}"
            return_code, stdout, stderr = await ssh.run_command(command=command, check=False)

            if return_code != 0:
                # Job not found is not an error - it may have completed and left scheduler memory
                if "Invalid job id" in stderr or "not found" in stderr.lower():
                    logger.debug(f"Job {job_id} not found in scontrol (may have completed)")
                    continue
                raise Exception(
                    f"failed to get job status with command {command} return code {return_code} stderr {stderr[:100]}"
                )

            if stdout.strip():
                try:
                    slurm_job = SlurmJob.from_scontrol_output(stdout)
                    slurm_jobs.append(slurm_job)
                except Exception as e:
                    logger.warning(f"Failed to parse scontrol output for job {job_id}: {e}")

        return slurm_jobs

    async def submit_job(
        self,
        ssh: SSHSession,
        local_sbatch_file: Path,
        remote_sbatch_file: HPCFilePath,
        args: tuple[str, ...] | None = None,
    ) -> int:
        if not self.verified_htc_dir:
            await ssh.run_command("mkdir -p " + str(remote_sbatch_file.parent))
            self.verified_htc_dir = True
        await ssh.scp_upload(local_file=local_sbatch_file, remote_path=remote_sbatch_file)
        command = f"sbatch --parsable {remote_sbatch_file}"
        if args:
            for arg in args:
                command += f" {arg}"
        return_code, stdout, stderr = await ssh.run_command(command=command)

        if return_code != 0:
            raise Exception(
                f"failed to get job status with command {command} return code {return_code} stderr {stderr[:100]}"
            )
        return int(stdout)
