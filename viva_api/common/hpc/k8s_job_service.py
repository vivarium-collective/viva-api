"""Kubernetes Job status service for polling workflow run status."""

import logging

from kubernetes import client as k8s_client

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus

logger = logging.getLogger(__name__)

# Map K8s Job condition types to JobStatus
_K8S_TERMINAL_CONDITIONS = {"Complete": JobStatus.COMPLETED, "Failed": JobStatus.FAILED}


def _job_to_status(job: k8s_client.V1Job) -> JobStatus:
    """Map a K8s Job object to a JobStatus enum."""
    if job.status is None:
        return JobStatus.UNKNOWN

    # Check terminal conditions first
    if job.status.conditions:
        for condition in job.status.conditions:
            if condition.type in _K8S_TERMINAL_CONDITIONS and condition.status == "True":
                return _K8S_TERMINAL_CONDITIONS[condition.type]

    # Infer from counts
    if job.status.active and job.status.active > 0:
        return JobStatus.RUNNING
    if job.status.ready and job.status.ready > 0:
        return JobStatus.RUNNING

    return JobStatus.PENDING


def _job_to_status_info(job: k8s_client.V1Job, job_id: JobId) -> JobStatusInfo:
    """Convert a K8s Job to a JobStatusInfo."""
    status = _job_to_status(job)

    start_time = None
    if job.status and job.status.start_time:
        start_time = job.status.start_time.isoformat()

    end_time = None
    if job.status and job.status.completion_time:
        end_time = job.status.completion_time.isoformat()

    error_message = None
    if status == JobStatus.FAILED and job.status and job.status.conditions:
        for condition in job.status.conditions:
            if condition.type == "Failed" and condition.message:
                error_message = condition.message
                break

    return JobStatusInfo(
        job_id=job_id,
        status=status,
        start_time=start_time,
        end_time=end_time,
        error_message=error_message,
    )


class K8sJobService:
    """Kubernetes Job operations: create, status, cancel, logs."""

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace
        # Load in-cluster config when running in a K8s pod,
        # or kubeconfig for local development
        try:
            k8s_client.Configuration.set_default(k8s_client.Configuration())
            from kubernetes import config as k8s_config

            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        self._batch_api = k8s_client.BatchV1Api()
        self._core_api = k8s_client.CoreV1Api()

    def create_job(self, job: k8s_client.V1Job) -> k8s_client.V1Job:
        """Create a Kubernetes Job."""
        return self._batch_api.create_namespaced_job(namespace=self._namespace, body=job)

    def get_job_status(self, job_name: str) -> JobStatusInfo | None:
        """Get status of a K8s Job by name."""
        try:
            job = self._batch_api.read_namespaced_job_status(name=job_name, namespace=self._namespace)
        except k8s_client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise
        return _job_to_status_info(job, JobId.k8s(job_name))

    def delete_job(self, job_name: str) -> None:
        """Delete a K8s Job with foreground propagation (kills pods)."""
        self._batch_api.delete_namespaced_job(
            name=job_name,
            namespace=self._namespace,
            body=k8s_client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        logger.info(f"Deleted K8s Job {job_name} in namespace {self._namespace}")

    def create_configmap(self, configmap: k8s_client.V1ConfigMap) -> k8s_client.V1ConfigMap:
        """Create a ConfigMap (for workflow config injection)."""
        return self._core_api.create_namespaced_config_map(namespace=self._namespace, body=configmap)

    def delete_configmap(self, name: str) -> None:
        """Delete a ConfigMap."""
        try:
            self._core_api.delete_namespaced_config_map(name=name, namespace=self._namespace)
        except k8s_client.rest.ApiException as e:
            if e.status != 404:
                raise

    def get_pod_termination(self, job_name: str) -> str | None:
        """Why this Job's pod stopped, as a short phrase — or ``None``.

        Kubernetes knows things the socket does not. When a worker dies mid-call
        the caller gets "worker closed the connection", which is true and
        useless: an OOM kill, a segfault and a deliberate delete are
        indistinguishable at the socket. The pod's terminated state names the
        cause, so read it and pass it on.

        Best-effort by construction — the pod may already be gone (Job TTL), the
        API may be unreachable, and neither is a reason to fail the caller's
        request. ``None`` means "no better answer available", never "it exited
        cleanly".
        """
        try:
            pods = self._core_api.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=f"job-name={job_name}",
            )
        except k8s_client.rest.ApiException:
            logger.warning(f"Failed to read pod termination for Job {job_name}")
            return None
        for pod in pods.items:
            for status in (pod.status.container_statuses or []) if pod.status else []:
                terminated = getattr(status.state, "terminated", None) if status.state else None
                if terminated is None:
                    continue
                reason = terminated.reason or "terminated"
                code = terminated.exit_code
                return f"{reason} (exit {code})" if code is not None else str(reason)
        return None

    def get_job_logs(self, job_name: str) -> str | None:
        """Get logs from the first pod of a Job."""
        try:
            pods = self._core_api.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=f"job-name={job_name}",
            )
            if not pods.items:
                return None
            pod_name = pods.items[0].metadata.name
            log: str = self._core_api.read_namespaced_pod_log(name=pod_name, namespace=self._namespace)
            return log
        except k8s_client.rest.ApiException:
            logger.warning(f"Failed to get logs for Job {job_name}")
            return None
