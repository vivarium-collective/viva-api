"""Where a run's outputs live: the two primitives every layout is built from.

One bucket, one prefix, and under it one directory per run, ``{prefix}/{experiment_id}/``. An
application's layout module (SMS: ``data_layout``, with its per-seed stores, caches and a
double-nested Nextflow download prefix) is built ON these, so the writer, the reader and the
downloader derive one location from one place and cannot drift.

Pure: the bucket and prefix are HANDED IN. Each caller reads them through its own settings seam
(``s3_work_bucket``, ``s3_output_prefix``), and a test that patches that seam is what this sees.
"""


def s3_uri(bucket: str, key: str) -> str:
    """``s3://<bucket>/<key>`` for a bucket-relative key."""
    return f"s3://{bucket}/{key}"


def experiment_prefix(prefix: str, experiment_id: str) -> str:
    """Bucket-relative key prefix of ONE run's outputs."""
    return f"{prefix}/{experiment_id}"


def results_uri(bucket: str, prefix: str, experiment_id: str) -> str:
    """Where a run syncs its outputs (trailing slash: a sync directory)."""
    return s3_uri(bucket, f"{experiment_prefix(prefix, experiment_id)}/")
