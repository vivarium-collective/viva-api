#!/usr/bin/env python3
"""Harvest every cd2fill Batch job still inside the 7-day window into a durable record.

Captures what expires (the invocation + staging timestamps) and RESOLVES each staged
tool to the exact S3 versionId that was current when the container downloaded it --
turning the lossy timestamp inference into a recorded fact before the window closes.
"""
import json, re, sys, datetime, bisect
import boto3
from botocore.config import Config

REG = "us-gov-west-1"
B = "smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91"
LG = "smsvpctest-ray-batch-RayBatchLogs2D18AAFC-vjj0f55ZeO5i"
cfg = Config(region_name=REG, retries={"max_attempts": 6, "mode": "adaptive"})
batch = boto3.client("batch", config=cfg)
logs = boto3.client("logs", config=cfg)
s3 = boto3.client("s3", config=cfg)

QUEUES = ["smsvpctest-ray-standalone", "smsvpctest-vecoli-task-amd64"]
STATES = ["SUBMITTED","PENDING","RUNNABLE","STARTING","RUNNING","SUCCEEDED","FAILED"]

def list_jobs():
    out = []
    for q in QUEUES:
        for st in STATES:
            tok = None
            while True:
                kw = dict(jobQueue=q, jobStatus=st, maxResults=100)
                if tok: kw["nextToken"] = tok
                try: r = batch.list_jobs(**kw)
                except Exception: break
                out += [j["jobId"] for j in r.get("jobSummaryList", []) if j["jobName"].startswith("cd2fill")]
                tok = r.get("nextToken")
                if not tok: break
    return out

# --- version index: for each tools/ key, versions sorted by LastModified ---
def version_index():
    idx, tok = {}, None
    while True:
        kw = dict(Bucket=B, Prefix="tools/")
        if tok: kw["KeyMarker"] = tok
        r = s3.list_object_versions(**kw)
        for v in r.get("Versions", []):
            idx.setdefault(v["Key"], []).append((v["LastModified"], v["VersionId"], v["Size"]))
        if not r.get("IsTruncated"): break
        tok = r.get("NextKeyMarker")
    for k in idx: idx[k].sort(key=lambda t: t[0])
    return idx

def resolve(idx, key, when):
    """newest version at or before `when` -- what the container actually got."""
    vs = idx.get(key)
    if not vs or when is None: return None
    times = [t for t, _, _ in vs]
    i = bisect.bisect_right(times, when) - 1
    if i < 0: return None
    t, vid, size = vs[i]
    return {"version_id": vid, "version_last_modified": t.isoformat(), "size": size,
            "staged_at": when.isoformat(),
            "exact": True if i == len(vs)-1 or times[i+1] > when else False}

def log_lines(stream):
    if not stream or stream == "None": return []
    out, tok = [], None
    while True:
        kw = dict(logGroupName=LG, logStreamName=stream, startFromHead=True, limit=10000)
        if tok: kw["nextToken"] = tok
        try: r = logs.get_log_events(**kw)
        except Exception: return out
        ev = r.get("events", [])
        out += [(e["timestamp"], e["message"]) for e in ev]
        nt = r.get("nextForwardToken")
        if not ev or nt == tok: return out
        tok = nt

def main(path):
    idx = version_index()
    ids = list_jobs()
    print(f"cd2fill jobs in the Batch window: {len(ids)}", file=sys.stderr)
    recs = []
    for i in range(0, len(ids), 100):
        for j in batch.describe_jobs(jobs=ids[i:i+100])["jobs"]:
            c = j.get("container", {}) or {}
            env = {e["name"]: e["value"] for e in c.get("environment", []) or []}
            cmd = env.get("CONTAINER_JOB_CMD", "")
            stream = c.get("logStreamName") or ""
            m = re.search(r"ray-container-([0-9a-f]+)", stream)
            staged = {}
            for ts, msg in log_lines(stream):
                mm = re.match(r"download: s3://[^/]+/(tools/[^\s]+) to ", msg)
                if mm:
                    when = datetime.datetime.fromtimestamp(ts/1000, datetime.timezone.utc)
                    staged[mm.group(1)] = resolve(idx, mm.group(1), when)
            recs.append({
                "job_name": j["jobName"], "job_id": j["jobId"], "status": j.get("status"),
                "status_reason": j.get("statusReason"), "exit_code": c.get("exitCode"),
                "created_at": j.get("createdAt"), "started_at": j.get("startedAt"),
                "stopped_at": j.get("stoppedAt"),
                "image": c.get("image"), "image_commit": m.group(1) if m else None,
                "log_stream": stream,
                "container_job_cmd": cmd,          # <- the thing that expires
                "staged_tools": staged,            # <- resolved to exact versionIds
            })
    recs.sort(key=lambda r: r.get("created_at") or 0)
    doc = {
        "harvested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "why": ("AWS Batch job records and the CloudWatch log group BOTH retain 7 days. "
                "The invocation (CONTAINER_JOB_CMD) and the staging timestamps exist nowhere else. "
                "S3 object versions are durable but unlinked to any run, so this harvest resolves "
                "each staged tool to the exact versionId current at download time, while that link "
                "is still derivable. See viva-api#655."),
        "jobs": recs,
    }
    json.dump(doc, open(path, "w"), indent=2, default=str)
    exact = sum(1 for r in recs for v in r["staged_tools"].values() if v and v["exact"])
    amb   = sum(1 for r in recs for v in r["staged_tools"].values() if v and not v["exact"])
    print(f"jobs={len(recs)}  tool-version links: exact={exact} ambiguous={amb}", file=sys.stderr)

main(sys.argv[1])
