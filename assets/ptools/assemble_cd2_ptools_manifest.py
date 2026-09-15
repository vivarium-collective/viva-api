#!/usr/bin/env python3
"""Assemble the CD2 ptools manifest: stores + fill-job provenance + simulator map."""
import json, re, subprocess, sys, datetime, collections
import boto3
from botocore.config import Config

REG = "us-gov-west-1"
b = boto3.client("batch", config=Config(region_name=REG, retries={"max_attempts":5,"mode":"adaptive"}))
QUEUES = ["smsvpctest-ray-standalone", "smsvpctest-vecoli-task-amd64"]

def batch_jobs():
    out = []
    for q in QUEUES:
        for st in ("RUNNING","SUCCEEDED","FAILED","RUNNABLE","STARTING","SUBMITTED"):
            tok = None
            while True:
                kw = dict(jobQueue=q, jobStatus=st, maxResults=100)
                if tok: kw["nextToken"] = tok
                try: r = b.list_jobs(**kw)
                except Exception: break
                out += [(q, j) for j in r.get("jobSummaryList", [])]
                tok = r.get("nextToken")
                if not tok: break
    return out

def describe(ids):
    d = {}
    for i in range(0, len(ids), 100):
        for j in b.describe_jobs(jobs=ids[i:i+100])["jobs"]:
            d[j["jobId"]] = j
    return d

def main(stores_path, out_json, out_tsv):
    stores = json.load(open(stores_path))
    # ---- fill-job provenance (Batch retention ~7 days; this snapshot outlives it) ----
    js = [(q, j) for q, j in batch_jobs() if j["jobName"].startswith("cd2fill")]
    det = describe([j["jobId"] for _, j in js])
    jobs = []
    for q, j in js:
        D = det.get(j["jobId"], {})
        c = D.get("container", {}) or {}
        env = {e["name"]: e["value"] for e in c.get("environment", []) or []}
        cmd = env.get("CONTAINER_JOB_CMD", "")
        stream = c.get("logStreamName", "") or ""
        m = re.search(r"ray-container-([0-9a-f]+)", stream)
        dests = sorted(set(re.findall(r"s3://[^\s\"']+/analyses/[A-Za-z0-9._-]+/", cmd)))
        views = re.findall(r'"([a-z0-9_,]*ptools[a-z0-9_,]*|[a-z0-9_,]*cd1_[a-z0-9_,]*)"', cmd)
        jobs.append({
            "job_name": j["jobName"], "job_id": j["jobId"], "queue": q,
            "status": j.get("status"), "status_reason": D.get("statusReason"),
            "started_at": j.get("startedAt"), "stopped_at": j.get("stoppedAt"),
            "image_commit": m.group(1) if m else None,
            "log_stream": stream, "dest_prefixes": dests,
            "views": sorted({v for grp in views for v in grp.split(",") if v}),
            "container_job_cmd": cmd,
        })
    # ---- simulator id <-> commit (viva-api registry) ----
    sims = []
    try:
        raw = subprocess.run(["curl","-s","-m","30","http://localhost:8080/core/v1/simulator/versions"],
                             capture_output=True, text=True).stdout
        sims = json.loads(raw)["versions"]
    except Exception as e:
        print("WARN: simulator registry unreachable:", e, file=sys.stderr)
    by_commit = {}
    for s in sims:
        by_commit.setdefault(s["git_commit_hash"], []).append(s["database_id"])
    for j in jobs:
        ic = j["image_commit"]
        if ic:
            hits = [sid for c, ids in by_commit.items() if c.startswith(ic[:7]) for sid in ids]
            j["simulator_ids"] = sorted(hits)
    # attach jobs to stores by dest prefix
    idx = collections.defaultdict(list)
    for j in jobs:
        for dp in j["dest_prefixes"]:
            mm = re.search(r"vecoli-output/([^/]+)/analyses/", dp)
            if mm: idx[mm.group(1)].append(j["job_name"])
    for r in stores:
        r["fill_jobs"] = sorted(set(idx.get(r["store"], [])))

    man = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "what_this_is": ("Authoritative join for the CD2 ptools fill campaign: run family -> unit -> S3 store -> "
                         "v2ecoli commit / parca cache (from <store>/seed_*/run_identity.json) -> fill bundles "
                         "actually present in S3 -> the Batch job that produced them -> viva-api simulator id. "
                         "Built because no single source held all of this; Batch job records expire ~7 days."),
        "sources": {
            "family_to_store_map": "s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/tools/fanout_multi.sh (header) + fanout_run1.sh + fanout_percell_fill.sh",
            "per_store_provenance": "<store>/seed_*/run_identity.json  (NOT at the store root)",
            "simulator_registry": "viva-api GET /core/v1/simulator/versions",
            "fired_definition": "AWS Batch CONTAINER_JOB_CMD env var (retention ~7 days)",
        },
        "caveats": [
            "run_identity.json exists ONLY for Run-3 stores; Run-2/Run-4 need an explicit SIM_DATA (see sim_data_policy).",
            "run_identity.json's simulator field is 'v2ecoli/0.1.0' (a package), NOT the viva-api simulator id; the simulator id is recoverable only via the fill job's image commit.",
            "Run-4 fills have two modes: 'full-percell' writes a fresh analysis-percellfill/ dir; 'append-metabolites' writes INTO the sim-time analysis-mnp*/ptools/ dir. A scan that looks only for new bundle names understates coverage.",
            "ptools_overview emits no standalone .tsv, so it appears in view_types only where a TSV exists.",
            "Duplicate store prefixes exist per unit (retry1 / re-fires). All are listed; prefer the one with fill bundles.",
        ],
        "simulator_map": [{"simulator_id": s["database_id"], "commit": s["git_commit_hash"],
                           "repo": s["git_repo_url"], "branch": s["git_branch"], "created_at": s["created_at"]}
                          for s in sorted(sims, key=lambda x: x["database_id"]) if s["database_id"] >= 180],
        "fill_jobs": jobs,
        "stores": stores,
    }
    json.dump(man, open(out_json, "w"), indent=2, default=str)

    cols = ["family","unit","store","s3_uri","v2ecoli_commit","parca_cache","sim_data_policy",
            "fill_bundle","bundle_kind","ptools_tsv","viz","view_types","bundle_s3_uri","fill_jobs"]
    with open(out_tsv, "w") as fh:
        fh.write("\t".join(cols) + "\n")
        for r in stores:
            bs = r.get("fill_bundles") or {None: {}}
            for bn, bv in bs.items():
                fh.write("\t".join(str(x) for x in [
                    r.get("family"), r.get("unit"), r.get("store"), r.get("s3_uri"),
                    (r.get("v2ecoli_commit") or "")[:9], r.get("parca_cache") or "",
                    r.get("sim_data_policy",""), bn or "", bv.get("kind",""),
                    bv.get("ptools_tsv",0), bv.get("viz",0), ",".join(bv.get("view_types",[])),
                    bv.get("s3_uri",""), ",".join(r.get("fill_jobs") or []),
                ]) + "\n")
    print(f"stores={len(stores)}  fill_jobs={len(jobs)}  simulators={len(man['simulator_map'])}", file=sys.stderr)

main(*sys.argv[1:4])
