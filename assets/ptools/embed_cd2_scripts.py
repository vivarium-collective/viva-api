#!/usr/bin/env python3
"""Make the harvest a SELF-CONTAINED backfill: embed the exact bytes of every
(tool, versionId) pair any harvested job staged, keyed by sha256.

This is one-time backfill input for viva-api#655's `tool_script` / `tool_run`
tables -- not a recurring lookback. After this the record no longer depends on
the S3 bucket, its versioning, or Batch/CloudWatch retention.
"""
import json, hashlib, sys
import boto3
from botocore.config import Config

B = "smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91"
s3 = boto3.client("s3", config=Config(region_name="us-gov-west-1",
                                      retries={"max_attempts": 6, "mode": "adaptive"}))

def main(path):
    doc = json.load(open(path))
    pairs = {}
    for j in doc["jobs"]:
        for key, v in (j.get("staged_tools") or {}).items():
            if v: pairs[(key, v["version_id"])] = v
    print(f"distinct (tool, versionId) pairs: {len(pairs)}", file=sys.stderr)

    scripts, misses = {}, []
    for (key, vid), v in sorted(pairs.items()):
        try:
            body = s3.get_object(Bucket=B, Key=key, VersionId=vid)["Body"].read()
        except Exception as e:
            misses.append({"key": key, "version_id": vid, "error": repr(e)}); continue
        sha = hashlib.sha256(body).hexdigest()
        try: text = body.decode("utf-8")
        except UnicodeDecodeError: text = None
        scripts[sha] = {
            "sha256": sha, "name": key.split("/")[-1], "s3_key": key,
            "version_id": vid, "version_last_modified": v["version_last_modified"],
            "size_bytes": len(body),
            "content": text,                      # the exact executed bytes
            "content_is_binary": text is None,
        }
        # stamp the hash back onto every job that staged this pair
        for j in doc["jobs"]:
            sv = (j.get("staged_tools") or {}).get(key)
            if sv and sv["version_id"] == vid: sv["sha256"] = sha

    doc["scripts"] = scripts
    doc["backfill_note"] = (
        "SELF-CONTAINED. `scripts` holds the exact bytes of every tool version any "
        "harvested job staged, keyed by sha256; each job's staged_tools entries carry "
        "the matching sha256. This is one-time backfill input for viva-api#655's "
        "tool_script/tool_run tables -- it is NOT a substitute for that mechanism, and "
        "is not intended to be re-run as a rolling lookback."
    )
    if misses: doc["fetch_failures"] = misses
    json.dump(doc, open(path, "w"), indent=2, default=str)
    total = sum(s["size_bytes"] for s in scripts.values())
    print(f"embedded scripts: {len(scripts)}  bytes={total}  failures={len(misses)}", file=sys.stderr)

main(sys.argv[1])
