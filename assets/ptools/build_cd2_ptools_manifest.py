#!/usr/bin/env python3
"""CD2 ptools run manifest — authoritative join of run -> store -> provenance -> fill bundles."""
import json, re, sys, concurrent.futures as cf
import boto3
from botocore.config import Config

B = "smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91"
ROOT = "vecoli-output/"
s3 = boto3.client("s3", config=Config(region_name="us-gov-west-1", max_pool_connections=32,
                                      retries={"max_attempts": 5, "mode": "adaptive"}))

# family -> (regex over store name, unit count, sim_data policy)  [from fanout_multi.sh + fanout_run1.sh]
FAMILIES = [
    ("run1",            r"sim199-sim199-cd2-run1-k4-coupled-kla350-seed(\d+)-[a-z0-9]+-[a-z0-9]+/$", 10, "per-seed cache (staged)"),
    ("run2",            r"(sim201)-cd2-run2-j3-refire3-parquet-96b27f38-10x10-[a-z0-9]+/$",           1, "explicit: cand2_j3_lam075_simData.cPickle"),
    ("run3",            r"sim200-cd2-run3-sweep-combo(\d{2})(?:-retry1)?-[a-z0-9]+/$",               36, "AUTO (run_identity.json)"),
    ("run4min-pathway", r"sim201-cd2-run4-genotype(\d+)-minimal-4x8-post301-96b27f38-sim201-[a-z0-9]+/$", 42, "explicit: carina_run4_genotype*_simData.cPickle"),
    ("run4min-native",  r"sim201-cd2-run4-native-design(\d+)-minimal-nativeoe-4x8-[a-z0-9]+/$",       16, "explicit: same Run-4-minimal base"),
    ("run4trp-pathway", r"sim204-cd2-run4-genotype(\d+)-withtrp-trpfix-4x8-[a-z0-9]+/$",              42, "explicit: same base (+Trp is runtime media)"),
    ("run4trp-native",  r"sim204-cd2-run4-native-design(\d+)-withtrp-trpfix-4x8-[a-z0-9]+/$",         16, "explicit: same base"),
]
# a fill bundle is either a dedicated dir, or ptools files appended into an analysis-mnp* dir
DEDICATED = ("analysis-percell-run3", "analysis-percellfill", "analysis-ptools-multiseed",
             "analysis-multiseed-run2j3", "analysis-multigeneration-run2j3",
             "analysis-ptools-multigeneration", "run1-seed")

def _pages(**kw):
    tok = None
    while True:
        if tok: kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(Bucket=B, **kw)
        yield r
        if not r.get("IsTruncated"): return
        tok = r["NextContinuationToken"]

def prefixes(p):
    return [x["Prefix"] for r in _pages(Prefix=p, Delimiter="/") for x in r.get("CommonPrefixes", [])]

def objects(p):
    return [(o["Key"], o["Size"], o["LastModified"]) for r in _pages(Prefix=p) for o in r.get("Contents", [])]

def get_json(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception: return None

def scan(store):
    name = store[len(ROOT):].rstrip("/")
    rec = {"store": name, "s3_uri": f"s3://{B}/{store}"}
    ident = None
    for sd in prefixes(store):
        if "/seed_" in sd or sd.rstrip("/").split("/")[-1].startswith("seed"):
            ident = get_json(sd + "run_identity.json")
            if ident: rec["run_identity_key"] = sd + "run_identity.json"; break
    if ident is None:
        ident = get_json(store + "run_identity.json")
        if ident: rec["run_identity_key"] = store + "run_identity.json"
    if ident:
        rec["v2ecoli_commit"] = (ident.get("code") or {}).get("commit")
        rec["sim_data_uri"] = (ident.get("sim_data") or {}).get("uri")
        rec["parca_cache"] = (rec["sim_data_uri"] or "").split("ray-parca-cache/")[-1].split("/")[0] or None
        rec["inputs_hash"] = (ident.get("cache_version") or {}).get("inputs_hash")
        rec["experiment_id"] = (ident.get("design") or {}).get("experiment_id")
    bundles, success = {}, 0
    for a in prefixes(store + "analyses/"):
        adir = a.rstrip("/").split("/")[-1]
        objs = objects(a)
        ptsv = [k for k, _, _ in objs if k.endswith(".tsv") and "/ptools/" in k]
        viz  = [k for k, _, _ in objs if "/viz/" in k]
        metab = sum(1 for k in ptsv if "ptools_metabolites" in k)
        dedicated = any(adir.startswith(f) for f in DEDICATED)
        if not dedicated and not (adir.startswith("analysis-mnp") and metab):
            success += sum(1 for k, _, _ in objs if k.endswith("_SUCCESS") or "success" in k.lower())
            continue
        views = sorted({k.split("/ptools/")[-1].split("__")[0] for k in ptsv})
        bundles[adir] = {
            "kind": "dedicated-fill" if dedicated else "append-metabolites(into sim-time analysis)",
            "s3_uri": f"s3://{B}/{a}", "ptools_tsv": len(ptsv), "viz": len(viz),
            "view_types": views, "objects": len(objs), "bytes": sum(s for _, s, _ in objs),
            "last_modified": max((m for _, _, m in objs), default=None),
        }
    rec["fill_bundles"] = bundles
    rec["n_objects_hint"] = success
    return rec

def main():
    stores = prefixes(ROOT)
    tagged = []
    for st in stores:
        nm = st[len(ROOT):]
        for fam, rx, cnt, pol in FAMILIES:
            m = re.match(rx, nm)
            if m: tagged.append((fam, m.group(1), st, pol)); break
    print(f"matched {len(tagged)} stores of {len(stores)} prefixes", file=sys.stderr)
    out = []
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        futs = {ex.submit(scan, st): (f, u, st, p) for f, u, st, p in tagged}
        for i, fu in enumerate(cf.as_completed(futs), 1):
            fam, unit, st, pol = futs[fu]
            try: rec = fu.result()
            except Exception as e: rec = {"store": st, "error": repr(e)}
            rec.update(family=fam, unit=unit, sim_data_policy=pol)
            out.append(rec)
            if i % 40 == 0: print(f"  {i}/{len(tagged)}", file=sys.stderr)
    out.sort(key=lambda r: (r["family"], int(r["unit"]) if str(r["unit"]).isdigit() else 0, r["store"]))
    json.dump(out, open(sys.argv[1], "w"), indent=2, default=str)
    print(f"wrote {sys.argv[1]} ({len(out)})", file=sys.stderr)

main()
