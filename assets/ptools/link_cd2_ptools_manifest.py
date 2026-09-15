#!/usr/bin/env python3
"""Link fill jobs -> stores/bundles. Commands use shell vars, so expand them,
and for fanout-driven jobs derive the dest from the script's naming convention."""
import json, re, sys

BUCKET = "smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91"

def expand(cmd):
    """Expand simple `VAR=value;` assignments appearing in the command."""
    env = dict(re.findall(r"(?:^|[;&]\s*)([A-Z][A-Z0-9_]*)=([^\s;]+)", cmd))
    s = cmd
    for _ in range(3):
        for k, v in env.items():
            s = s.replace("${%s}" % k, v).replace("$%s" % k, v)
    return s, env

def derive(job):
    """Return (bundle_name, selector) describing what this job writes."""
    cmd = job["container_job_cmd"]
    s, env = expand(cmd)
    dests = sorted(set(re.findall(r"s3://[A-Za-z0-9._-]+/vecoli-output/([^/]+)/analyses/([A-Za-z0-9._-]+)/", s)))
    if dests:
        return [{"store": st, "bundle": bn} for st, bn in dests], env
    # fanout-driven: derive from the script + its env
    if "fanout_multi.sh" in cmd:
        scales = (env.get("SCALES") or "multigeneration,multiseed").split(",")
        return [{"family": env.get("FAMILY"), "bundle": f"analysis-ptools-{sc}"} for sc in scales if sc], env
    if "fanout_run3_percell.sh" in cmd:
        lo, hi = env.get("LO", "0"), env.get("HI", "35")
        return [{"family": "run3", "bundle": "analysis-percell-run3",
                 "units": [f"{n:02d}" for n in range(int(lo), int(hi) + 1)]}], env
    if "fanout_percell_fill.sh" in cmd:
        # the script's own selector covers BOTH Run-4 minimal arms (pathway genotypes + native designs)
        return [{"family": f, "bundle": "analysis-percellfill|analysis-mnp*(append-metabolites)"}
                for f in ("run4min-pathway", "run4min-native")], env
    if "fanout_run1.sh" in cmd:
        return [{"family": "run1", "bundle": "run1-seed{N}-ptools*"}], env
    return [], env

def main(path):
    m = json.load(open(path))
    by_store = {}
    for j in m["fill_jobs"]:
        tgts, env = derive(j)
        j["writes"] = tgts
        j["env_knobs"] = {k: v for k, v in env.items()
                          if k in ("FAMILY","SCALES","LO","HI","MEM_LIMIT","SIM_DATA","VIEWS","P","OUT")}
        for t in tgts:
            if "store" in t:
                by_store.setdefault(t["store"], set()).add(j["job_name"])
    # family/unit-level linkage
    for r in m["stores"]:
        names = set(by_store.get(r["store"], set()))
        for j in m["fill_jobs"]:
            for t in j.get("writes", []):
                if t.get("family") != r["family"]:
                    continue
                if "units" in t and r["unit"] not in t["units"]:
                    continue
                names.add(j["job_name"])
        r["fill_jobs"] = sorted(names)
    json.dump(m, open(path, "w"), indent=2, default=str)
    linked = sum(1 for r in m["stores"] if r["fill_jobs"])
    direct = sum(1 for j in m["fill_jobs"] if any("store" in t for t in j.get("writes", [])))
    print(f"stores linked to >=1 job: {linked}/{len(m['stores'])}; jobs with explicit dest: {direct}/{len(m['fill_jobs'])}")

main(sys.argv[1])
