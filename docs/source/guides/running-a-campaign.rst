Running a Sensitivity Campaign on AWS
======================================

A **campaign** is a multi-parca run: several ParCa instances, each fed a
different perturbed RNA-seq dataset, then simulated, analyzed, and compared.
All of that machinery lives in vEcoli — viva-api just launches it.

This tutorial is the shortest path from zero to a running campaign using
``atlantis`` and custom data sources. For the design and full data model,
see ``doc/sensitivity_campaigns.rst`` in the vEcoli repo — it's the
authoritative reference for operators, manifest schemas, and the
sensitivity_overview output columns.

.. contents:: On this page
   :local:
   :depth: 2

Prerequisites
-------------

Three sibling repos in the same parent directory:

.. code-block:: text

   ~/code/
     viva-api/              (this repo)
     vEcoli/               (or CovertLab/vEcoli fork on the accepted list)
     ecoli-sources/        (primary RNA-seq datasets + perturbation operators)
     ecoli-sources-vegas/  (optional private overlay)

Plus, on your workstation:

- ``uv`` and ``aws`` CLIs on PATH
- ``STORAGE_S3_BUCKET`` configured in the server you'll target (see
  :doc:`aws-s3-setup`)
- Write access to the vEcoli branch you'll push (atlantis builds from git)

Step 1 — Authenticate to AWS
-----------------------------

.. code-block:: bash

   aws sso login --profile <your-sso-profile>
   export AWS_PROFILE=<your-sso-profile>

   # Confirm
   aws sts get-caller-identity

If your profile also needs region, set ``AWS_REGION=us-east-1`` (or whatever
matches the viva-api bucket). The ``--sources`` flag shells out to
``aws s3 sync``, so whatever ``aws s3 ls s3://$STORAGE_S3_BUCKET/`` accepts
will work here.

Step 1b — Open a tunnel (GovCloud / internal-ALB deployments only)
-------------------------------------------------------------------

If you're targeting a public viva-api deployment (e.g. ``sms.cam.uchc.edu``,
``sms-dev.cam.uchc.edu``), **skip this step** — atlantis hits the public URL
directly.

If you're targeting a GovCloud or test-VPC deployment (e.g. ``smscdk``,
``smsvpctest``), the API lives behind an **internal** Application Load
Balancer inside a private VPC — no public DNS. To reach it, open an SSM
port-forward from your laptop, through the Batch submit-node EC2, to the
ALB. The helper lives in this repo:

.. code-block:: bash

   # In a separate terminal, keep this running for the session
   AWS_PROFILE=<your-sso-profile> AWS_DEFAULT_REGION=us-gov-west-1 \
       ./scripts/sms-tunnel.sh -s smsvpctest

Once the tunnel is up, the API is reachable locally:

.. code-block:: bash

   export API_BASE_URL=http://localhost:8080

All subsequent ``atlantis ...`` commands in *this* shell will route through
the tunnel. The same tunnel also exposes the Pathway Tools web UI at
``http://localhost:8080/`` and the API docs at ``http://localhost:8080/docs``
— they share the internal ALB.

Prerequisites for the tunnel: AWS CLI v2 and the `Session Manager plugin
<https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html>`_.
Substitute a different ``-s`` value for other stacks (e.g. ``-s smscdk``
for the main GovCloud deployment). Use ``-p <port>`` if 8080 is taken
locally, and then set ``API_BASE_URL`` to match.

Step 2 — Set up ecoli-sources siblings
---------------------------------------

.. code-block:: bash

   cd ~/code
   git clone https://github.com/CovertLab/ecoli-sources.git
   git clone https://github.com/CovertLab/ecoli-sources-vegas.git  # if you have access

Each repo is a directory of TSVs plus a ``data/manifest.tsv`` that names
the datasets. The first repo you pass to ``--sources`` backs
``ECOLI_SOURCES`` (the primary data root); each additional one becomes an
overlay manifest appended to ``ECOLI_SOURCES_OVERLAYS``.

**Sanity check:**

.. code-block:: bash

   ls ecoli-sources/data/manifest.tsv
   ls ecoli-sources-vegas/data/manifest.tsv

If a directory has no ``data/manifest.tsv``, the sync will warn but still
proceed — vEcoli's ingestion will just never find a manifest there by
convention.

Step 3 — Author a campaign spec
--------------------------------

Campaign specs live in vEcoli at ``configs/campaigns/<name>.spec.json``
and describe *what* to perturb, not *how* to run Nextflow. Minimal shape:

.. code-block:: json

   {
     "name": "pilot_expression_noise",
     "source_dataset_id": "vecoli_m9_glucose_minus_aas",
     "operator": "add_log_normal_noise",
     "param_grid": {
       "sigma": [0.1, 0.2, 0.4],
       "seed":  [0, 1]
     },
     "include_source_as_baseline": true,
     "base_config": "configs/test_multi_parca.json",
     "sim": {
       "generations": 3,
       "n_init_sims": 3,
       "analysis_options": {
         "multiseed": { "cd1_higher_order_properties": {} },
         "multivariant": {
           "sensitivity_overview": {
             "campaign_sidecar": "configs/campaigns/pilot_expression_noise.campaign.json"
           }
         }
       }
     }
   }

``param_grid`` is Cartesian-product expanded — the example above generates
``3 × 2 = 6`` perturbed datasets plus 1 baseline = 7 variants.

Available operators: ``add_log_normal_noise``, ``scale_gene_set``,
``zero_genes``, ``drop_and_fill``, ``interpolate_datasets``,
``quantile_match``. See the vEcoli
``ecoli-sources/processing/perturbations.py`` module for signatures and
the ``sensitivity_campaigns.rst`` document for the full operator table.

Step 4 — Generate the Nextflow config
--------------------------------------

The meta-runner reads the spec, materializes perturbed TSVs under
``$ECOLI_SOURCES/data/perturbations/``, appends rows to the manifest, and
emits a Nextflow-ready JSON config:

.. code-block:: bash

   cd ~/code/vEcoli
   export ECOLI_SOURCES=$HOME/code/ecoli-sources
   export ECOLI_SOURCES_OVERLAYS=$HOME/code/ecoli-sources-vegas/data/manifest.tsv

   uv run runscripts/run_sensitivity_campaign.py \
       --spec configs/campaigns/pilot_expression_noise.spec.json

This produces two files:

- ``configs/campaigns/pilot_expression_noise.json`` — the Nextflow config
  atlantis will consume
- ``configs/campaigns/pilot_expression_noise.campaign.json`` — sidecar with
  the full generated dataset list, consumed by the
  ``sensitivity_overview`` analysis

The meta-runner is idempotent: re-running reuses the same perturbed TSVs
(hash-addressed by ``(operator, params, seed)``). Pass ``--dry-run`` to
preview, or ``--regenerate`` to overwrite.

Step 5 — Commit and push the vEcoli branch
-------------------------------------------

Atlantis builds the simulator from a git URL + branch, so the generated
config has to be reachable from GitHub:

.. code-block:: bash

   cd ~/code/vEcoli
   git checkout -b pilot-expression-noise
   git add configs/campaigns/
   # If you added a new analysis module, add it too:
   #   git add ecoli/analysis/multivariant/<your_module>.py
   git commit -m "add pilot expression noise campaign"
   git push origin pilot-expression-noise

Don't commit the perturbed TSVs under ``data/perturbations/`` — those are
gitignored and get regenerated at launch from the sync (Step 7 materializes
them in the container's S3-backed ``$ECOLI_SOURCES``).

Step 6 — Build the simulator
-----------------------------

.. code-block:: bash

   cd ~/code/viva-api
   uv run atlantis simulator latest \
       --repo-url https://github.com/CovertLab/vEcoli \
       --branch pilot-expression-noise

The command fetches the commit, uploads it, and polls the container build.
Save the **Simulator ID** from the output (e.g. ``Simulator ID: 23``).

For a private fork, use that repo URL instead — the list of accepted repos
is maintained server-side.

Step 7 — Launch the workflow with custom sources
-------------------------------------------------

.. code-block:: bash

   uv run atlantis simulation run pilot-expression-noise 23 \
       --config-filename campaigns/pilot_expression_noise.json \
       --sources ../ecoli-sources \
       --sources ../ecoli-sources-vegas \
       --run-parca \
       --poll

.. note::

   ``--config-filename`` is relative to the repo's ``configs/`` directory —
   the server prepends ``configs/`` itself and will reject values that
   start with ``configs/``.

What happens:

1. Each ``--sources`` directory is synced to
   ``s3://{STORAGE_S3_BUCKET}/sources/<basename>/`` via ``aws s3 sync``
   (``.venv/``, ``__pycache__/``, ``.git/`` excluded).
2. The primary URI is threaded to the container as ``ECOLI_SOURCES``;
   subsequent source manifests are joined with ``;`` into
   ``ECOLI_SOURCES_OVERLAYS``.
3. Nextflow launches one ParCa per variant (multi-parca fan-out), then
   simulations, then analyses — all reading from the S3-backed sources.

``--run-parca`` is required for campaigns (each variant needs its own
ParCa). ``--poll`` prints status every 30 s; drop it for fire-and-forget.

See :doc:`cli-reference` for the full option list, including
``--sources-prefix`` and ``--sources-delete``.

Step 8 — Download results
--------------------------

There are two tiers of output retrieval: the CLI tarball (small, curated)
and direct S3 sync (everything).

Find your real experiment id
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The server decorates the ``experiment_id`` you passed at launch. The actual
id on S3 is ``sim{simulator_id}-{your_id}-{4-char-uuid}``, e.g.
``sim23-pilot-expression-noise-a3f2``. Retrieve it:

.. code-block:: bash

   uv run atlantis simulation get <SIM_ID>
   # Look for the config.experiment_id field.

Use this decorated id everywhere you hit S3 directly.

Option A — CLI tarball (curated subset)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   uv run atlantis simulation outputs <SIM_ID> --dest ./results

This packages, from the server-side output prefix:

- every ``.tsv`` and ``.json`` under ``analyses/``
- ``nextflow/workflow_config.json``

and streams them as a tar.gz into ``./results/``. Everything else on S3
(Parquet history, daughter_states, per-variant ``parca_*/kb``,
``variant_sim_data/``, HTML plots) is excluded to keep the archive small
and the response fast. If you only want the headline metric tables, this
is the fastest path.

Option B — Full S3 tree (``aws s3 sync``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the HTML plots, Parquet history, daughter_states, or the ParCa
pickles, sync the output prefix directly. The vEcoli Nextflow publish
convention nests the id twice (``publishDir/{id}/``):

.. code-block:: bash

   EID=sim23-pilot-expression-noise-a3f2   # decorated id from `simulation get`
   aws s3 sync \
       "s3://$S3_WORK_BUCKET/vecoli-output/$EID/$EID/" \
       "./results/$EID/"

Layout you'll get:

.. code-block:: text

   results/<EID>/
     parca_{0..N-1}/kb/                     # per-variant ParCa outputs
     variant_sim_data/
     history/                               # Parquet, one shard per sim
     daughter_states/
     analyses/
       variant={0..N-1}/
         plots/analysis=cd1_higher_order_properties/ ...
         plots/analysis=mass_fraction_summary/ ...
       plots/analysis=sensitivity_overview/
         sensitivity_overview.html          # 4-panel axis-vs-metric scatter
         sensitivity_overview.tsv           # per-variant metric table
     nextflow/workflow_config.json

The headline deliverable is ``sensitivity_overview.tsv`` —
per-variant ``mass_drift_per_gen_fg`` is the primary "unhealthy sim"
signal, ``frac_max_gen`` tells you which variants never finished, and
``axis_value`` is the operator-parameter value for the x-axis (e.g.
``sigma`` for ``add_log_normal_noise``).

For a post-hoc run summary (parca status, durations, failure reasons)
outside the Nextflow analysis graph:

.. code-block:: bash

   cd ~/code/vEcoli
   uv run wholecell/io/multiparca_analysis.py \
       --out_dir results/<EID> \
       -o results/<EID>/reports/

Troubleshooting
---------------

- **``--sources`` refuses to run**: check ``aws sts get-caller-identity``
  and ``$STORAGE_S3_BUCKET``. The CLI exits early if either is missing.
- **Parca fails with "dataset_id not found in manifest"**: your spec
  referenced a ``source_dataset_id`` that isn't in any of the manifests you
  synced. Re-check ``ecoli-sources/data/manifest.tsv``.
- **Parca fails with "duplicate dataset_id"**: a ``dataset_id`` is defined
  in both the primary and an overlay manifest. Rename one.
- **``atlantis`` can't reach the API**: on a GovCloud/VPC deployment, the
  SSM tunnel from Step 1b must be running in another terminal and
  ``API_BASE_URL`` must point at ``http://localhost:<tunnel-port>``.
- **SSM tunnel exits with "TargetNotConnected"**: the submit-node EC2 may
  be stopped, or your SSO credentials have expired. Re-run
  ``aws sso login --profile <profile>`` and retry.
- **Simulator build fails with "branch not allowed"**: your repo/branch
  isn't on the server-side accept list. Push to ``CovertLab/vEcoli`` or
  contact the admin.
- **``simulation outputs`` returned only ``workflow_config.json``**:
  the ``analyses/`` prefix on S3 has no ``.tsv``/``.json`` files — the
  curated tarball excludes everything else. Two common causes:

  - The workflow didn't reach the analysis step (failed upstream, or still
    running). Check ``atlantis simulation status <id>`` and
    ``atlantis simulation log <id>``.
  - The analyses ran but only produced HTML plots (e.g. plotly figures
    with no companion TSV). Use Option B (``aws s3 sync``) above instead —
    HTMLs are on S3, they're just filtered out of the tarball.

  To confirm what's actually on S3, list with the decorated id from
  ``simulation get``:

  .. code-block:: bash

     aws s3 ls "s3://$S3_WORK_BUCKET/vecoli-output/<EID>/<EID>/" --recursive

See also
--------

- :doc:`end-to-end-workflow` — non-campaign simulation workflow (single
  ParCa, no custom sources)
- :doc:`cli-reference` — all ``atlantis simulation run`` flags
- :doc:`aws-s3-setup` — bucket + IAM setup for ``--sources``
- vEcoli ``doc/sensitivity_campaigns.rst`` — full campaign design,
  operator catalog, schema validation, output columns
