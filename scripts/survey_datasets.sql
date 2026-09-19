-- Survey the dataset registry after a walk: is what landed any good?
--
-- Written for docs/runbook-dataset-walk.md step 5, against the restored local copy:
--
--     docker exec -i viva-dump psql -U postgres -d postgres -q -f - < scripts/survey_datasets.sql
--
-- It is read-only and works against any database with a `dataset` table, so it is also the
-- quickest quality check on a real site (through a pod's psql) once the walk is enabled there.
--
-- What to look for, in order of how loudly it fails: `duplicate_uris` must be 0 (it is the
-- upsert key); a producer of NONE is impossible by CHECK constraint; missing coordinates
-- should line up exactly with the aggregate protocols (a multiseed table has no single seed),
-- and a missing `n_tp` should be a ptools_overview file, whose header carries commentary
-- rather than a timepoint row.

\echo '=== 1. shape: what landed, by kind and origin'
SELECT kind,
       attributes->>'origin' AS origin,
       count(*) AS rows,
       count(*) FILTER (WHERE NOT available) AS gone
FROM dataset GROUP BY 1, 2 ORDER BY 3 DESC;

\echo '=== 2. producer: claimed by an analysis run, or found under a simulation'
SELECT CASE WHEN analysis_id IS NOT NULL THEN 'analysis run'
            WHEN parca_dataset_id IS NOT NULL THEN 'parca dataset'
            WHEN simulation_id IS NOT NULL THEN 'simulation (found under)'
            ELSE 'NONE — should be impossible' END AS producer,
       count(*) AS rows,
       count(DISTINCT attributes->>'analysis_dir') AS bundles
FROM dataset GROUP BY 1 ORDER BY 2 DESC;

\echo '=== 3. coordinate coverage on ptools tables (the fields a picker needs)'
SELECT count(*) AS ptools_rows,
       count(*) FILTER (WHERE view IS NOT NULL) AS has_view,
       count(*) FILTER (WHERE attributes ? 'protocol') AS has_protocol,
       count(*) FILTER (WHERE attributes ? 'variant') AS has_variant,
       count(*) FILTER (WHERE attributes ? 'seed') AS has_seed,
       count(*) FILTER (WHERE attributes ? 'generation') AS has_generation,
       count(*) FILTER (WHERE attributes ? 'agent') AS has_agent,
       count(*) FILTER (WHERE attributes ? 'n_tp') AS has_n_tp
FROM dataset WHERE kind = 'ptools-analysis';

\echo '=== 4. the missing coordinates should BE the aggregates: protocol distribution'
SELECT coalesce(attributes->>'protocol', '(none)') AS protocol, count(*) AS rows
FROM dataset WHERE kind = 'ptools-analysis' GROUP BY 1 ORDER BY 2 DESC;

\echo '=== 5. n_tp read from the TSV headers, and which views lack it'
SELECT attributes->>'n_tp' AS n_tp, count(*) AS rows
FROM dataset WHERE kind = 'ptools-analysis' GROUP BY 1 ORDER BY count(*) DESC LIMIT 10;

SELECT view, count(*) AS rows, count(*) FILTER (WHERE NOT (attributes ? 'n_tp')) AS missing_n_tp
FROM dataset WHERE kind = 'ptools-analysis'
GROUP BY 1 ORDER BY missing_n_tp DESC, rows DESC;

\echo '=== 6. integrity: duplicate uris (the upsert key — must be zero), empty names, missing size'
SELECT (SELECT count(*) FROM (SELECT uri FROM dataset GROUP BY uri HAVING count(*) > 1) d) AS duplicate_uris,
       count(*) FILTER (WHERE display_name IS NULL OR display_name = '') AS no_display_name,
       count(*) FILTER (WHERE size_bytes IS NULL) AS no_size,
       count(*) FILTER (WHERE source IS NULL) AS no_source,
       count(*) FILTER (WHERE tags = '[]'::jsonb OR tags IS NULL) AS no_tags
FROM dataset;

\echo '=== 7. untagged rows are inherited, not lost: tags follow the producing simulation'
SELECT CASE WHEN analysis_id IS NOT NULL THEN 'analysis-claimed' ELSE 'simulation (found under)' END AS producer,
       count(*) AS rows,
       count(*) FILTER (WHERE tags = '[]'::jsonb OR tags IS NULL) AS untagged
FROM dataset GROUP BY 1;

\echo '=== 8. per simulation: what the walk recovered'
SELECT d.simulation_id, s.experiment_id, count(*) AS rows,
       count(DISTINCT d.attributes->>'analysis_dir') AS bundles,
       count(*) FILTER (WHERE d.kind = 'ptools-analysis') AS ptools,
       count(*) FILTER (WHERE d.kind = 'figure') AS figures,
       count(*) FILTER (WHERE d.kind = 'report') AS reports
FROM dataset d LEFT JOIN simulation s ON s.id = d.simulation_id
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;

\echo '=== 9. analyses that claim a bundle, and how many rows each got'
SELECT d.analysis_id, a.name, a.backend, a.status, count(*) AS rows
FROM dataset d JOIN analysis a ON a.id = d.analysis_id
GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC LIMIT 10;

\echo '=== 10. coverage: how much of the fleet the walk actually touched'
SELECT (SELECT count(*) FROM simulation) AS simulations_in_db,
       (SELECT count(DISTINCT simulation_id) FROM dataset) AS simulations_with_rows,
       (SELECT count(*) FROM analysis) AS analyses_in_db,
       (SELECT count(DISTINCT analysis_id) FROM dataset WHERE analysis_id IS NOT NULL) AS analyses_claiming;

\echo '=== 11. registered bytes, and the size distribution the content route will serve'
SELECT count(*) AS rows,
       pg_size_pretty(sum(size_bytes)) AS total,
       pg_size_pretty(max(size_bytes)) AS largest,
       pg_size_pretty((percentile_disc(0.5) WITHIN GROUP (ORDER BY size_bytes))::bigint) AS median
FROM dataset;

\echo '=== 12. a full row, to eyeball attributes, tags and source'
SELECT id, kind, view, display_name, size_bytes, available, attributes, tags, source
FROM dataset WHERE kind = 'ptools-analysis' ORDER BY id LIMIT 1;
