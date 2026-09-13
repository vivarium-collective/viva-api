// A 3-process toy shaped like a v2ecoli campaign (parca -> lineage -> analysis),
// used to produce REAL Nextflow trace.csv / .nextflow.log fixtures for viva-api's
// tests (tests/fixtures/nextflow/README.md). No science, no AWS.
nextflow.enable.dsl = 2

params.fail_analysis = false
params.flaky = false

process parca_v0 {
    output:
    path 'cache.txt'
    script:
    "echo chassis > cache.txt"
}

process lineage_v0_s0 {
    input:
    path cache
    output:
    path 'history.txt'
    script:
    "cat $cache > history.txt; echo generation0 >> history.txt"
}

process flaky_v0 {
    errorStrategy 'retry'
    maxRetries 1
    input:
    path history
    output:
    path 'flaky.txt'
    script:
    """
    if [ ${task.attempt} -eq 1 ]; then echo 'simulated OOM on attempt 1' >&2; exit 137; fi
    echo ok > flaky.txt
    """
}

process analysis_v0 {
    input:
    path history
    output:
    path 'analysis.json'
    script:
    if (params.fail_analysis)
        """
        echo 'Traceback (most recent call last):' >&2
        echo "FileNotFoundError: [Errno 2] No such file or directory: 'analysis.config.json'" >&2
        exit 1
        """
    else
        "echo '{}' > analysis.json"
}

workflow {
    cache = parca_v0()
    history = lineage_v0_s0(cache)
    if (params.flaky) { flaky_v0(history) }
    analysis_v0(history)
}
