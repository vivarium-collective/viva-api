from viva_api.compose.container_def import build_pbg_def


def test_def_installs_process_bigraph_and_embeds_runner() -> None:
    d = build_pbg_def("pbg").representation
    assert "Bootstrap: docker" in d
    assert "pip install --no-cache-dir process-bigraph bigraph-schema" in d
    assert "/opt/run_pbg.py" in d
    assert "%runscript" in d


def test_def_injects_extra_pip_deps() -> None:
    d = build_pbg_def("pbg", ["git+https://github.com/x/y.git@abc"]).representation
    assert "git+https://github.com/x/y.git@abc" in d
