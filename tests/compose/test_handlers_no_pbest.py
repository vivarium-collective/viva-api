import pathlib

import viva_api.compose.handlers as h


def test_handlers_module_has_no_pbest_import() -> None:
    src = pathlib.Path(h.__file__).read_text()
    assert "pbest" not in src
