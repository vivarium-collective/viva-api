"""Tests for the Nextflow log truncation logic."""

from viva_api.common.handlers.simulations import _truncate_log


class TestTruncateLog:
    def test_short_log_unchanged(self) -> None:
        """Logs shorter than head + tail threshold are returned as-is."""
        short = "\n".join(f"line {i}" for i in range(20))
        assert _truncate_log(short) == short

    def test_long_log_with_executor(self) -> None:
        """Long log with 'executor' line is truncated to head + executor block."""
        head = [f"header line {i}" for i in range(25)]
        middle = [f"middle line {i}" for i in range(200)]
        tail = [
            "executor >  awsbatch (11)",
            "[7f/6d9197] runParca | 1 of 1 done",
            "Completed at: 09-Apr-2026",
            "Duration    : 14m 26s",
            "Succeeded   : 11",
        ]
        full = "\n".join(head + middle + tail)
        result = _truncate_log(full)
        # Head should be first 20 lines
        result_lines = result.splitlines()
        for i in range(20):
            assert f"header line {i}" in result_lines[i]
        # Should contain truncation marker
        assert "... truncated ..." in result
        # Should contain the executor tail
        assert "executor >  awsbatch (11)" in result
        assert "Succeeded   : 11" in result
        # Should NOT contain middle lines
        assert "middle line 100" not in result

    def test_long_log_without_executor_uses_last_15(self) -> None:
        """When no 'executor' line exists, fallback to last 15 lines."""
        lines = [f"line {i}" for i in range(100)]
        full = "\n".join(lines)
        result = _truncate_log(full)
        assert "... truncated ..." in result
        assert "line 99" in result
        assert "line 85" in result
        assert "line 50" not in result

    def test_truncation_marker_present(self) -> None:
        lines = [f"line {i}" for i in range(100)]
        lines[80] = "executor >  awsbatch (5)"
        full = "\n".join(lines)
        result = _truncate_log(full)
        assert "... truncated ..." in result
