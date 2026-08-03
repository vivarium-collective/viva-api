"""Unit tests for infer_n_tp_from_tsv (pure, no DB)."""

from viva_api.analysis.models import AVAILABLE_NTP, infer_n_tp_from_tsv


def _tsv(n_tp: int) -> str:
    # Header: a leading non-`t` label column, then n_tp columns named t0, t1, ...
    header = "\t".join(["$"] + [f"t{i}" for i in range(n_tp)])
    row = "\t".join(["geneA"] + ["1.0"] * n_tp)
    return f"{header}\n{row}\n"


def test_counts_t_columns_for_available_ntp() -> None:
    for n in AVAILABLE_NTP:
        assert infer_n_tp_from_tsv(_tsv(n)) == n


def test_ignores_non_t_columns() -> None:
    header = "\t".join(["$", "geneID", "t0", "t1", "t2", "name"])
    assert infer_n_tp_from_tsv(f"{header}\nx\ty\t1\t2\t3\tz\n") == 3


def test_reads_only_the_header_line() -> None:
    # trailing data rows (even with leading 't' cells) must not affect the count
    text = "$\tt0\tt1\ntotals\t9\t9\n"
    assert infer_n_tp_from_tsv(text) == 2


def test_zero_when_no_t_columns() -> None:
    assert infer_n_tp_from_tsv("$\tgeneID\tvalue\n") == 0
