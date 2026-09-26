# plan-core decision log — one file per entry (since 2026-09-26)

`docs/plan-core.md`'s decision log was a single list that every lane prepended to, so parallel PRs
conflicted on it and each merge after the first cost a resolution and a CI re-run. From 2026-09-26
a new entry is **one file here**, named `<YYYY-MM-DD>-<lane>-<slug>.md`, and the ledger row in
`plan-core.md` is touched only by a **docs PR that folds a day's entries** — not by the PR that
does the work.

Lanes (2026-09-26): `a` = UConn core (`u3/`, `u4/` branches), `b` = Stanford checkpoints and the
prod jump (`f3/`, `pjump/`), `c` = core code (`p4/`, `p5/`). A lane writes only its own entries.

An entry is what a log entry always was: dated, one concern, what was done, what it proved, what it
deliberately left out, and any decision it records — written so a reader in six months needs no
other context. The heading line is `- **<date>** — **<title>.**` exactly as in `plan-core.md`, so
the fold is a concatenation.
