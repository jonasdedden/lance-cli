# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Renamed the project to `lance-cli`.** The crate on crates.io is now
  `lance-cli` (was `arrs-cli`), the PyPI package is `lance-cli` (was
  `rust-arrs`), the binary is `lance-cli` (was `arrs`), and the library is
  `lance_cli` (was `arrs`). Reinstall under the new name; no flag, subcommand,
  or output format changed. Existing shell-completion scripts are stale and
  should be regenerated with `lance-cli completions <shell>`.
- **The tool is now Lance-only.** It was structured to grow into other
  Arrow-backed formats — a `Dataset` trait with a Lance adapter behind it, a
  `LanceCapabilities` side-trait for everything Lance could do that the trait
  could not express, and per-format capability probes. Nothing else was ever
  implemented, so the indirection is gone: commands now talk to a concrete
  `LanceDataset` directly.
  - `--with-row-id` / `--with-row-addr` and `--branch` / `--version` / `--tag` /
    `--as-of` are no longer documented as "Lance only", because everything is.
  - Opening a path that is not a Lance dataset now reports `not a Lance
    dataset: <path>` instead of `unrecognised dataset format at <path>`.
  - The `--help` option group `Lance options` is now `Version options`;
    `--with-row-id` / `--with-row-addr` moved to `Selection options`.
  - The `--help` command sections are now `Commands` (everything that reads
    rows, including `search`) and `Metadata commands` (everything answered from
    the manifest), replacing the old format-agnostic/Lance-only split.
  - `doc/lance.md` is now `doc/versions.md`.
- Trimmed two dependencies that the crate no longer needs. `async-trait` is gone
  entirely (nothing implements a trait any more), and `anyhow` moved to
  `[dev-dependencies]` — only the fixture example ever used it, so it was being
  pulled into every downstream build for nothing.

## [0.2.1] - 2026-08-27

### Changed

- Rewrote the README. It is now a short overview — install, command tables,
  global flags, and worked examples with real output — with the reference
  material moved into `doc/`: install and shell completions, a per-command
  reference, columns and filters, output formats, Lance versions and row
  identity, diffing, and remote object stores. Links are absolute so they
  resolve on both crates.io and PyPI.
- Updated the GitHub Actions used by CI and the release workflow:
  `actions/checkout` v7, `actions/download-artifact` v8, `actions/setup-python`
  v7, and `astral-sh/setup-uv` pinned to v10.0.1 (upstream publishes no `v10`
  major alias).

### Fixed

- The PyPI project page had no summary, showing "None" in search results.
  `description` is now declared dynamic in `pyproject.toml`, so maturin takes
  the `Summary` metadata field from `Cargo.toml` instead of omitting it.
- The release workflow could report success without publishing anything. It now
  verifies up front that the tag matches the `Cargo.toml` version and that the
  CHANGELOG has a matching section, no longer passes `skip-existing` to the
  PyPI upload (which turned "published nothing" into a green check), and
  asserts after each publish that the version is actually live on the registry.

## [0.2.0] - 2026-07-22

### Added

- `--where <predicate>` SQL-style predicate filtering for `cat`, `head`, `tail`,
  `rowcount`, and `sample`. The filter is applied before row selection, and
  filtered `rowcount` uses the backend's native filtered count (pushed into
  Lance scalar indices when available). (#6)
- `fragments` command (Lance only): lists dataset fragments with physical rows,
  deleted rows, data file count/paths and on-disk size, with a table-mode summary
  line, `--verbose` and `--no-size` flags, and `--branch`/`--version`/`--tag`
  support (#15).
- Support remote object-store URIs (`s3://`, `gs://`, `az://`, `file://`) as
  dataset inputs for every command, using ambient environment credentials (#7).
- `search` command: Lance nearest-neighbor (ANN / flat KNN) vector search with
  `--column`, `--vector`/`--vector-file`, `-k`, `--nprobes`, `--refine-factor`,
  an appended `_distance` column, and projection support (#16).
- `stats` command: per-column summary statistics (a `df.describe()` for
  datasets). Streams the dataset once with memory independent of the row count,
  reporting count, nulls, min/max (numeric, temporal, string, boolean),
  mean/stddev (numeric only, sample stddev), and an approximate distinct count
  (exact up to a cap, then reported as `>10000`). Nested, binary, decimal, and
  dictionary columns report count/nulls only and never error. Respects
  `--columns`/`--exclude-columns`, composes with `--where`, and renders in
  `table`/`jsonl`/`csv`. A default-`None` `Dataset::stats` trait hook leaves the
  door open for backends to answer from metadata instead of scanning. (#8)
- `index-stats` command reporting per-index coverage — `indexed_rows`,
  `unindexed_rows`, and a `coverage` percentage — which diverge as rows are
  appended after an index is built. `--format jsonl` adds a `detail` column
  carrying Lance's raw statistics JSON verbatim so type-specific internals
  (IVF partitions, PQ sub-vectors, …) pass through unmodified. (#17)
- `--as-of <instant>` time travel by timestamp (Lance only): reads the latest
  version whose commit timestamp is at or before the given instant on the chosen
  branch. Accepts RFC 3339 with offset, a naive datetime (interpreted as UTC),
  or a date (midnight UTC); mutually exclusive with `--version`/`--tag`, combines
  with `--branch`; echoes the resolved version on stderr. (#18)
- `freq` command: value counts for a column. Emits one `value`/`count`/`percent`
  row per distinct value (with an explicit `NULL` row), rendered in any output
  format. Supports `-n/--limit` (≥ 1; folding the remainder into an `<other>`
  row), `--sort count|value` with type-aware ordering (numbers numerically,
  temporals chronologically, `NULL`/`NaN` last) and a deterministic string
  tie-break, and `--where` composition; rejects nested/binary columns and guards
  against runaway cardinality (~1M distinct values). (#9)
- Richer `--columns` / `--exclude-columns` projection: shell-style glob patterns
  (`*`, `?`) matched against top-level column names (schema-order expansion,
  no-match is an error), and nested struct field paths (`meta.user.id`) validated
  against the Arrow schema. A token that exactly matches a top-level column is
  taken literally (escapes globs/dots). Nested leaves are surfaced as flat,
  dotted-named columns (matching Lance's scanner) across `head`/`cat`/`take`/
  `sample`/`tail`/`schema`; excluding a nested path prunes that leaf and flattens
  the struct's surviving leaves. (#10)
- `stat` command (Lance only): a one-screen, metadata-only dataset health
  summary — path, manifest version, live/deleted rows (with ratio), columns,
  fragment count with min/max/median row spread, on-disk data size, and
  version/branch/tag/index counts. Runs the underlying lookups concurrently and
  makes no data scan, so it stays instant on large datasets. Respects
  `--branch`/`--version`/`--tag`, has a `--no-size` escape hatch, and appends a
  conservative plain-language compaction hint when fragment/deletion thresholds
  are crossed. `--format jsonl` emits a single stable-schema object with raw
  numbers for scripting; `table`/`csv` render a two-column key/value view. Named
  as the singular of the per-column `stats` command (the `stat(1)` analogy);
  `--help` cross-references the two. (#20)
- `diff` command (Lance only): compare two versions of one dataset and report
  row deltas (added/deleted split from fragment metadata, not just net), schema
  changes (columns added/removed/retyped), fragment changes (added/removed/
  rewritten), index changes (created/dropped), and the version log of the
  `(from, to]` range. Endpoints select by `--from`/`--to` version or
  `--from-tag`/`--to-tag`, scoped with `--branch`; `--to` defaults to the branch
  latest. Human-readable summary by default, `--format jsonl` for a single
  machine-readable record. Exit codes follow `diff(1)`: `0` identical, `1`
  different, `2` error. (#19)
- Output-control flags for the rendering layer (#11):
  - `--max-list-items N`: truncate list / large-list / fixed-size-list
    rendering to the first `N` elements, appending an explicit `… (K more)`
    marker element. Applied per nesting level, in `jsonl` and nested
    table cells; the marker is a JSON string so arrays stay valid JSON. CSV
    still rejects nested columns.
  - `--max-cell-width N`: `table`-only per-cell truncation to at most `N`
    characters with a trailing `…`, counted on character boundaries (never
    splits a multi-byte UTF-8 codepoint), identical for TTY and piped output.
  - `--float-precision N`: render `f16`/`f32`/`f64` with exactly `N` fractional
    digits in every format (`NaN`/`Infinity` unaffected).

  Truncation and precision are rendering-only and lossy (documented as such);
  defaults leave existing output byte-identical. `BinaryFormat` is now bundled
  with these knobs into a single `RenderOptions` value threaded through the
  writers.
- `diff` dataset-vs-dataset mode: `arrs diff A B` compares two *different*
  datasets (generic over the backend) by schema, Arrow schema-metadata, and row
  count, reporting columns added/removed/retyped (nested types compared
  structurally, nullability folded into the type label), metadata keys added/
  removed/changed, and the row-count delta. `--columns`/`--exclude-columns` scope
  the comparison to the projected columns; row-level content is not compared (an
  explicit non-goal). The single `diff` verb now spans two modes: two positional
  datasets select this mode (Lance version selectors are then rejected), one
  dataset plus `--from`/`--from-tag` selects the version diff (#19). Human summary
  by default, `--format jsonl` for a stable machine-readable record; exit codes
  follow `diff(1)` (`0` identical, `1` differ, `2` error). The schema-delta
  rendering is shared with the version diff via `commands::diff_common`. (#13)
- `blob` command: extract a single cell's binary/blob payload to a file
  (`-o <file>`) or stdout. Takes `--column` and a single `--index` (with `take`'s
  negative-index semantics). Works on plain `Binary`/`LargeBinary`/
  `FixedSizeBinary`/`BinaryView` columns via `Dataset::take`, and on Lance
  blob-encoded columns (`lance-encoding:blob`) via the streaming blob API
  (`take_blobs_by_indices`), which are detected from field metadata and read in bounded
  chunks so multi-GB payloads never materialize in memory. Refuses to write raw
  bytes to a terminal, errors (non-zero exit) on null cells / out-of-range
  indices / non-binary columns, and never leaves a partial file on `-o` (temp
  file + atomic rename). Honours `--branch`/`--version`/`--tag`/`--as-of`. (#22)
- `completions <shell>` command: prints a shell completion script (bash, zsh,
  fish, powershell, elvish) to stdout, generated from the CLI definition via
  `clap_complete`. Requires no dataset input and bypasses the format/output
  machinery. See the README install section for per-shell installation. (#14)
- Scan progress indicator on **stderr** for long scans (`cat`, `stats`, `freq`,
  and the filtered `head`/`tail`/`sample` paths). Shown only when stderr is a
  TTY and never on stdout, so piping is unaffected; a bar with an ETA when the
  row total is known cheaply (no filter), otherwise a rows-scanned spinner. Opt
  out with the global `--no-progress` flag. (#14)
- `cat` expands glob patterns in its inputs (`data/part_*.lance`) when the shell
  did not, matching `*`/`?`/`[` against local paths, concatenating matches in
  lexicographic order, and erroring clearly on no match. A literal path that
  exists is used as-is even if it contains glob metacharacters, and remote URIs
  pass through untouched. (#14)
- `--with-row-id` / `--with-row-addr` output columns (Lance only) for `cat`,
  `head`, `tail`, `take`, and `sample`: append the row's identity (`_rowid`,
  stable across deletions — and across compaction only for datasets written with
  Lance's stable row ids enabled) and/or its physical address
  (`_rowaddr` = `fragment_id << 32 | offset`) as `UInt64` columns, after the
  projected columns (`_rowid` before `_rowaddr`). Emitted whenever the flag is
  set regardless of
  `--columns`/`--exclude-columns`; explicitly excluding a requested pseudo-column
  errors with a hint to drop the flag. Values are consistent across commands for
  the same rows (scan and `take` paths alike) and stay correct across deletions
  (surviving `_rowid`s become non-contiguous). A `Dataset::supports_row_id`
  capability hook rejects the flags on formats that can't provide them with a
  clear "not supported by this format" error. (#21)
- `--format ipc` Arrow IPC streaming output for the row-producing commands
  (`cat`, `head`, `tail`, `take`, `sample`), writing the lossless Arrow IPC
  stream to stdout via `arrow::ipc::writer::StreamWriter` at constant memory.
  Bypasses value rendering entirely, making arrs composable with DuckDB
  (`read_arrow`), pyarrow, Polars, and ADBC tools with zero fidelity loss.
  Refuses to write to a terminal, rejects the inapplicable value-rendering flags
  (`--binary-format`/`--max-list-items`/`--max-cell-width`/`--float-precision`),
  and is rejected on metadata/summary commands (a possible follow-up). (#12)

### Changed

- Error exit code is now `2` (was `1`) for every command, so that exit code `1`
  is reserved for `diff` reporting that two versions differ (mirroring
  `diff(1)`). `0` still means success. (#19)
- `commands::dispatch` now returns `Result<commands::Outcome>` instead of
  `Result<()>`, carrying whether `diff` found the versions identical or
  different so `main` can pick the exit code. (#19)
- `Dataset::scan` now takes a `ScanOptions` struct (projection + filter) instead
  of a positional projection argument; `Dataset::count_rows` takes an optional
  filter. (#6)
- `indices` output now includes an index `type` column (`BTree`, `IVF_PQ`,
  `INVERTED`, …), sourced from Lance's index statistics. (#17)
- Library API: `arrs::stats::compute` gained a `&ScanProgress` parameter (used
  to drive the scan progress indicator). This is a breaking signature change for
  direct library consumers — acceptable under 0.x, but noted here. CLI users are
  unaffected. (#14)
- `ScanOptions` gained a `row_ids` field and `Dataset::take` a `row_ids`
  argument, both carrying the `--with-row-id`/`--with-row-addr` selection. (#21)
- `--help` now groups options into labelled sections instead of one flat list:
  `Selection options` (`--columns`/`--exclude-columns`/`--where`), `Output
  options` (`--format`/`--binary-format`/`--max-list-items`/`--max-cell-width`/
  `--float-precision`/`--no-progress`), and `Lance options` (the
  `--branch`/`--version`/`--tag`/`--as-of` selectors and
  `--with-row-id`/`--with-row-addr`). Command-specific flags and positionals stay
  in the default block. Applied uniformly across every command via clap
  `help_heading`/`next_help_heading`. (#47)
- `arrs --help` now groups the subcommand list into labelled sections —
  `Commands` (format-agnostic: `cat`/`head`/`tail`/`take`/`sample`/`rowcount`/
  `stats`/`freq`/`schema`/`diff`/`blob`), `Lance commands`
  (`stat`/`versions`/`branches`/`tags`/`indices`/`index-stats`/`fragments`/
  `search`), and `Setup` (`completions`/`help`) — instead of one flat block.
  Shell completions still include every command (they are generated from a
  separate, un-hidden command tree), and `arrs help <cmd>` / `arrs <cmd> --help`
  are unaffected. (#50)
