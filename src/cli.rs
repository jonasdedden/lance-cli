use std::path::PathBuf;

use clap::{ArgGroup, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Csv,
    Jsonl,
    /// Pretty-printed table for interactive use; nested cells are JSON-encoded.
    /// Buffers all rows before printing, so prefer `jsonl`/`csv` for very large
    /// inputs piped through `cat`/`head`/etc.
    Table,
    /// Arrow IPC streaming format written to stdout, for lossless composition
    /// with the Arrow ecosystem (DuckDB, Polars, pyarrow, ADBC). Batches go
    /// straight from the scan to `arrow::ipc::writer::StreamWriter`, so output is
    /// fully streaming at constant memory. Only valid on the row-producing
    /// commands (`cat`/`head`/`tail`/`take`/`sample`); the value-rendering flags
    /// (`--binary-format`, `--max-list-items`, `--max-cell-width`,
    /// `--float-precision`) do not apply to it and are rejected. lance-cli
    /// refuses to write it to a terminal — redirect or pipe it.
    Ipc,
}

/// How to render Binary / LargeBinary / FixedSizeBinary / BinaryView values.
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum BinaryFormat {
    /// Drop top-level binary columns from output; render nested binary as null.
    None,
    /// `\xHH` lowercase-hex-escaped strings.
    Hex,
    /// Standard-alphabet base64 strings.
    Base64,
}

/// Ordering for `freq` output rows.
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum FreqSort {
    /// Most frequent values first; ties broken by value (ascending, NULL last).
    Count,
    /// Values in ascending order (NULL last).
    Value,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum SchemaType {
    /// Logical arrow schema.
    Arrow,
    /// Physical (Lance-native) schema.
    Physical,
}

/// Selectors for which version of a dataset to read.
///
/// `--branch` is independent and can be combined with any of `--version`,
/// `--tag`, or `--as-of`. `--version`, `--tag`, and `--as-of` all name a
/// single version and are therefore mutually exclusive. With no flags set,
/// the latest version of `main` is used.
#[derive(Debug, Clone, Args, Default)]
#[command(next_help_heading = "Version options")]
pub struct VersionArgs {
    /// Read from the named branch (default: main).
    #[arg(long)]
    pub branch: Option<String>,

    /// Read from a specific version on the chosen branch.
    #[arg(long, conflicts_with = "tag")]
    pub version: Option<u64>,

    /// Read from a specific tag on the chosen branch.
    #[arg(long, conflicts_with = "version")]
    pub tag: Option<String>,

    /// Read the latest version whose commit timestamp is at or before this
    /// instant, on the chosen branch. Accepts RFC 3339 with an offset
    /// (`2026-07-01T12:00:00Z`), a naive datetime interpreted as UTC
    /// (`2026-07-01T12:00:00`), or a date interpreted as midnight UTC
    /// (`2026-07-01`).
    #[arg(long = "as-of", conflicts_with_all = ["version", "tag"])]
    pub as_of: Option<String>,
}

/// SQL-style row predicate shared by every row-producing command (and
/// `rowcount`). Kept as its own flattened `Args` group so the flag definition
/// lives in exactly one place.
#[derive(Debug, Clone, Args, Default)]
#[command(next_help_heading = "Selection options")]
pub struct FilterArg {
    /// Keep only rows matching this SQL-style predicate (e.g.
    /// `"score > 0.5 AND split = 'test'"`). Applied before row selection, so
    /// `head`/`tail`/`sample` operate on the matching rows.
    #[arg(long = "where", value_name = "PREDICATE")]
    pub predicate: Option<String>,
}

/// The `--with-row-id` / `--with-row-addr` pseudo-column flags, shared by the
/// row-producing commands (`cat`/`head`/`tail`/`take`/`sample`). Kept as its own
/// flattened `Args` group so the flag definitions live in one place.
#[derive(Debug, Clone, Args, Default)]
#[command(next_help_heading = "Selection options")]
pub struct RowIdArgs {
    /// Append a `_rowid` column: the per-row identity. Stable
    /// across deletions; stable across compaction only for datasets written with
    /// Lance's stable row ids enabled (off by default, in which case `_rowid` is
    /// address-based and is rewritten by compaction). Always emitted regardless
    /// of --columns/--exclude-columns.
    #[arg(long = "with-row-id")]
    pub with_row_id: bool,

    /// Append a `_rowaddr` column: the physical address of the row
    /// (`fragment_id << 32 | offset`) in the current version. Always emitted
    /// regardless of --columns/--exclude-columns.
    #[arg(long = "with-row-addr")]
    pub with_row_addr: bool,
}

impl RowIdArgs {
    /// Convert the parsed flags into the [`crate::row_id::RowIds`] threaded
    /// through `ScanOptions` and `LanceDataset::take`.
    pub fn flags(&self) -> crate::row_id::RowIds {
        crate::row_id::RowIds {
            with_row_id: self.with_row_id,
            with_row_addr: self.with_row_addr,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "lance-cli", about = "Inspect Lance datasets.", version)]
pub struct Cli {
    // Global flags are grouped into `--help` sections via per-arg `help_heading`
    // (see also the `next_help_heading` on the flattened VersionArgs/RowIdArgs/
    // FilterArg structs). clap derives help-section *ordering* from the order in
    // which each heading is first encountered while registering arguments, and it
    // offers no API to reorder sections. Because a subcommand's own args
    // (positionals, command-specific flags, and the flattened Version/Selection
    // structs) register before these propagated globals, the Version section
    // always renders at its flatten site. To keep the relative order of these
    // globals stable and sensible, Selection is declared before Output here, and
    // every Output flag is kept contiguous.
    /// Comma-separated list of columns to include.
    #[arg(
        long,
        global = true,
        value_delimiter = ',',
        help_heading = "Selection options"
    )]
    pub columns: Option<Vec<String>>,

    /// Comma-separated list of columns to exclude. Takes precedence over --columns.
    #[arg(
        long = "exclude-columns",
        global = true,
        value_delimiter = ',',
        help_heading = "Selection options"
    )]
    pub exclude_columns: Option<Vec<String>>,

    /// Output format for row-producing commands. When unset, metadata commands
    /// (versions/branches/tags/indices/fragments) default to `table` (fully
    /// buffered to enable column alignment); everything else to `jsonl`
    /// (streaming).
    #[arg(long, global = true, value_enum, help_heading = "Output options")]
    pub format: Option<Format>,

    /// How to render binary columns in the output.
    #[arg(long = "binary-format", global = true, value_enum, default_value_t = BinaryFormat::None, help_heading = "Output options")]
    pub binary_format: BinaryFormat,

    /// Truncate list / large-list / fixed-size-list rendering to the first N
    /// elements, appending an explicit marker element `… (K more)`. Applies to
    /// jsonl/json output and to nested table cells, at every nesting level.
    /// Lossy: truncated output is for viewing, not round-tripping. Default:
    /// unlimited (current output preserved byte-for-byte).
    #[arg(
        long = "max-list-items",
        global = true,
        value_name = "N",
        help_heading = "Output options"
    )]
    pub max_list_items: Option<usize>,

    /// Table format only: truncate each rendered *data* cell to at most N
    /// characters, ending with `…` when shortened. Header cells (column names)
    /// are never truncated. Counts characters and never splits a multi-byte
    /// UTF-8 codepoint (CJK display width is out of scope). `N = 0` renders every
    /// non-empty cell as a bare `…`. Lossy. Default: unlimited.
    #[arg(
        long = "max-cell-width",
        global = true,
        value_name = "N",
        help_heading = "Output options"
    )]
    pub max_cell_width: Option<usize>,

    /// Render f16 / f32 / f64 values with exactly N fractional digits in every
    /// format (`NaN`/`Infinity` are unaffected). Uses `format!`-style
    /// round-half-to-even. Lossy. Default: full round-trip precision.
    #[arg(
        long = "float-precision",
        global = true,
        value_name = "N",
        help_heading = "Output options"
    )]
    pub float_precision: Option<usize>,

    /// Disable the scan progress indicator on stderr. Progress is drawn only for
    /// long scans and only when stderr is a TTY, so it is already absent when
    /// output is piped; this flag suppresses it unconditionally.
    #[arg(long = "no-progress", global = true, help_heading = "Output options")]
    pub no_progress: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Concatenate one or more datasets and print every row.
    Cat {
        /// Dataset paths or object-store URIs (at least one).
        #[arg(required = true)]
        inputs: Vec<String>,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        row_ids: RowIdArgs,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print the first N rows.
    Head {
        input: String,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        row_ids: RowIdArgs,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print the last N rows.
    Tail {
        input: String,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        row_ids: RowIdArgs,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print rows at the given indices (comma-separated; supports `a:b`, `a:`, `:b`, negatives).
    Take {
        input: String,
        #[arg(long, allow_hyphen_values = true)]
        indices: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        row_ids: RowIdArgs,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Extract one cell's binary/blob payload to a file or stdout.
    ///
    /// Works on plain `Binary`/`LargeBinary`/`FixedSizeBinary`/`BinaryView`
    /// columns and on Lance blob-encoded columns (the latter streamed with
    /// bounded memory). Writes the raw bytes to `-o <file>` (overwriting any
    /// existing file), or to stdout when `-o` is omitted (refused when stdout is
    /// a terminal — redirect or use `-o`). Output is raw bytes, not rows, so the
    /// global `--format` flag is rejected; `--columns`/`--binary-format` do not
    /// apply and are ignored.
    Blob {
        input: String,
        /// Binary or Lance blob-encoded column to extract from.
        #[arg(long)]
        column: String,
        /// Row index to extract (single value; negatives count from the end,
        /// like `take`, so `-1` is the last row).
        #[arg(long, allow_hyphen_values = true)]
        index: i64,
        /// Write the payload to this file instead of stdout, overwriting any
        /// existing file. On success the file is written atomically; a failed
        /// extraction leaves no partial file.
        #[arg(short = 'o', long, value_name = "FILE")]
        output: Option<PathBuf>,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print the number of rows.
    Rowcount {
        input: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Randomly sample N rows without replacement.
    Sample {
        input: String,
        #[arg(short = 'n', long)]
        limit: u64,
        /// Optional u64 seed for reproducibility.
        #[arg(long)]
        seed: Option<u64>,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        row_ids: RowIdArgs,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Per-column summary statistics, one row per column (like `df.describe()`).
    ///
    /// Scans the data to report count (non-null), nulls, min/max (numeric,
    /// temporal, string, boolean), mean/stddev (numeric only), and an approximate
    /// distinct count (exact up to a cap, then reported as `>N`). Nested/binary/
    /// decimal columns report count and nulls only. For a whole-dataset health
    /// summary (rows, fragments, size, versions) instead, see `stat`.
    Stats {
        input: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Count occurrences of each distinct value in a column (value counts).
    Freq {
        input: String,
        /// Column to compute value counts for.
        #[arg(long)]
        column: String,
        /// Show only the top N rows; the remaining values are summarized as an
        /// `<other>` row. Without this, every distinct value is listed. Must be
        /// at least 1.
        #[arg(short = 'n', long, value_parser = clap::value_parser!(u64).range(1..))]
        limit: Option<u64>,
        /// Row ordering: `count` (most frequent first, default) or `value`.
        #[arg(long, value_enum, default_value_t = FreqSort::Count)]
        sort: FreqSort,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// One-screen dataset health summary — the `stat(1)` of datasets.
    ///
    /// Metadata-only (no data scan): rows, deleted rows + ratio, columns,
    /// fragment row-count spread, on-disk size, and version/branch/tag/index
    /// counts, with a conservative compaction hint. For per-column statistics
    /// (the `df.describe()`-style breakdown) use `stats` instead.
    Stat {
        input: String,
        /// Skip on-disk size computation, avoiding object-store lookups on very
        /// remote or huge datasets. The `data size` field is left empty/null.
        #[arg(long = "no-size")]
        no_size: bool,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print the schema of the dataset.
    Schema {
        input: String,
        /// Which schema flavor to print.
        #[arg(long = "type", value_enum, default_value_t = SchemaType::Arrow)]
        ty: SchemaType,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print versions of the dataset.
    Versions {
        input: String,
        /// Scope to a specific branch (default: main).
        #[arg(long)]
        branch: Option<String>,
        /// Hide versions that have no tag (default: show all versions).
        #[arg(long = "tagged-only", default_value_t = false)]
        tagged_only: bool,
    },

    /// Print branches available for the dataset.
    Branches { input: String },

    /// Print tags defined on the dataset, across all branches.
    Tags { input: String },

    /// Print indices defined on the dataset.
    Indices {
        input: String,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// List fragments with row, deletion, file, and size info.
    Fragments {
        input: String,
        /// Show each fragment's data file paths in table output (they are
        /// always included in jsonl/csv output).
        #[arg(long)]
        verbose: bool,
        /// Skip on-disk size computation, avoiding object-store lookups on very
        /// remote or huge datasets. The `size` column is left empty.
        #[arg(long = "no-size")]
        no_size: bool,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Diff two datasets, or two versions of one dataset.
    ///
    /// One verb, two modes, chosen by how many datasets you name:
    ///
    /// * DATASET-VS-DATASET (`lance-cli diff A B`): compares two different
    ///   datasets by schema, schema-metadata and row count. Version selectors
    ///   name a version of *one* dataset and are therefore rejected here:
    ///   passing --from/--to/--from-tag/--to-tag/--branch alongside a second
    ///   dataset is an error.
    ///
    /// * VERSION (`lance-cli diff DS --from <ref>`): compares two versions of
    ///   one dataset (row, schema, fragment, index and version-log deltas).
    ///   Selected by giving a single dataset plus at least one of --from or
    ///   --from-tag.
    ///
    /// Exit codes follow diff(1): 0 when the two sides are identical, 1 when
    /// they differ, 2 on error.
    ///
    /// Human-readable summary by default; pass --format jsonl for a single
    /// machine-readable JSON record. In dataset-vs-dataset mode,
    /// --columns/--exclude-columns scope the comparison to the projected
    /// columns.
    Diff {
        /// First dataset. In version mode, the single dataset to diff across
        /// versions.
        input: String,
        /// Second dataset. Its presence selects dataset-vs-dataset mode, in
        /// which the version selectors are rejected.
        other: Option<String>,
        /// (Version mode) Left-hand ("from") version number.
        #[arg(long, conflicts_with = "from_tag")]
        from: Option<u64>,
        /// (Version mode) Left-hand ("from") tag; resolves to its `(branch, version)`.
        #[arg(long = "from-tag", conflicts_with = "from")]
        from_tag: Option<String>,
        /// (Version mode) Right-hand ("to") version number. Defaults to the
        /// latest version of the same branch as the "from" endpoint.
        #[arg(long, conflicts_with = "to_tag")]
        to: Option<u64>,
        /// (Version mode) Right-hand ("to") tag; resolves to its `(branch, version)`.
        #[arg(long = "to-tag", conflicts_with = "to")]
        to_tag: Option<String>,
        /// (Version mode) Scope both endpoints to this branch (default: main).
        /// A tag on a different branch is rejected.
        #[arg(long)]
        branch: Option<String>,
    },

    /// Nearest-neighbor vector search; appends a `_distance` column.
    ///
    /// Uses an ANN index on the column when present, otherwise falls back to
    /// flat (brute-force) KNN and prints a note to stderr. The output honours
    /// the global `--columns`/`--exclude-columns` projection, with `_distance`
    /// always included.
    #[command(group(ArgGroup::new("query_vector").required(true).args(["vector", "vector_file"])))]
    Search {
        input: String,
        /// Vector column to search (a fixed-size-list-of-float column).
        #[arg(long)]
        column: String,
        /// Query vector as an inline JSON array, e.g. '[0.1, 0.2, 0.3]'.
        #[arg(long)]
        vector: Option<String>,
        /// Read the query vector (a JSON array) from a file, or '-' for stdin.
        #[arg(long = "vector-file")]
        vector_file: Option<PathBuf>,
        /// Number of nearest neighbors to return.
        #[arg(short = 'k', default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        k: u64,
        /// IVF partitions to probe (index tuning; no effect without an index).
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        nprobes: Option<u64>,
        /// Refine factor for re-ranking search results (index tuning).
        #[arg(long = "refine-factor")]
        refine_factor: Option<u32>,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Print per-index coverage: indexed vs unindexed row counts.
    IndexStats {
        input: String,
        #[command(flatten)]
        version: VersionArgs,
    },

    /// Generate a shell completion script and print it to stdout.
    ///
    /// Takes no dataset input; writes the script for the given shell and exits.
    /// See the README install section for where to install each shell's script,
    /// e.g. `lance-cli completions fish > ~/.config/fish/completions/lance-cli.fish`.
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, powershell, elvish).
        #[arg(value_enum)]
        shell: Shell,
    },
}

// ── Subcommand grouping in `lance-cli --help` (issue #50) ────────────────────
//
// Follow-up to #47's option grouping. clap 4.6 renders *every* subcommand in a
// single flat "Commands:" block (`HelpTemplate::write_subcommands`); there is no
// native multi-section subcommand grouping — `subcommand_help_heading` only
// renames that one heading, and there is no per-subcommand `help_heading`.
//
// Mechanisms evaluated against real clap 4.6 behavior:
//
//   (a) A custom top-level `help_template`. The template engine exposes a single
//       `{subcommands}` tag that dumps *all* visible subcommands as one block —
//       it cannot inject a heading mid-block — so a template alone cannot split
//       them. It is, however, the vehicle for (c).
//   (b) `display_order` clustering. Lets us order commands (Lance last) but still
//       under one "Commands:" heading, so it fails the "≥2 clearly-headed
//       sections" bar on its own.
//   (c) Hide the real subcommands and hand-render the whole grouped command list.
//       CHOSEN. `hide = true` is display-only: parsing, `lance-cli <cmd> --help`,
//       and `lance-cli help <cmd>` are all unaffected. With every subcommand hidden,
//       clap's automatic "Commands:" block is suppressed (`has_visible_
//       subcommands()` → false) while the flattened #47 option groups still
//       render via `{all-args}`. We splice a pre-rendered, grouped, headed
//       command list into the template ahead of `{all-args}`.
//
// COMPLETIONS SAFETY (the #14 hazard): `hide = true` drops a subcommand from
// `clap_complete` output and from `lance-cli help` discoverability. We avoid that by
// never hiding on the derive: the completion generator builds its own pristine
// `Cli::command()` (see `commands/completions.rs`), and the hiding here is
// applied only to the throwaway command used for parsing/help in
// `command_grouped`. So all commands stay in every shell's completions — the
// `polish.rs` suite asserts a metadata command (`fragments`) is present — and
// `lance-cli help <cmd>` / `lance-cli <cmd> --help` keep working. Trade-off: the grouped
// list is plain text, so command names/headings are not ANSI-styled the way
// clap's own block is (immaterial when piped, which is how it is tested).
//
// Maintainability: the section membership lists below are the single source of
// truth for both rendering and the exhaustiveness test in `tests/help_sections.rs`, which
// fails if any subcommand is unassigned or assigned twice — the sections cannot
// silently rot when a new command is added.

/// Subcommands that read the dataset's rows. Declaration order is the render
/// order under "Commands:".
pub const DATA_COMMANDS: &[&str] = &[
    "cat", "head", "tail", "take", "sample", "search", "rowcount", "stats", "freq", "schema",
    "diff", "blob",
];

/// Subcommands that answer from the manifest alone, without reading row data —
/// the dataset's catalog: its versions, tags, indices and physical layout.
pub const METADATA_COMMANDS: &[&str] = &[
    "stat",
    "versions",
    "branches",
    "tags",
    "indices",
    "index-stats",
    "fragments",
];

/// Setup / tooling subcommands, including clap's auto-generated `help`.
pub const SETUP_COMMANDS: &[&str] = &["completions", "help"];

/// The `--help` command sections in render order: `(heading, member names)`.
pub const COMMAND_SECTIONS: &[(&str, &[&str])] = &[
    ("Commands", DATA_COMMANDS),
    ("Metadata commands", METADATA_COMMANDS),
    ("Setup", SETUP_COMMANDS),
];

impl Cli {
    /// Parse `std::env::args`, rendering `--help` with grouped subcommands.
    ///
    /// Drop-in replacement for `clap::Parser::parse` (used by `main`); the only
    /// difference is the grouped top-level help built by [`Cli::command_grouped`].
    pub fn parse_grouped() -> Self {
        let mut matches = Self::command_grouped().get_matches();
        // `from_arg_matches` only fails on an internal derive/matches mismatch,
        // which cannot happen here; mirror clap's own `parse()` by exiting.
        Self::from_arg_matches_mut(&mut matches).unwrap_or_else(|err| err.exit())
    }

    /// The top-level [`clap::Command`] with its subcommands grouped into the
    /// `--help` sections defined by [`COMMAND_SECTIONS`]. See the module comment
    /// above for the mechanism and the rejected alternatives.
    pub fn command_grouped() -> clap::Command {
        let mut cmd = Self::command();
        // Materialize clap's auto `help` subcommand (and the help/version flags)
        // now, so we can read `help`'s one-liner for the Setup section and hide
        // it alongside the real subcommands below. `build()` is idempotent
        // (guarded by an internal "Built" flag), so the rebuild inside
        // `get_matches` is a no-op and the hidden state set here survives.
        cmd.build();
        let sections = render_command_sections(&cmd);
        // Hide every subcommand so clap's single automatic "Commands:" block is
        // suppressed; our grouped `sections` (spliced into the template) become
        // the only command list. Hiding is display-only — see the module comment.
        //
        // With no *visible* subcommands, clap also drops the `<COMMAND>` token
        // from the auto-generated usage line, so we restore it via
        // `override_usage` (a command is still required — hiding does not change
        // parsing).
        cmd.mut_subcommands(|sc| sc.hide(true))
            .override_usage("lance-cli [OPTIONS] <COMMAND>")
            .help_template(grouped_help_template(&sections))
    }
}

/// clap's default top-level help template with the pre-rendered command
/// `sections` spliced in ahead of `{all-args}`. With every subcommand hidden,
/// `{all-args}` renders only the (still #47-grouped) global options. Assembled
/// by concatenation rather than `format!` so a brace in any one-liner can never
/// be read as a format specifier; clap echoes an unknown `{token}` verbatim.
fn grouped_help_template(sections: &str) -> String {
    let mut template =
        String::from("{before-help}{about-with-newline}\n{usage-heading} {usage}\n\n");
    template.push_str(sections);
    template.push_str("\n\n{all-args}{after-help}");
    template
}

/// Render the grouped, headed command list for the top-level `--help`, pulling
/// each one-liner straight from the built command's subcommand `about` text (so
/// they never drift from the derive). All sections share one column width so the
/// names line up as a single table, matching clap's own alignment.
fn render_command_sections(cmd: &clap::Command) -> String {
    let longest = COMMAND_SECTIONS
        .iter()
        .flat_map(|(_, names)| names.iter())
        .map(|name| name.len())
        .max()
        .unwrap_or(0);
    let mut out = String::new();
    for (section_idx, (heading, names)) in COMMAND_SECTIONS.iter().enumerate() {
        if section_idx > 0 {
            out.push_str("\n\n");
        }
        out.push_str(heading);
        out.push(':');
        for name in names.iter() {
            let about = cmd
                .get_subcommands()
                .find(|sub| sub.get_name() == *name)
                .and_then(|sub| sub.get_about())
                .map(|about| about.to_string())
                .unwrap_or_default();
            // clap indents subcommands two spaces and pads to the longest name.
            out.push_str(&format!("\n  {name:<longest$}  {about}"));
        }
    }
    out
}
