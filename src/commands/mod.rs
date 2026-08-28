mod blob;
mod branches;
mod cat;
mod common;
mod completions;
mod diff;
mod diff_common;
mod fragments;
mod freq;
mod head;
mod index_stats;
mod indices;
pub mod progress;
mod rowcount;
mod sample;
mod schema;
mod search;
mod stat;
mod stats;
mod tags;
mod tail;
mod take;
mod version_diff;
mod versions;

use std::io::IsTerminal;

use crate::Result;
use crate::cli::{BinaryFormat, Cli, Command, Format};
use crate::error::Error;
use crate::output::RenderOptions;

/// What a successfully-run command signals to the process exit code.
///
/// Every command yields `Success` except `diff`, which yields `Different` when
/// the two versions it compared are not identical. `main` maps `Success` → 0,
/// `Different` → 1, and any `Err` → 2 (see `diff(1)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Different,
}

pub async fn dispatch(cli: Cli) -> Result<Outcome> {
    // Reject `--format` on commands that don't emit row-shaped output — including
    // `completions` — before anything else runs. This must precede the
    // `completions` interception below, otherwise `lance-cli completions bash
    // --format csv` would silently ignore `--format` instead of erroring.
    if let Some(name) = command_ignoring_format(&cli.command)
        && cli.format.is_some()
    {
        return Err(Error::FormatNotApplicable { command: name });
    }

    // `completions` takes no dataset input and bypasses the format/output
    // machinery: intercept it before any of that runs and exit 0.
    if let Command::Completions { shell } = cli.command {
        completions::run(shell);
        return Ok(Outcome::Success);
    }

    let columns = cli.columns.as_deref();
    let exclude = cli.exclude_columns.as_deref();
    let render = RenderOptions {
        binary_format: cli.binary_format,
        max_list_items: cli.max_list_items,
        max_cell_width: cli.max_cell_width,
        float_precision: cli.float_precision,
    };
    // The scan progress indicator is opt-out and TTY-gated: never drawn when
    // `--no-progress` is set or when stderr is redirected/piped. Folding both
    // into one flag here keeps every command's own logic to "bar vs spinner".
    let show_progress = !cli.no_progress && std::io::stderr().is_terminal();
    let explicit_format = cli.format;
    // `diff` owns its own format semantics (human summary vs `--format jsonl`)
    // and its own exit-code outcome, so it is branched out before the
    // row-format machinery the other commands share.
    //
    // A single `diff` verb spans two modes, chosen by the number of positional
    // datasets:
    //   * two datasets (`diff A B`)         -> dataset-vs-dataset diff;
    //   * one dataset + `--from`/`--from-tag`-> version diff.
    // Conflicting combinations (a second dataset alongside version selectors, or
    // one dataset with no `--from`) are rejected here rather than by clap, which
    // cannot express "required only when the second positional is absent".
    if let Command::Diff {
        input,
        other,
        from,
        from_tag,
        to,
        to_tag,
        branch,
    } = cli.command
    {
        if let Some(other) = other {
            // Dataset-vs-dataset mode: the version selectors are ambiguous
            // across two different datasets, so any of them is a hard error.
            if from.is_some()
                || from_tag.is_some()
                || to.is_some()
                || to_tag.is_some()
                || branch.is_some()
            {
                return Err(Error::DiffSelectorsInTwoDatasetMode);
            }
            return diff::run(&input, &other, columns, exclude, explicit_format).await;
        }
        // Version mode: a left-hand selector is mandatory (the old clap
        // `from_ref` group, now enforced here so two-dataset mode can omit it).
        if from.is_none() && from_tag.is_none() {
            return Err(Error::DiffMissingFromRef);
        }
        let selectors = version_diff::DiffSelectors {
            branch,
            from_version: from,
            from_tag,
            to_version: to,
            to_tag,
        };
        return version_diff::run(&input, selectors, explicit_format).await;
    }
    let format = resolve_format(explicit_format, &cli.command);
    // IPC is a binary, lossless stream: it is only valid on the row-producing
    // commands, the value-rendering flags do not apply to it, and it must never
    // be dumped into a terminal. Validate all three before any command runs so
    // nothing is written to stdout on the rejection paths. `format` is only ever
    // `Ipc` when it was passed explicitly (it is never a per-command default).
    if format == Format::Ipc {
        validate_ipc(&cli.command, &render)?;
    }
    run_command(cli.command, format, render, columns, exclude, show_progress).await?;
    Ok(Outcome::Success)
}

async fn run_command(
    command: Command,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    show_progress: bool,
) -> Result<()> {
    match command {
        Command::Cat {
            inputs,
            filter,
            row_ids,
            version,
        } => {
            cat::run(
                &inputs,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                row_ids.flags(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Head {
            input,
            limit,
            filter,
            row_ids,
            version,
        } => {
            head::run(
                &input,
                limit,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                row_ids.flags(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Tail {
            input,
            limit,
            filter,
            row_ids,
            version,
        } => {
            tail::run(
                &input,
                limit,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                row_ids.flags(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Take {
            input,
            indices,
            filter,
            row_ids,
            version,
        } => {
            take::run(
                &input,
                &indices,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                row_ids.flags(),
                &version,
            )
            .await
        }
        Command::Blob {
            input,
            column,
            index,
            output,
            version,
        } => {
            // `blob` emits raw bytes, not row-shaped output. `--format` is a hard
            // error via `command_ignoring_format` above (the rowcount/schema
            // precedent for non-row-shaped commands); `--columns`/`--binary-format`
            // don't apply either but are silently ignored, matching how metadata
            // commands treat inapplicable projection/rendering flags.
            blob::run(&input, &column, index, output.as_deref(), &version).await
        }
        Command::Rowcount {
            input,
            filter,
            version,
        } => rowcount::run(&input, filter.predicate.as_deref(), &version).await,
        Command::Sample {
            input,
            limit,
            seed,
            filter,
            row_ids,
            version,
        } => {
            sample::run(
                &input,
                limit,
                seed,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                row_ids.flags(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Freq {
            input,
            column,
            limit,
            sort,
            filter,
            version,
        } => {
            freq::run(
                &input,
                &column,
                limit,
                sort,
                format,
                render,
                filter.predicate.as_deref(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Schema { input, ty, version } => {
            schema::run(&input, ty, columns, exclude, &version).await
        }
        Command::Stats {
            input,
            filter,
            version,
        } => {
            stats::run(
                &input,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &version,
                show_progress,
            )
            .await
        }
        Command::Versions {
            input,
            branch,
            tagged_only,
        } => versions::run(&input, branch.as_deref(), tagged_only, format, render).await,
        Command::Branches { input } => branches::run(&input, format, render).await,
        Command::Tags { input } => tags::run(&input, format, render).await,
        Command::Indices {
            input,
            version: version_args,
        } => indices::run(&input, &version_args, format, render).await,
        Command::Fragments {
            input,
            verbose,
            no_size,
            version: version_args,
        } => fragments::run(&input, &version_args, verbose, no_size, format, render).await,
        Command::Search {
            input,
            column,
            vector,
            vector_file,
            k,
            nprobes,
            refine_factor,
            version,
        } => {
            // clap's `query_vector` group guarantees exactly one of these is set.
            let source = match (vector.as_deref(), vector_file.as_deref()) {
                (Some(inline), _) => search::QuerySource::Inline(inline),
                (None, Some(path)) => search::QuerySource::File(path),
                (None, None) => unreachable!("clap requires one of --vector/--vector-file"),
            };
            search::run(
                &input,
                &column,
                source,
                k as usize,
                nprobes.map(|n| n as usize),
                refine_factor,
                format,
                render,
                columns,
                exclude,
                &version,
            )
            .await
        }
        Command::IndexStats {
            input,
            version: version_args,
        } => index_stats::run(&input, &version_args, format, render).await,
        Command::Stat {
            input,
            no_size,
            version: version_args,
        } => stat::run(&input, &version_args, no_size, format, render).await,
        // `diff` is intercepted in `dispatch` (distinct format + exit-code
        // handling) and never reaches this shared row-format path.
        Command::Diff { .. } => unreachable!("diff is dispatched separately"),
        // `completions` is intercepted at the top of `dispatch` (no dataset
        // input, no format machinery) and never reaches here.
        Command::Completions { .. } => unreachable!("completions is dispatched separately"),
    }
}

/// Apply the per-command default when `--format` was not given on the CLI.
/// Summary/metadata commands (freq/versions/branches/tags/indices/index-stats/
/// fragments/stats/stat) default to
/// `Table`; everything else defaults to `Jsonl`.
fn resolve_format(explicit: Option<Format>, cmd: &Command) -> Format {
    if let Some(f) = explicit {
        return f;
    }
    match cmd {
        Command::Freq { .. }
        | Command::Versions { .. }
        | Command::Branches { .. }
        | Command::Tags { .. }
        | Command::Indices { .. }
        | Command::IndexStats { .. }
        | Command::Fragments { .. }
        | Command::Stats { .. }
        | Command::Stat { .. } => Format::Table,
        _ => Format::Jsonl,
    }
}

/// Returns the command name for those that don't emit row-shaped output, so
/// `--format` against them is a hard error rather than a silent no-op.
fn command_ignoring_format(cmd: &Command) -> Option<&'static str> {
    match cmd {
        Command::Rowcount { .. } => Some("rowcount"),
        Command::Schema { .. } => Some("schema"),
        Command::Blob { .. } => Some("blob"),
        // `completions` prints a shell script, not row-shaped output.
        Command::Completions { .. } => Some("completions"),
        _ => None,
    }
}

/// Gate `--format ipc` before any output is emitted:
///
/// - reject IPC on commands that aren't one of the row-producing five
///   (metadata/summary commands may add it as a follow-up, per the issue);
/// - reject the value-rendering flags, which are meaningless for a lossless
///   binary stream (we reject rather than silently ignore, matching the repo's
///   hard-error stance on inapplicable rendering knobs — documented in README);
/// - refuse to write the binary stream to a terminal.
fn validate_ipc(cmd: &Command, render: &RenderOptions) -> Result<()> {
    if let Some(command) = command_rejecting_ipc(cmd) {
        return Err(Error::IpcNotApplicable { command });
    }
    if render.binary_format != BinaryFormat::None {
        return Err(Error::IpcRenderingFlag {
            flag: "--binary-format",
        });
    }
    if render.max_list_items.is_some() {
        return Err(Error::IpcRenderingFlag {
            flag: "--max-list-items",
        });
    }
    if render.max_cell_width.is_some() {
        return Err(Error::IpcRenderingFlag {
            flag: "--max-cell-width",
        });
    }
    if render.float_precision.is_some() {
        return Err(Error::IpcRenderingFlag {
            flag: "--float-precision",
        });
    }
    crate::output::ipc::guard_not_terminal(std::io::stdout().is_terminal())
}

/// For `--format ipc`: `None` for the row-producing commands that can emit an
/// Arrow IPC stream (cat/head/tail/take/sample), `Some(name)` for every command
/// that rejects it. Commands that ignore `--format` entirely (rowcount/schema)
/// are caught earlier by `command_ignoring_format`, and `diff` is dispatched
/// separately and rejects IPC itself, so those arms are defensive only.
fn command_rejecting_ipc(cmd: &Command) -> Option<&'static str> {
    match cmd {
        Command::Cat { .. }
        | Command::Head { .. }
        | Command::Tail { .. }
        | Command::Take { .. }
        | Command::Sample { .. } => None,
        Command::Stats { .. } => Some("stats"),
        Command::Freq { .. } => Some("freq"),
        Command::Stat { .. } => Some("stat"),
        Command::Versions { .. } => Some("versions"),
        Command::Branches { .. } => Some("branches"),
        Command::Tags { .. } => Some("tags"),
        Command::Indices { .. } => Some("indices"),
        Command::IndexStats { .. } => Some("index-stats"),
        Command::Fragments { .. } => Some("fragments"),
        Command::Search { .. } => Some("search"),
        Command::Rowcount { .. } => Some("rowcount"),
        Command::Schema { .. } => Some("schema"),
        Command::Diff { .. } => Some("diff"),
        Command::Blob { .. } => Some("blob"),
        // Defensive: `completions` already hard-errors on any `--format` via
        // `command_ignoring_format`; the arm only satisfies exhaustiveness.
        Command::Completions { .. } => Some("completions"),
    }
}
