use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::make_stdout_writer;
use crate::commands::progress::ScanProgress;
use crate::dataset;
use crate::output::RenderOptions;
use crate::projection;
use crate::stats;

/// `lance-cli stats`: per-column summary statistics (a `df.describe()` for datasets).
///
/// Streams the dataset once, folding `arrow` aggregate state per column, and
/// prints one row per column through the shared metadata-table writer, so
/// `--format table|jsonl|csv` all work. Respects `--columns`/`--exclude-columns`
/// and `--where`.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    version: &VersionArgs,
    show_progress: bool,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    let schema = ds.arrow_schema();
    let projection = projection::resolve(&schema, columns, exclude)?;

    // Progress: `stats` always scans the whole (optionally filtered) dataset.
    // With no filter the row total is a cheap metadata `count_rows`, so show a
    // bar with an ETA; with a filter the surviving-row total is unknown, so use
    // a rows-scanned spinner.
    let total = if show_progress && filter.is_none() {
        Some(ds.count_rows(None).await?)
    } else {
        None
    };
    let progress = ScanProgress::new(show_progress, total);
    let column_stats =
        stats::compute(ds.as_ref(), &progress, projection.as_deref(), filter).await?;
    progress.finish();
    let batch = stats::to_record_batch(&column_stats)?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&stats::output_schema())?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
