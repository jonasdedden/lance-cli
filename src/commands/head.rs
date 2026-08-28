use futures::StreamExt;

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{make_stdout_writer, prepare_row_id_columns, project_arrow_schema};
use crate::commands::progress::ScanProgress;
use crate::dataset::{self, ScanOptions};
use crate::output::RenderOptions;
use crate::projection;
use crate::row_id::{self, RowIds};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    limit: u64,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    row_ids: RowIds,
    version: &VersionArgs,
    show_progress: bool,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    let arrow_schema = ds.arrow_schema();
    let columns = prepare_row_id_columns(columns, exclude, row_ids)?;
    let projection = projection::resolve(&arrow_schema, columns.as_deref(), exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());
    let projected_schema = row_id::extend_schema(&projected_schema, row_ids);

    // Progress: an unfiltered `head` stops after `limit` rows, so it barely
    // scans and needs no indicator. A filtered `head` may scan far to find
    // sparse matches, so show a rows-scanned spinner (the surviving-row total is
    // unknown up front).
    let progress = ScanProgress::new(show_progress && filter.is_some(), None);

    // Open the scan before emitting the header: the scan validates the
    // predicate eagerly, so an invalid `--where` must not leave a stray header
    // on stdout.
    let mut stream = if limit > 0 {
        let options = ScanOptions {
            projection: projection.as_deref(),
            filter,
            row_ids,
        };
        Some(progress.wrap(ds.scan(&options).await?))
    } else {
        None
    };

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;

    if let Some(stream) = stream.as_mut() {
        let mut remaining = limit;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let rows = batch.num_rows() as u64;
            if rows <= remaining {
                writer.write_batch(&batch)?;
                remaining -= rows;
            } else {
                let slice = batch.slice(0, remaining as usize);
                writer.write_batch(&slice)?;
                remaining = 0;
            }
            if remaining == 0 {
                break;
            }
        }
    }
    writer.finish()?;
    progress.finish();
    Ok(())
}
