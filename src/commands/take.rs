use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{make_stdout_writer, prepare_row_id_columns, project_arrow_schema};
use crate::dataset;
use crate::error::Error;
use crate::indices;
use crate::output::RenderOptions;
use crate::projection;
use crate::row_id::{self, RowIds};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    indices_raw: &str,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    row_ids: RowIds,
    version: &VersionArgs,
) -> Result<()> {
    // Indices are positional; a content predicate would make the mapping
    // ambiguous, so reject the combination rather than pick a silent winner.
    if filter.is_some() {
        return Err(Error::TakeWhereConflict);
    }

    let ds = dataset::open(input, Some(version)).await?;
    let arrow_schema = ds.arrow_schema();
    let columns = prepare_row_id_columns(columns, exclude, row_ids)?;
    let projection = projection::resolve(&arrow_schema, columns.as_deref(), exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());
    let projected_schema = row_id::extend_schema(&projected_schema, row_ids);

    let rowcount = ds.count_rows(None).await?;
    let indices = indices::resolve(indices_raw, rowcount)?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;

    if !indices.is_empty() {
        let batch = ds.take(&indices, projection.as_deref(), row_ids).await?;
        writer.write_batch(&batch)?;
    }
    writer.finish()?;
    Ok(())
}
