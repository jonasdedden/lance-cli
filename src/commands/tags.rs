use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use crate::Result;
use crate::cli::Format;
use crate::commands::common::make_stdout_writer;
use crate::dataset;
use crate::output::RenderOptions;

pub async fn run(input: &str, format: Format, render: RenderOptions) -> Result<()> {
    let ds = dataset::open(input, None).await?;

    let tags = ds.list_tags().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("branch", DataType::Utf8, false),
        Field::new("version", DataType::UInt64, false),
    ]));
    let name_col: Arc<dyn Array> = Arc::new(StringArray::from(
        tags.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
    ));
    let branch_col: Arc<dyn Array> = Arc::new(StringArray::from(
        tags.iter().map(|t| t.branch.as_str()).collect::<Vec<_>>(),
    ));
    let version_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        tags.iter().map(|t| t.version).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![name_col, branch_col, version_col])?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
