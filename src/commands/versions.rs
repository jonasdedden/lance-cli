use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::Result;
use crate::cli::Format;
use crate::commands::common::make_stdout_writer;
use crate::dataset;
use crate::output::RenderOptions;

pub async fn run(
    input: &str,
    branch: Option<&str>,
    tagged_only: bool,
    format: Format,
    render: RenderOptions,
) -> Result<()> {
    let ds = dataset::open(input, None).await?;

    let versions = ds.list_versions(branch, tagged_only).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("version", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("tag", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let version_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        versions.iter().map(|v| v.version).collect::<Vec<_>>(),
    ));
    let timestamp_col: Arc<dyn Array> = Arc::new(
        TimestampMicrosecondArray::from(
            versions
                .iter()
                .map(|v| v.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let tag_col: Arc<dyn Array> = Arc::new(StringArray::from(
        versions
            .iter()
            .map(|v| v.tag.as_deref())
            .collect::<Vec<_>>(),
    ));
    let message_col: Arc<dyn Array> = Arc::new(StringArray::from(
        versions
            .iter()
            .map(|v| v.message.as_deref())
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![version_col, timestamp_col, tag_col, message_col],
    )?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
