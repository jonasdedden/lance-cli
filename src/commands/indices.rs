use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::make_stdout_writer;
use crate::dataset;
use crate::output::RenderOptions;

pub async fn run(
    input: &str,
    version: &VersionArgs,
    format: Format,
    render: RenderOptions,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;

    let indices = ds.list_indices().await?;

    let columns_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("uuid", DataType::Utf8, false),
        Field::new("columns", DataType::List(columns_field), false),
        Field::new("dataset_version", DataType::UInt64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));

    let name_col: Arc<dyn Array> = Arc::new(StringArray::from(
        indices.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
    ));
    let type_col: Arc<dyn Array> = Arc::new(StringArray::from(
        indices
            .iter()
            .map(|i| i.index_type.as_str())
            .collect::<Vec<_>>(),
    ));
    let uuid_col: Arc<dyn Array> = Arc::new(StringArray::from(
        indices.iter().map(|i| i.uuid.as_str()).collect::<Vec<_>>(),
    ));
    let mut columns_builder = ListBuilder::new(StringBuilder::new());
    for idx in &indices {
        for c in &idx.columns {
            columns_builder.values().append_value(c);
        }
        columns_builder.append(true);
    }
    let columns_col: Arc<dyn Array> = Arc::new(columns_builder.finish());
    let version_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        indices
            .iter()
            .map(|i| i.dataset_version)
            .collect::<Vec<_>>(),
    ));
    let created_at_col: Arc<dyn Array> = Arc::new(
        TimestampMicrosecondArray::from(
            indices
                .iter()
                .map(|i| i.created_at.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            name_col,
            type_col,
            uuid_col,
            columns_col,
            version_col,
            created_at_col,
        ],
    )?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
