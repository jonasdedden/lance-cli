use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use crate::Result;
use crate::cli::Format;
use crate::commands::common::make_stdout_writer;
use crate::dataset::{self, IndexStats};
use crate::output::RenderOptions;

pub async fn run(
    input: &str,
    version: &crate::cli::VersionArgs,
    format: Format,
    render: RenderOptions,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;

    let stats = ds.index_stats().await?;

    // The `detail` column carries the raw Lance statistics JSON so callers can
    // pass through type-specific internals (IVF partitions, PQ sub-vectors, …).
    // It's verbose, so only jsonl includes it; table/csv stay concise.
    let include_detail = matches!(format, Format::Jsonl);

    let mut fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("indexed_rows", DataType::UInt64, false),
        Field::new("unindexed_rows", DataType::UInt64, false),
        Field::new("coverage", DataType::Utf8, false),
    ];
    if include_detail {
        fields.push(Field::new("detail", DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let name_col: Arc<dyn Array> = Arc::new(StringArray::from(
        stats.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
    ));
    let type_col: Arc<dyn Array> = Arc::new(StringArray::from(
        stats
            .iter()
            .map(|s| s.index_type.as_str())
            .collect::<Vec<_>>(),
    ));
    let indexed_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        stats.iter().map(|s| s.indexed_rows).collect::<Vec<_>>(),
    ));
    let unindexed_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        stats.iter().map(|s| s.unindexed_rows).collect::<Vec<_>>(),
    ));
    let coverage_col: Arc<dyn Array> = Arc::new(StringArray::from(
        stats.iter().map(coverage_display).collect::<Vec<_>>(),
    ));

    let mut columns: Vec<Arc<dyn Array>> =
        vec![name_col, type_col, indexed_col, unindexed_col, coverage_col];
    if include_detail {
        let detail_col: Arc<dyn Array> = Arc::new(StringArray::from(
            stats.iter().map(|s| s.detail.as_str()).collect::<Vec<_>>(),
        ));
        columns.push(detail_col);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}

/// Render coverage as a one-decimal percentage (`98.0%`), or `n/a` for an index
/// with no rows at all (where coverage is undefined).
fn coverage_display(stats: &IndexStats) -> String {
    match stats.coverage() {
        Some(fraction) => format!("{:.1}%", fraction * 100.0),
        None => "n/a".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(indexed: u64, unindexed: u64) -> IndexStats {
        IndexStats {
            name: "idx".to_string(),
            index_type: "BTree".to_string(),
            indexed_rows: indexed,
            unindexed_rows: unindexed,
            detail: String::new(),
        }
    }

    #[test]
    fn coverage_display_rounds_to_one_decimal() {
        assert_eq!(coverage_display(&stats(980_000, 20_000)), "98.0%");
        assert_eq!(coverage_display(&stats(100, 0)), "100.0%");
        // 4/6 = 0.6666… rounds to 66.7%.
        assert_eq!(coverage_display(&stats(4, 2)), "66.7%");
        // 2/3 of a thousand → 66.7% as well (half-up on the hidden digit).
        assert_eq!(coverage_display(&stats(1, 2)), "33.3%");
    }

    #[test]
    fn coverage_display_is_na_for_empty_index() {
        assert_eq!(coverage_display(&stats(0, 0)), "n/a");
    }
}
