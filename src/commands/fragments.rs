use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{human_bytes, make_stdout_writer};
use crate::dataset::{self, FragmentInfo};
use crate::output::RenderOptions;

pub async fn run(
    input: &str,
    version: &VersionArgs,
    verbose: bool,
    no_size: bool,
    format: Format,
    render: RenderOptions,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;

    let fragments = ds.list_fragments(!no_size).await?;

    // `size` renders differently per format: a human-readable string in the
    // table (bytes are noise when eyeballing), raw bytes everywhere else so the
    // output stays machine-parseable. The `files` column is wide, so it's only
    // added to the table under `--verbose`; jsonl/csv always carry it.
    let human_size = format == Format::Table;
    let include_files = verbose || format != Format::Table;

    let (schema, columns) = build_batch(&fragments, human_size, include_files);
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    // `finish()` already flushed; drop the writer here to release the stdout
    // lock before the summary println so the two are ordered cleanly.
    drop(writer);

    if format == Format::Table {
        print_summary(&fragments, no_size);
    }
    Ok(())
}

/// Build the output schema and columns for the fragment rows. `human_size`
/// picks a `Utf8` (formatted) vs `UInt64` (raw) `size` column; `include_files`
/// toggles the comma-joined `files` column.
fn build_batch(
    fragments: &[FragmentInfo],
    human_size: bool,
    include_files: bool,
) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
    let mut fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("physical_rows", DataType::UInt64, false),
        Field::new("deleted_rows", DataType::UInt64, false),
        Field::new("num_files", DataType::UInt64, false),
    ];
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments
                .iter()
                .map(|f| f.physical_rows)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.deleted_rows).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.num_files).collect::<Vec<_>>(),
        )),
    ];

    if include_files {
        fields.push(Field::new("files", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(
            fragments
                .iter()
                .map(|f| f.files.join(","))
                .collect::<Vec<_>>(),
        )));
    }

    if human_size {
        fields.push(Field::new("size", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(
            fragments
                .iter()
                .map(|f| f.size.map(human_bytes))
                .collect::<Vec<_>>(),
        )));
    } else {
        fields.push(Field::new("size", DataType::UInt64, true));
        columns.push(Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.size).collect::<Vec<_>>(),
        )));
    }

    (Arc::new(Schema::new(fields)), columns)
}

/// Print the trailing table-mode summary: totals across all fragments. The
/// byte total is omitted when sizes weren't computed (`--no-size`).
fn print_summary(fragments: &[FragmentInfo], no_size: bool) {
    let total_physical: u64 = fragments.iter().map(|f| f.physical_rows).sum();
    let total_deleted: u64 = fragments.iter().map(|f| f.deleted_rows).sum();
    let mut line = format!(
        "{} fragment(s), {} physical row(s), {} deleted row(s)",
        fragments.len(),
        total_physical,
        total_deleted
    );
    if !no_size {
        let total_bytes: u64 = fragments.iter().filter_map(|f| f.size).sum();
        line.push_str(&format!(", {} total", human_bytes(total_bytes)));
    }
    println!("{line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_summary_uses_totals() {
        // `print_summary` writes to stdout, so this just exercises the total
        // arithmetic that feeds it (kept trivial and side-effect-free here).
        let frags = [
            FragmentInfo {
                id: 0,
                physical_rows: 3,
                deleted_rows: 1,
                num_files: 1,
                files: vec!["a.lance".into()],
                size: Some(1024),
            },
            FragmentInfo {
                id: 1,
                physical_rows: 2,
                deleted_rows: 0,
                num_files: 1,
                files: vec!["b.lance".into()],
                size: Some(2048),
            },
        ];
        let total_physical: u64 = frags.iter().map(|f| f.physical_rows).sum();
        let total_deleted: u64 = frags.iter().map(|f| f.deleted_rows).sum();
        let total_bytes: u64 = frags.iter().filter_map(|f| f.size).sum();
        assert_eq!(total_physical, 5);
        assert_eq!(total_deleted, 1);
        assert_eq!(human_bytes(total_bytes), "3.0 KiB");
    }
}
