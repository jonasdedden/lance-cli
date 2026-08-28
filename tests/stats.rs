//! Integration tests for the `stats` command engine (`lance_cli::stats`).
//!
//! Each test writes a real Lance dataset, then folds it through
//! `stats::compute` and asserts on known expected statistics. Rendering to the
//! three output formats is exercised via `stats::to_record_batch`.

mod common;

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance::Dataset as LanceInner;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use lance_cli::cli::{BinaryFormat, Format};
use lance_cli::commands::progress::ScanProgress;
use lance_cli::dataset::{self, ColumnStats};
use lance_cli::output::make_writer;
use lance_cli::output::table::TableStyle;
use lance_cli::stats;

use common::{tempdir, write_full};

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Pluck a single column's stats by name (panics if absent).
fn col<'a>(stats: &'a [ColumnStats], name: &str) -> &'a ColumnStats {
    stats
        .iter()
        .find(|s| s.column == name)
        .unwrap_or_else(|| panic!("column {name} not found in stats"))
}

/// Compute stats for the dataset at `path` (all columns, no filter).
async fn compute_all(path: &str) -> Vec<ColumnStats> {
    let ds = dataset::open(path, None).await.unwrap();
    stats::compute(ds.as_ref(), &ScanProgress::disabled(), None, None)
        .await
        .unwrap()
}

fn mixed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn mixed_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec![
        Some("alice"),
        Some("bob"),
        None,
        Some("dan"),
        Some("eve"),
    ]);
    let scores = Float64Array::from(vec![Some(10.0), Some(20.0), Some(30.0), None, Some(40.0)]);
    RecordBatch::try_new(
        mixed_schema(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )
    .unwrap()
}

/// Write `batches` to a fresh Lance dataset — first batch is the initial write,
/// the rest are appends (each becomes a separate fragment / scan batch).
async fn write_fragments(tmp: &TempDir, name: &str, batches: Vec<RecordBatch>) -> String {
    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();
    let schema = batches[0].schema();
    let mut it = batches.into_iter();
    let first = it.next().unwrap();
    let iter = RecordBatchIterator::new(vec![Ok(first)], schema.clone());
    let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    for b in it {
        let iter = RecordBatchIterator::new(vec![Ok(b)], schema.clone());
        ds.append(iter, None).await.unwrap();
    }
    uri
}

#[test]
fn numeric_string_known_statistics() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_fragments(&tmp, "mixed", vec![mixed_batch()]).await;
        let stats = compute_all(&path).await;

        let id = col(&stats, "id");
        assert_eq!(id.data_type, "Int32");
        assert_eq!(id.count, 5);
        assert_eq!(id.nulls, 0);
        assert_eq!(id.min.as_deref(), Some("1"));
        assert_eq!(id.max.as_deref(), Some("5"));
        assert_eq!(id.mean, Some(3.0));
        // sample stddev of 1..5 = sqrt(2.5)
        assert!((id.stddev.unwrap() - 2.5_f64.sqrt()).abs() < 1e-12);
        assert_eq!(id.distinct.as_deref(), Some("5"));

        let name = col(&stats, "name");
        assert_eq!(name.data_type, "Utf8");
        assert_eq!(name.count, 4);
        assert_eq!(name.nulls, 1);
        assert_eq!(name.min.as_deref(), Some("alice"));
        assert_eq!(name.max.as_deref(), Some("eve"));
        assert_eq!(name.mean, None);
        assert_eq!(name.stddev, None);
        assert_eq!(name.distinct.as_deref(), Some("4"));

        let score = col(&stats, "score");
        assert_eq!(score.count, 4);
        assert_eq!(score.nulls, 1);
        assert_eq!(score.min.as_deref(), Some("10"));
        assert_eq!(score.max.as_deref(), Some("40"));
        assert_eq!(score.mean, Some(25.0));
        // sample stddev of [10,20,30,40] = sqrt(500/3)
        assert!((score.stddev.unwrap() - (500.0_f64 / 3.0).sqrt()).abs() < 1e-9);
        assert_eq!(score.distinct.as_deref(), Some("4"));
    });
}

#[test]
fn multi_fragment_folds_across_batches() {
    runtime().block_on(async {
        let tmp = tempdir();
        // Same logical data as `mixed_batch`, split across three appends.
        let b1 = RecordBatch::try_new(
            mixed_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
                Arc::new(Float64Array::from(vec![Some(10.0), Some(20.0)])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            mixed_schema(),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
            ],
        )
        .unwrap();
        let b3 = RecordBatch::try_new(
            mixed_schema(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec![Some("dan"), Some("eve")])),
                Arc::new(Float64Array::from(vec![None, Some(40.0)])),
            ],
        )
        .unwrap();
        let path = write_fragments(&tmp, "frag", vec![b1, b2, b3]).await;
        let stats = compute_all(&path).await;

        let id = col(&stats, "id");
        assert_eq!(id.count, 5);
        assert_eq!(id.min.as_deref(), Some("1"));
        assert_eq!(id.max.as_deref(), Some("5"));
        assert_eq!(id.mean, Some(3.0));
        assert_eq!(id.distinct.as_deref(), Some("5"));

        let score = col(&stats, "score");
        assert_eq!(score.count, 4);
        assert_eq!(score.mean, Some(25.0));
        assert_eq!(score.distinct.as_deref(), Some("4"));
    });
}

#[test]
fn nan_floats_skip_minmax_but_taint_mean() {
    runtime().block_on(async {
        let tmp = tempdir();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
                Some(3.0),
            ]))],
        )
        .unwrap();
        let path = write_fragments(&tmp, "nan", vec![batch]).await;
        let stats = compute_all(&path).await;

        let score = col(&stats, "score");
        // NaN is a non-null value.
        assert_eq!(score.count, 3);
        assert_eq!(score.nulls, 0);
        // min/max ignore NaN → the real numeric range.
        assert_eq!(score.min.as_deref(), Some("1"));
        assert_eq!(score.max.as_deref(), Some("3"));
        // mean/stddev are tainted to NaN.
        assert!(score.mean.unwrap().is_nan());
        assert!(score.stddev.unwrap().is_nan());
        // distinct counts NaN once: {1, NaN, 3}.
        assert_eq!(score.distinct.as_deref(), Some("3"));
    });
}

#[test]
fn all_null_column() {
    runtime().block_on(async {
        let tmp = tempdir();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![None::<&str>, None, None]))],
        )
        .unwrap();
        let path = write_fragments(&tmp, "allnull", vec![batch]).await;
        let stats = compute_all(&path).await;

        let name = col(&stats, "name");
        assert_eq!(name.count, 0);
        assert_eq!(name.nulls, 3);
        assert_eq!(name.min, None);
        assert_eq!(name.max, None);
        assert_eq!(name.distinct.as_deref(), Some("0"));
    });
}

#[test]
fn empty_dataset_yields_zero_count_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let empty = RecordBatch::new_empty(mixed_schema());
        let path = write_fragments(&tmp, "empty", vec![empty]).await;
        let stats = compute_all(&path).await;

        assert_eq!(stats.len(), 3);
        for s in &stats {
            assert_eq!(s.count, 0, "column {} count", s.column);
            assert_eq!(s.nulls, 0, "column {} nulls", s.column);
            assert_eq!(s.min, None);
            assert_eq!(s.max, None);
            assert_eq!(s.mean, None);
            assert_eq!(s.stddev, None);
        }
        // Numeric/string columns still report a distinct count (zero).
        assert_eq!(col(&stats, "id").distinct.as_deref(), Some("0"));
    });
}

#[test]
fn nested_and_binary_columns_report_count_nulls_only() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_full(&tmp, "full").await;
        let stats = compute_all(path.to_str().unwrap()).await;

        // Binary column: count/nulls only.
        let data = col(&stats, "data");
        assert_eq!(data.count, 2);
        assert_eq!(data.nulls, 1);
        assert_eq!(data.min, None);
        assert_eq!(data.max, None);
        assert_eq!(data.mean, None);
        assert_eq!(data.distinct, None);

        // Nested list column: count/nulls only, no error.
        let tags = col(&stats, "tags");
        assert_eq!(tags.count, 2);
        assert_eq!(tags.nulls, 1);
        assert_eq!(tags.min, None);
        assert_eq!(tags.distinct, None);

        // Boolean column: min/max + distinct.
        let flag = col(&stats, "flag");
        assert_eq!(flag.count, 2);
        assert_eq!(flag.nulls, 1);
        assert_eq!(flag.min.as_deref(), Some("false"));
        assert_eq!(flag.max.as_deref(), Some("true"));
        assert_eq!(flag.distinct.as_deref(), Some("2"));

        // Timestamp column: min/max formatted, distinct, no mean.
        let ts = col(&stats, "ts");
        assert_eq!(ts.count, 2);
        assert_eq!(ts.nulls, 1);
        assert!(
            ts.min
                .as_deref()
                .unwrap()
                .starts_with("2023-11-14T22:13:20"),
            "unexpected ts min: {:?}",
            ts.min
        );
        assert!(ts.max.as_deref().unwrap() > ts.min.as_deref().unwrap());
        assert_eq!(ts.mean, None);
        assert_eq!(ts.distinct.as_deref(), Some("2"));
    });
}

#[test]
fn projection_and_filter_respected() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_fragments(&tmp, "proj", vec![mixed_batch()]).await;
        let ds = dataset::open(&path, None).await.unwrap();

        // Projection: only `score`.
        let only_score = vec!["score".to_string()];
        let stats = stats::compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            Some(&only_score),
            None,
        )
        .await
        .unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].column, "score");

        // Projection order (non-schema order) is preserved in the output rows.
        let reordered = vec!["score".to_string(), "id".to_string()];
        let stats = stats::compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            Some(&reordered),
            None,
        )
        .await
        .unwrap();
        let names: Vec<&str> = stats.iter().map(|s| s.column.as_str()).collect();
        assert_eq!(names, vec!["score", "id"]);

        // An unknown projected column is a caller error, not a panic.
        let bogus = vec!["nope".to_string()];
        let err = stats::compute(ds.as_ref(), &ScanProgress::disabled(), Some(&bogus), None)
            .await
            .unwrap_err();
        assert!(matches!(err, lance_cli::error::Error::UnknownColumn { .. }));

        // Filter: id > 2 keeps rows (3, 4, 5) → score values {30, null, 40}.
        let stats = stats::compute(ds.as_ref(), &ScanProgress::disabled(), None, Some("id > 2"))
            .await
            .unwrap();
        let id = col(&stats, "id");
        assert_eq!(id.count, 3);
        assert_eq!(id.min.as_deref(), Some("3"));
        assert_eq!(id.max.as_deref(), Some("5"));
        let score = col(&stats, "score");
        assert_eq!(score.count, 2);
        assert_eq!(score.min.as_deref(), Some("30"));
        assert_eq!(score.max.as_deref(), Some("40"));
        assert_eq!(score.mean, Some(35.0));
    });
}

#[test]
fn distinct_caps_on_high_cardinality() {
    runtime().block_on(async {
        let tmp = tempdir();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let n = stats::DISTINCT_CAP as i32 + 50;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..n).collect::<Vec<_>>()))],
        )
        .unwrap();
        let path = write_fragments(&tmp, "hicard", vec![batch]).await;
        let stats = compute_all(&path).await;

        let id = col(&stats, "id");
        assert_eq!(id.count, n as u64);
        assert_eq!(
            id.distinct.as_deref(),
            Some(format!(">{}", stats::DISTINCT_CAP).as_str())
        );
    });
}

#[test]
fn renders_all_three_formats() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_fragments(&tmp, "fmt", vec![mixed_batch()]).await;
        let stats = compute_all(&path).await;
        let batch = stats::to_record_batch(&stats).unwrap();
        let schema = stats::output_schema();

        let render = |format: Format| -> String {
            let mut out: Vec<u8> = Vec::new();
            {
                let mut w = make_writer(
                    format,
                    BinaryFormat::None,
                    TableStyle::Plain,
                    Cursor::new(&mut out),
                );
                w.start(&schema).unwrap();
                w.write_batch(&batch).unwrap();
                w.finish().unwrap();
            }
            String::from_utf8(out).unwrap()
        };

        // CSV: header + one row per column.
        let csv = render(Format::Csv);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines[0],
            "column,type,count,nulls,min,max,mean,stddev,distinct"
        );
        assert_eq!(lines.len(), 4); // header + 3 columns
        assert!(lines.iter().any(|l| l.starts_with("id,Int32,5,0,1,5,3")));

        // JSONL: one JSON object per column.
        let jsonl = render(Format::Jsonl);
        let objs: Vec<serde_json::Value> = jsonl
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(objs.len(), 3);
        let id = objs.iter().find(|o| o["column"] == "id").unwrap();
        assert_eq!(id["count"], 5);
        assert_eq!(id["mean"], 3.0);
        assert_eq!(id["distinct"], "5");

        // Table: renders without error and contains the header cells.
        let table = render(Format::Table);
        assert!(table.contains("column"));
        assert!(table.contains("distinct"));
        assert!(table.contains("alice"));
    });
}
