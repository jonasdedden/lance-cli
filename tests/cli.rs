//! End-to-end integration tests: drive the library API with realistic
//! Lance datasets and assert on the captured output.

mod common;

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use futures::StreamExt;
use lance::Dataset as LanceInner;
use lance::dataset::NewColumnTransform;
use lance_cli::cli::{BinaryFormat, Cli, Command, FilterArg, Format, RowIdArgs, VersionArgs};
use lance_cli::commands::dispatch;
use lance_cli::dataset;
use lance_cli::dataset::ScanOptions;
use lance_cli::indices;
use lance_cli::output::make_writer;
use lance_cli::output::table::TableStyle;
use lance_cli::projection;
use lance_cli::row_id::RowIds;
use lance_index::DatasetIndexExt as _;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use tokio::runtime::Runtime;

use common::{
    tempdir, write_full, write_simple, write_simple_two_versions, write_simple_with_deletions,
    write_struct, write_with_binary,
};

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn project(schema: &SchemaRef, projection: Option<&[String]>) -> SchemaRef {
    match projection {
        None => schema.clone(),
        Some(cols) => {
            let fields: Vec<_> = cols
                .iter()
                .map(|n| projection::projected_field(schema, n))
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        }
    }
}

async fn collect_cat(
    inputs: Vec<PathBuf>,
    format: Format,
    binary_format: BinaryFormat,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
) -> lance_cli::Result<String> {
    let mut out: Vec<u8> = Vec::new();
    {
        let first = dataset::open(inputs[0].to_str().unwrap(), None).await?;
        let s = first.arrow_schema();
        let proj = projection::resolve(&s, columns, exclude)?;
        let projected = project(&s, proj.as_deref());
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        for p in &inputs {
            let ds = dataset::open(p.to_str().unwrap(), None).await?;
            let options = ScanOptions {
                projection: proj.as_deref(),
                filter: None,
                ..Default::default()
            };
            let mut stream = ds.scan(&options).await?;
            while let Some(b) = stream.next().await {
                w.write_batch(&b?)?;
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_head(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> lance_cli::Result<String> {
    let ds = dataset::open(input.to_str().unwrap(), None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        let mut remaining = limit;
        if remaining > 0 {
            let mut stream = ds.scan(&ScanOptions::default()).await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let rows = batch.num_rows() as u64;
                if rows <= remaining {
                    w.write_batch(&batch)?;
                    remaining -= rows;
                } else {
                    w.write_batch(&batch.slice(0, remaining as usize))?;
                    remaining = 0;
                }
                if remaining == 0 {
                    break;
                }
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_tail(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> lance_cli::Result<String> {
    let ds = dataset::open(input.to_str().unwrap(), None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let take_n = limit.min(rowcount);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if take_n > 0 {
            let start = rowcount - take_n;
            let idx: Vec<u64> = (start..rowcount).collect();
            let batch = ds.take(&idx, None, RowIds::default()).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_take(
    input: &Path,
    idx: &str,
    format: Format,
    binary_format: BinaryFormat,
) -> lance_cli::Result<String> {
    let ds = dataset::open(input.to_str().unwrap(), None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let indices = indices::resolve(idx, rowcount)?;
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if !indices.is_empty() {
            let batch = ds.take(&indices, None, RowIds::default()).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

/// Like `collect_take` but with a projection, exercising the nested-path
/// flattening on the `take` code path.
async fn collect_take_cols(
    input: &Path,
    idx: &str,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
) -> lance_cli::Result<String> {
    let ds = dataset::open(input.to_str().unwrap(), None).await?;
    let s = ds.arrow_schema();
    let proj = projection::resolve(&s, columns, exclude)?;
    let projected = project(&s, proj.as_deref());
    let rowcount = ds.count_rows(None).await?;
    let indices = indices::resolve(idx, rowcount)?;
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Jsonl,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if !indices.is_empty() {
            let batch = ds
                .take(&indices, proj.as_deref(), RowIds::default())
                .await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_sample(
    input: &Path,
    limit: u64,
    seed: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> lance_cli::Result<String> {
    use rand::SeedableRng;
    use rand::prelude::*;
    use rand_chacha::ChaCha20Rng;

    let ds = dataset::open(input.to_str().unwrap(), None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let mut pool: Vec<u64> = (0..rowcount).collect();
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    pool.shuffle(&mut rng);
    pool.truncate(limit as usize);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if !pool.is_empty() {
            let batch = ds.take(&pool, None, RowIds::default()).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

/// Scan `input` with a `--where` filter and return every matching row as
/// JSONL. Mirrors what `cat --where` / `head --where` (large limit) produce.
async fn collect_scan_where(input: &str, filter: &str) -> lance_cli::Result<String> {
    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Jsonl,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        let options = ScanOptions {
            projection: None,
            filter: Some(filter),
            ..Default::default()
        };
        let mut stream = ds.scan(&options).await?;
        while let Some(batch) = stream.next().await {
            w.write_batch(&batch?)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

// -------------------- tests --------------------

#[test]
fn rowcount_is_5_for_simple_fixture() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "simple").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 5);
    });
}

#[test]
fn cat_jsonl_emits_nan_and_infinity_as_strings() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 5);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["id"], 1);
        assert_eq!(v0["name"], "alice");
        assert_eq!(v0["score"], 10.5);
        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v2["score"], "NaN");
        let v3: serde_json::Value = serde_json::from_str(lines[3]).unwrap();
        assert_eq!(v3["score"], "Infinity");
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["score"], serde_json::Value::Null);
    });
}

#[test]
fn cat_csv_header_and_null_cells() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,name,score");
        assert_eq!(lines[1], "1,alice,10.5");
        assert_eq!(lines[2], "2,bob,");
        assert_eq!(lines[3], "3,,NaN");
        assert_eq!(lines[4], "4,dan,inf");
        assert_eq!(lines[5], "5,eve,-1.25");
        assert_eq!(lines.len(), 6);
    });
}

#[test]
fn head_respects_limit() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_head(&p, 2, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(out.lines().count(), 2);
    });
}

#[test]
fn head_with_oversize_limit_returns_all_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_head(&p, 100, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(out.lines().count(), 5);
    });
}

#[test]
fn tail_returns_last_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_tail(&p, 2, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v0["id"], 4);
        assert_eq!(v1["id"], 5);
    });
}

#[test]
fn take_supports_ranges_and_negatives() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_take(&p, "-1,0,1:2", Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let ids: Vec<i64> = out
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, vec![5, 1, 2, 3]);
    });
}

#[test]
fn take_out_of_range_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let err = collect_take(&p, "100", Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            lance_cli::error::Error::IndexOutOfRange { .. }
        ));
    });
}

#[test]
fn sample_is_reproducible_with_seed() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let a = collect_sample(&p, 3, 42, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let b = collect_sample(&p, 3, 42, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.lines().count(), 3);
    });
}

#[test]
fn jsonl_binary_hex_emits_backslash_x_format() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let v0: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v0["data"], "\\x00\\xff");
    });
}

#[test]
fn jsonl_binary_none_renders_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let v0: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v0["data"], "BINARY_DATA");
        assert_eq!(v0["id"], 1);
        // Null binary values stay null.
        let v1: serde_json::Value = serde_json::from_str(out.lines().nth(1).unwrap()).unwrap();
        assert_eq!(v1["data"], serde_json::Value::Null);
    });
}

#[test]
fn csv_binary_none_renders_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        assert_eq!(lines[1], "1,BINARY_DATA");
        // Null binary cell stays empty.
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], "3,BINARY_DATA");
    });
}

#[test]
fn jsonl_binary_none_placeholder_for_nested_binary() {
    // Nested binary (inside a struct) should also be replaced by the placeholder
    // rather than silently becoming null.
    runtime().block_on(async {
        use arrow_array::{BinaryArray, Int32Array, RecordBatch, RecordBatchIterator, StructArray};
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;

        let inner_fields: Fields = vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrap",
            DataType::Struct(inner_fields.clone()),
            true,
        )]));
        let payload = Arc::new(BinaryArray::from_opt_vec(vec![Some(b"hello".as_ref())]));
        let n = Arc::new(Int32Array::from(vec![7]));
        let wrap = StructArray::new(inner_fields, vec![payload, n], None);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(wrap)]).unwrap();
        let tmp = tempdir();
        let path = tmp.path().join("struct_bin");
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        lance_cli::dataset::write_dataset(&path, iter)
            .await
            .unwrap();

        let out = collect_cat(vec![path], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v["wrap"]["payload"], "BINARY_DATA");
        assert_eq!(v["wrap"]["n"], 7);
    });
}

#[test]
fn csv_binary_hex_emits_escape_sequence() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        // csv::Writer quotes when a record field contains characters that need escaping.
        // Backslashes on their own are not special in CSV, so these render unquoted.
        assert_eq!(lines[1], r"1,\x00\xff");
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], r"3,\x68\x69");
    });
}

#[test]
fn csv_binary_base64_is_valid_base64() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::Base64, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        assert_eq!(lines[1], "1,AP8=");
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], "3,aGk=");
    });
}

#[test]
fn jsonl_binary_base64_emits_standard_alphabet() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Base64, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["data"], "AP8=");
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["data"], serde_json::Value::Null);
        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v2["data"], "aGk=");
    });
}

#[test]
fn explicit_include_of_binary_with_none_still_emits_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let cols = vec!["id".to_string(), "data".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["data"], "BINARY_DATA");
    });
}

#[test]
fn jsonl_emits_lists_as_arrays() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let mut lines = out.lines();
        let v0: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(v0["tags"], serde_json::json!(["a", "b"]));
        let v1: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(v1["tags"], serde_json::Value::Null);
    });
}

#[test]
fn columns_preserves_user_order() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let cols = vec!["score".to_string(), "id".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["score", "id"]);
    });
}

#[test]
fn exclude_columns_drops_specified() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let excl = vec!["name".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            None,
            Some(&excl),
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["id", "score"]);
    });
}

#[test]
fn unknown_column_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let cols = vec!["zzz".to_string()];
        let err = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, lance_cli::error::Error::UnknownColumn { .. }));
    });
}

// -------------------- globs & nested paths (#10) --------------------

fn cols(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn keys(line: &str) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    v.as_object().unwrap().keys().cloned().collect()
}

#[test]
fn glob_include_expands_in_schema_order_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["id", "emb_*"])),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            keys(out.lines().next().unwrap()),
            cols(&["id", "emb_0", "emb_1", "emb_2"])
        );
    });
}

#[test]
fn glob_exclude_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            None,
            Some(&cols(&["emb_*"])),
        )
        .await
        .unwrap();
        // meta untouched by the glob stays a whole struct column.
        assert_eq!(
            keys(out.lines().next().unwrap()),
            cols(&["id", "score", "meta"])
        );
    });
}

#[test]
fn glob_no_match_errors_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let err = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["nope_*"])),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, lance_cli::error::Error::NoGlobMatch { .. }));
    });
}

#[test]
fn nested_path_through_cat_flattens_to_dotted_columns() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["meta.user.id", "id"])),
            None,
        )
        .await
        .unwrap();
        let line = out.lines().next().unwrap();
        assert_eq!(keys(line), cols(&["meta.user.id", "id"]));
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["meta.user.id"], 10);
        assert_eq!(v["id"], 1);
    });
}

#[test]
fn parent_and_leaf_overlap_agree_scan_and_take() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let want = cols(&["meta", "meta.user.id"]);
        // Scan path (cat).
        let cat = collect_cat(
            vec![p.clone()],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&want),
            None,
        )
        .await
        .unwrap();
        // Take path.
        let take = collect_take_cols(&p, "0", Some(&want), None).await.unwrap();

        let cat0 = cat.lines().next().unwrap();
        let take0 = take.lines().next().unwrap();
        // Whole struct + duplicate flat leaf, and both paths agree.
        assert_eq!(keys(cat0), cols(&["meta", "meta.user.id"]));
        assert_eq!(keys(take0), cols(&["meta", "meta.user.id"]));
        let cv: serde_json::Value = serde_json::from_str(cat0).unwrap();
        let tv: serde_json::Value = serde_json::from_str(take0).unwrap();
        assert_eq!(cv, tv);
        assert_eq!(cv["meta.user.id"], 10);
        assert_eq!(cv["meta"]["user"]["id"], 10);
    });
}

#[test]
fn nested_and_glob_combined_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["emb_*", "meta.user.id"])),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            keys(out.lines().next().unwrap()),
            cols(&["emb_0", "emb_1", "emb_2", "meta.user.id"])
        );
    });
}

#[test]
fn nested_path_through_take_flattens() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_take_cols(&p, "0", Some(&cols(&["meta.user.name", "id"])), None)
            .await
            .unwrap();
        let line = out.lines().next().unwrap();
        assert_eq!(keys(line), cols(&["meta.user.name", "id"]));
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["meta.user.name"], "alice");
        assert_eq!(v["id"], 1);
    });
}

/// The header schema built by `project_arrow_schema`/`project` must equal the
/// actual batch schema Lance's scanner returns for a nested projection — the
/// JSONL/CSV writers rely on this being exact.
#[test]
fn nested_projection_header_matches_scan_output() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        let s = ds.arrow_schema();
        let projcols = cols(&["meta.source", "meta.user.name", "id", "emb_1"]);
        let proj = projection::resolve(&s, Some(&projcols), None).unwrap();
        let header = project(&s, proj.as_deref());
        let options = ScanOptions {
            projection: proj.as_deref(),
            filter: None,
            ..Default::default()
        };
        let mut stream = ds.scan(&options).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(header.fields().len(), batch.schema().fields().len());
        for (h, b) in header.fields().iter().zip(batch.schema().fields().iter()) {
            assert_eq!(h.name(), b.name(), "field name mismatch");
            assert_eq!(
                h.data_type(),
                b.data_type(),
                "type mismatch for {}",
                h.name()
            );
            assert_eq!(
                h.is_nullable(),
                b.is_nullable(),
                "nullability mismatch for {}",
                h.name()
            );
        }
    });
}

#[test]
fn exclude_nested_leaf_flattens_siblings_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            None,
            Some(&cols(&["meta.user.id", "emb_*"])),
        )
        .await
        .unwrap();
        let line = out.lines().next().unwrap();
        assert_eq!(
            keys(line),
            cols(&["id", "score", "meta.user.name", "meta.source"])
        );
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["meta.user.name"], "alice");
        assert_eq!(v["meta.source"], "web");
    });
}

#[test]
fn nested_invalid_field_errors_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let err = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["meta.nope"])),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            lance_cli::error::Error::UnknownNestedField { .. }
        ));
    });
}

#[test]
fn nested_non_struct_traversal_errors_through_cat() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let err = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols(&["score.x"])),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            lance_cli::error::Error::NonStructField { .. }
        ));
    });
}

/// `--where` filters rows *before* projection, so filtering on a column that is
/// projected away still works.
#[test]
fn where_on_projected_away_column_still_filters() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = common::write_struct(&tmp, "st").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        let s = ds.arrow_schema();
        let proj = projection::resolve(&s, Some(&cols(&["id"])), None).unwrap();
        let options = ScanOptions {
            projection: proj.as_deref(),
            filter: Some("score > 1.5"),
            ..Default::default()
        };
        use arrow_array::Array as _;
        let mut stream = ds.scan(&options).await.unwrap();
        let mut ids_out = Vec::new();
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            assert_eq!(b.num_columns(), 1, "only id projected");
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap();
            ids_out.extend(col.values().iter().copied());
        }
        // Only row 2 (score 2.5) passes; its id is 2.
        assert_eq!(ids_out, vec![2]);
    });
}

/// End-to-end through the real `head` binary on a nested projection.
#[test]
fn head_nested_path_end_to_end_binary() {
    let tmp = tempdir();
    let p = runtime().block_on(async { common::write_struct(&tmp, "st").await });
    let out = run_cli(
        &["head", "--format", "jsonl", "--columns", "meta.user.id,id"],
        &p,
    );
    assert!(
        out.status.success(),
        "head failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let first = stdout.lines().next().expect("at least one row");
    let v: serde_json::Value = serde_json::from_str(first).unwrap();
    assert_eq!(v["meta.user.id"], 10);
    assert_eq!(v["id"], 1);
}

/// `schema --type arrow` reports nested projections as flat dotted fields.
#[test]
fn schema_arrow_nested_flat_fields() {
    let tmp = tempdir();
    let p = runtime().block_on(async { common::write_struct(&tmp, "st").await });
    let out = run_cli(&["schema", "--columns", "meta.user.id,id"], &p);
    assert!(
        out.status.success(),
        "schema failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("meta.user.id"),
        "flat dotted field missing:\n{stdout}"
    );
}

#[test]
fn cat_table_renders_header_and_rows_in_ascii() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Table, BinaryFormat::None, None, None)
            .await
            .unwrap();
        // Test runs are non-tty → ASCII preset (uses '+', '|', '-').
        assert!(out.contains('+'), "table border missing in:\n{out}");
        assert!(out.contains("| name"), "header missing in:\n{out}");
        assert!(out.contains("| alice"), "alice row missing in:\n{out}");
        assert!(
            out.contains("NaN"),
            "NaN should render literally in:\n{out}"
        );
    });
}

#[test]
fn jsonl_emits_lists_as_arrays_table_compatibility() {
    // Table format must render nested cells as compact JSON literals, even
    // though CSV would have rejected them. Uses the full fixture which has a
    // List<Utf8> column.
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Table, BinaryFormat::None, None, None)
            .await
            .unwrap();
        assert!(
            out.contains("[\"a\",\"b\"]"),
            "list cell missing in:\n{out}"
        );
    });
}

#[test]
fn format_on_schema_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Table),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            max_list_items: None,
            max_cell_width: None,
            float_precision: None,
            no_progress: false,
            command: Command::Schema {
                input: "does-not-matter".to_string(),
                ty: lance_cli::cli::SchemaType::Arrow,
                version: lance_cli::cli::VersionArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::FormatNotApplicable { command: "schema" })
        ));
    });
}

#[test]
fn format_on_rowcount_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Jsonl),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            max_list_items: None,
            max_cell_width: None,
            float_precision: None,
            no_progress: false,
            command: Command::Rowcount {
                input: "does-not-matter".to_string(),
                filter: FilterArg::default(),
                version: lance_cli::cli::VersionArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::FormatNotApplicable {
                command: "rowcount"
            })
        ));
    });
}

#[test]
fn empty_cat_via_dispatch_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Jsonl),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            max_list_items: None,
            max_cell_width: None,
            float_precision: None,
            no_progress: false,
            command: Command::Cat {
                inputs: vec![],
                filter: FilterArg::default(),
                row_ids: RowIdArgs::default(),
                version: lance_cli::cli::VersionArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(res, Err(lance_cli::error::Error::EmptyInputs)));
    });
}

#[test]
fn csv_quotes_column_name_containing_comma() {
    // csv::Writer handles quoting automatically; names with commas or newlines
    // emerge as standard-CSV-quoted tokens rather than being rejected.
    runtime().block_on(async {
        use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
        use arrow_schema::{DataType, Field, Schema};
        let tmp = tempdir();
        let path = tmp.path().join("weird");
        let schema = Arc::new(Schema::new(vec![Field::new("a,b", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        lance_cli::dataset::write_dataset(&path, iter)
            .await
            .unwrap();
        let out = collect_cat(vec![path], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let mut lines = out.lines();
        assert_eq!(lines.next().unwrap(), r#""a,b""#);
        assert_eq!(lines.next().unwrap(), "1");
    });
}

// -------------------- --where predicate filtering --------------------

fn ids(out: &str) -> Vec<i64> {
    out.lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect()
}

#[test]
fn where_number_predicate_filters_and_preserves_order() {
    // Filtered scan keeps dataset order, so `head -n 1` semantics fall out of
    // taking the first matching row.
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(p.to_str().unwrap(), "id >= 3")
            .await
            .unwrap();
        assert_eq!(ids(&out), vec![3, 4, 5]);
    });
}

#[test]
fn where_string_predicate_filters_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(p.to_str().unwrap(), "name = 'alice'")
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 1);
        let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["name"], "alice");
    });
}

#[test]
fn where_empty_result_set_yields_no_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(p.to_str().unwrap(), "id > 100")
            .await
            .unwrap();
        assert_eq!(out.lines().count(), 0);
    });
}

#[test]
fn rowcount_with_where_uses_filtered_count() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        assert_eq!(ds.count_rows(Some("id >= 4")).await.unwrap(), 2);
        // Empty result set counts as zero, not an error.
        assert_eq!(ds.count_rows(Some("id > 100")).await.unwrap(), 0);
    });
}

#[test]
fn invalid_where_predicate_on_scan_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        let options = ScanOptions {
            projection: None,
            filter: Some("not_a_column > 1"),
            ..Default::default()
        };
        // `scan` yields a non-Debug `BatchStream` on success, so match rather
        // than `unwrap_err`.
        match ds.scan(&options).await {
            Err(lance_cli::error::Error::InvalidPredicate(_)) => {}
            other => panic!("expected InvalidPredicate, got {:?}", other.map(|_| ())),
        }
    });
}

#[test]
fn invalid_where_predicate_on_rowcount_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(p.to_str().unwrap(), None).await.unwrap();
        let err = ds
            .count_rows(Some("this is not sql ((("))
            .await
            .unwrap_err();
        assert!(matches!(err, lance_cli::error::Error::InvalidPredicate(_)));
    });
}

#[test]
fn take_with_where_is_rejected() {
    runtime().block_on(async {
        let cli = Cli {
            format: None,
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            max_list_items: None,
            max_cell_width: None,
            float_precision: None,
            no_progress: false,
            command: Command::Take {
                input: "does-not-matter".to_string(),
                indices: "0".to_string(),
                filter: FilterArg {
                    predicate: Some("id > 1".to_string()),
                },
                row_ids: RowIdArgs::default(),
                version: VersionArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::TakeWhereConflict)
        ));
    });
}

// The streaming filtered `tail` and `sample` algorithms themselves are unit
// tested against the real production functions (with a multi-fragment fixture)
// in `src/commands/tail.rs` and `src/commands/sample.rs`.

// -------------------- stdout hygiene on error paths --------------------
//
// These drive the real binary and assert nothing reaches stdout when a command
// errors — in CSV the header row is emitted by `writer.start()`, so an empty
// stdout proves `start()` was never reached. Regression guard for the header
// leaking before a failed `count_rows` / predicate parse.

fn run_cli(args: &[&str], path: &Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lance-cli"))
        .args(args)
        .arg(path)
        .output()
        .expect("spawn lance-cli binary")
}

fn assert_clean_failure(out: &std::process::Output, stderr_needle: &str) {
    assert!(!out.status.success(), "command unexpectedly succeeded");
    assert!(
        out.stdout.is_empty(),
        "stdout should be empty on error, got: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(stderr_needle),
        "stderr missing {stderr_needle:?}, got: {stderr}"
    );
}

#[test]
fn head_where_invalid_predicate_writes_nothing_to_stdout() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &["head", "--format", "csv", "--where", "not_a_column > 1"],
        &p,
    );
    assert_clean_failure(&out, "invalid --where predicate");
}

#[test]
fn tail_where_invalid_predicate_writes_nothing_to_stdout() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &["tail", "--format", "csv", "--where", "not_a_column > 1"],
        &p,
    );
    assert_clean_failure(&out, "invalid --where predicate");
}

#[test]
fn cat_where_invalid_predicate_writes_nothing_to_stdout() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &["cat", "--format", "csv", "--where", "not_a_column > 1"],
        &p,
    );
    assert_clean_failure(&out, "invalid --where predicate");
}

#[test]
fn sample_where_invalid_predicate_writes_nothing_to_stdout() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &[
            "sample",
            "-n",
            "2",
            "--format",
            "csv",
            "--where",
            "not_a_column > 1",
        ],
        &p,
    );
    assert_clean_failure(&out, "invalid --where predicate");
}

#[test]
fn sample_oversize_writes_nothing_to_stdout() {
    // Regression: on main this printed nothing; the header must not leak before
    // the `limit > rowcount` check. The fixture has 5 rows.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(&["sample", "-n", "100", "--format", "csv"], &p);
    assert_clean_failure(&out, "larger than");
}

// --------------------------- search arg parsing -----------------------------

/// `-k 0` must be rejected at the clap layer with a clean message rather than
/// dying deep inside Lance with a registry-path error.
#[test]
fn search_rejects_k_zero() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "search",
        "--column",
        "embedding",
        "--vector",
        "[0.1]",
        "-k",
        "0",
        "ds.lance",
    ]);
    assert!(res.is_err(), "expected -k 0 to be rejected");
}

/// A positive `-k` parses fine.
#[test]
fn search_accepts_positive_k() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "search",
        "--column",
        "embedding",
        "--vector",
        "[0.1]",
        "-k",
        "5",
        "ds.lance",
    ]);
    assert!(res.is_ok(), "expected -k 5 to parse: {res:?}");
}

/// `--nprobes 0` silently returns zero rows on indexed datasets, so reject it
/// at the clap layer.
#[test]
fn search_rejects_nprobes_zero() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "search",
        "--column",
        "embedding",
        "--vector",
        "[0.1]",
        "--nprobes",
        "0",
        "ds.lance",
    ]);
    assert!(res.is_err(), "expected --nprobes 0 to be rejected");
}

/// Supplying neither `--vector` nor `--vector-file` is a parse error.
#[test]
fn search_requires_a_query_vector() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "search",
        "--column",
        "embedding",
        "ds.lance",
    ]);
    assert!(res.is_err(), "expected missing query vector to be rejected");
}

// ----------------------------- --as-of parsing ------------------------------

/// `--as-of` and `--version` both name a single version → clap must reject the
/// combination.
#[test]
fn as_of_conflicts_with_version() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "head",
        "--as-of",
        "2026-07-01T12:00:00Z",
        "--version",
        "1",
        "ds.lance",
    ]);
    assert!(res.is_err(), "expected --as-of + --version to be rejected");
}

/// `--as-of` and `--tag` are likewise mutually exclusive.
#[test]
fn as_of_conflicts_with_tag() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "head",
        "--as-of",
        "2026-07-01",
        "--tag",
        "release",
        "ds.lance",
    ]);
    assert!(res.is_err(), "expected --as-of + --tag to be rejected");
}

/// `--as-of` composes with `--branch` (they select different axes).
#[test]
fn as_of_combines_with_branch() {
    let res = <Cli as clap::Parser>::try_parse_from([
        "lance-cli",
        "head",
        "--as-of",
        "2026-07-01T12:00:00Z",
        "--branch",
        "dev",
        "ds.lance",
    ]);
    assert!(res.is_ok(), "expected --as-of + --branch to parse: {res:?}");
}

// -------------------- freq (value counts) --------------------

#[test]
fn freq_defaults_to_table_format() {
    // No --format given: freq is a summary command and defaults to Table.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(&["freq", "--column", "name"], &p);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("| value"), "header missing:\n{stdout}");
    assert!(stdout.contains("| count"), "header missing:\n{stdout}");
    assert!(stdout.contains("| alice"), "value row missing:\n{stdout}");
    // The one null `name` cell becomes an explicit NULL row.
    assert!(stdout.contains("| NULL"), "NULL row missing:\n{stdout}");
}

#[test]
fn freq_csv_reports_counts_and_percent() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    // Every name is distinct (4 present + 1 null over 5 rows) → each 20%.
    let out = run_cli(
        &[
            "freq", "--column", "name", "--sort", "value", "--format", "csv",
        ],
        &p,
    );
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "value,count,percent");
    assert_eq!(lines[1], "alice,1,20.0%");
    // NULL sorts last under --sort value.
    assert_eq!(*lines.last().unwrap(), "NULL,1,20.0%");
}

#[test]
fn freq_where_composes_over_filtered_subset() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &[
            "freq", "--column", "name", "--format", "csv", "--where", "id >= 4",
        ],
        &p,
    );
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Only ids 4,5 survive: names dan, eve — each 50% of the filtered subset.
    let mut data: Vec<&str> = stdout.lines().skip(1).collect();
    data.sort_unstable();
    assert_eq!(data, vec!["dan,1,50.0%", "eve,1,50.0%"]);
}

#[test]
fn freq_on_nested_column_fails_cleanly() {
    // The full fixture has a List<Utf8> column `tags`; freq must reject it and
    // leave stdout untouched.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_full(&tmp, "f").await });
    let out = run_cli(&["freq", "--column", "tags", "--format", "csv"], &p);
    assert_clean_failure(&out, "freq cannot count");
}

#[test]
fn freq_rejects_zero_limit() {
    // clap enforces -n >= 1; `-n 0` is a usage error, nothing reaches stdout.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &["freq", "--column", "name", "-n", "0", "--format", "csv"],
        &p,
    );
    assert!(!out.status.success(), "expected -n 0 to be rejected");
    assert!(
        out.stdout.is_empty(),
        "stdout should be empty: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
}
// ------------------------------ diff (issue #19) ----------------------------

fn diff_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn diff_batch(ids: Vec<i32>, vals: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        diff_schema(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(vals)),
        ],
    )
    .unwrap()
}

/// Build a four-version dataset on `main` exercising every diff bucket:
///   v1 write [1,2]            (fragment 0, 2 rows)
///   v2 append [3]            (fragment 1, +1 row); tag `v2` → version 2
///   v3 delete id=1           (tombstone in fragment 0)
///   v4 add column `doubled`  (schema evolution: appends a file to each fragment)
/// No index (index builds serialise poorly across parallel test threads — the
/// index test uses its own fixture).
fn build_diff_fixture(path: &Path) -> Runtime {
    let rt = runtime();
    rt.block_on(async {
        let uri = path.to_string_lossy().into_owned();
        let iter = RecordBatchIterator::new(
            vec![Ok(diff_batch(vec![1, 2], vec!["a", "b"]))],
            diff_schema(),
        );
        let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();

        let iter =
            RecordBatchIterator::new(vec![Ok(diff_batch(vec![3], vec!["c"]))], diff_schema());
        ds.append(iter, None).await.unwrap();
        ds.tags().create("v2", 2u64).await.unwrap();

        ds.delete("id = 1").await.unwrap();

        ds.add_columns(
            NewColumnTransform::SqlExpressions(vec![("doubled".to_string(), "id * 2".to_string())]),
            None,
            None,
        )
        .await
        .unwrap();
    });
    rt
}

/// Run `lance-cli diff <args> <path>` and return the process output.
fn run_diff(args: &[&str], path: &Path) -> std::process::Output {
    let mut full = vec!["diff"];
    full.extend_from_slice(args);
    run_cli(&full, path)
}

/// Run a jsonl diff and parse the single JSON record, asserting the exit code.
fn diff_json(args: &[&str], path: &Path, expect_code: i32) -> serde_json::Value {
    let mut full: Vec<&str> = args.to_vec();
    full.extend_from_slice(&["--format", "jsonl"]);
    let out = run_diff(&full, path);
    assert_eq!(
        out.status.code(),
        Some(expect_code),
        "exit code mismatch; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "diff jsonl not parseable ({e}); stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

#[test]
fn diff_pure_append_reports_added_rows_and_fragment() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let v = diff_json(&["--from", "1", "--to", "2"], &path, 1);
    assert_eq!(v["identical"], serde_json::json!(false));
    assert_eq!(v["rows"]["from"], serde_json::json!(2));
    assert_eq!(v["rows"]["to"], serde_json::json!(3));
    assert_eq!(v["rows"]["added"], serde_json::json!(1));
    assert_eq!(v["rows"]["deleted"], serde_json::json!(0));
    assert_eq!(v["rows"]["net"], serde_json::json!(1));
    assert_eq!(v["fragments"]["added"], serde_json::json!([1]));
    assert_eq!(v["fragments"]["removed"], serde_json::json!([]));
}

#[test]
fn diff_delete_reports_deleted_split_not_just_net() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    // v2 (3 live rows) -> v3 (id=1 tombstoned, 2 live rows).
    let v = diff_json(&["--from", "2", "--to", "3"], &path, 1);
    assert_eq!(v["rows"]["from"], serde_json::json!(3));
    assert_eq!(v["rows"]["to"], serde_json::json!(2));
    assert_eq!(v["rows"]["added"], serde_json::json!(0));
    assert_eq!(v["rows"]["deleted"], serde_json::json!(1));
    assert_eq!(v["rows"]["net"], serde_json::json!(-1));
    // A tombstone rewrites no data file, so it stays out of the fragment buckets.
    assert_eq!(v["fragments"]["added"], serde_json::json!([]));
    assert_eq!(v["fragments"]["removed"], serde_json::json!([]));
    assert_eq!(v["fragments"]["rewritten"], serde_json::json!([]));
}

#[test]
fn diff_schema_evolution_reports_added_column_and_rewritten_fragments() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    // v3 -> v4 added the `doubled` column via schema evolution.
    let v = diff_json(&["--from", "3", "--to", "4"], &path, 1);
    let added = v["schema"]["added"].as_array().unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["name"], serde_json::json!("doubled"));
    assert!(
        added[0]["type"].as_str().unwrap().starts_with("Int32"),
        "unexpected type: {}",
        added[0]["type"]
    );
    // Both surviving fragments gained a data file → rewritten, not removed/added.
    assert_eq!(v["fragments"]["rewritten"], serde_json::json!([0, 1]));
    assert_eq!(v["fragments"]["added"], serde_json::json!([]));
    assert_eq!(v["fragments"]["removed"], serde_json::json!([]));
    // Row count is unchanged by adding a column.
    assert_eq!(v["rows"]["added"], serde_json::json!(0));
    assert_eq!(v["rows"]["deleted"], serde_json::json!(0));
}

#[test]
fn diff_to_defaults_to_branch_latest_and_lists_version_log() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    // No --to → latest (v4). Version log covers (1, 4].
    let v = diff_json(&["--from", "1"], &path, 1);
    assert_eq!(v["to"]["version"], serde_json::json!(4));
    let versions: Vec<u64> = v["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["version"].as_u64().unwrap())
        .collect();
    assert_eq!(versions, vec![2, 3, 4]);
}

#[test]
fn diff_selects_endpoints_by_tag() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let v = diff_json(&["--from-tag", "v2", "--to", "3"], &path, 1);
    assert_eq!(v["from"]["version"], serde_json::json!(2));
    assert_eq!(v["from"]["branch"], serde_json::json!("main"));
    assert_eq!(v["to"]["version"], serde_json::json!(3));
}

#[test]
fn diff_identical_versions_exit_zero_jsonl() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let v = diff_json(&["--from", "4", "--to", "4"], &path, 0);
    assert_eq!(v["identical"], serde_json::json!(true));
    assert_eq!(v["rows"]["net"], serde_json::json!(0));
}

#[test]
fn diff_identical_versions_human_says_no_differences() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let out = run_diff(&["--from", "4", "--to", "4"], &path);
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("No differences."),
        "expected 'No differences.' in:\n{stdout}"
    );
}

#[test]
fn diff_human_summary_reports_rows_and_schema() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let out = run_diff(&["--from", "1", "--to", "4"], &path);
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Rows:"), "missing Rows line:\n{stdout}");
    assert!(
        stdout.contains("+ doubled"),
        "missing added column:\n{stdout}"
    );
    assert!(
        stdout.contains("Versions in range"),
        "missing version log:\n{stdout}"
    );
}

#[test]
fn diff_missing_dataset_errors_with_exit_two() {
    let tmp = tempdir();
    let missing = tmp.path().join("nope.lance");
    let out = run_diff(&["--from", "1", "--to", "2"], &missing);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout should be empty on error");
}

#[test]
fn diff_unsupported_format_errors_with_exit_two() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    build_diff_fixture(&path);

    let out = run_diff(&["--from", "1", "--to", "2", "--format", "csv"], &path);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("diff supports only"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn diff_restore_reports_added_rows_not_no_differences() {
    // Regression: a version `restore` that un-deletes rows changes only the live
    // row count. It must report `+added` rows and exit 1, never "No differences".
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    let uri = path.to_string_lossy().into_owned();
    runtime().block_on(async {
        // v1: 9 rows.
        let iter = RecordBatchIterator::new(
            vec![Ok(diff_batch(
                (1..=9).collect(),
                vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"],
            ))],
            diff_schema(),
        );
        let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
        // v2: delete 2 rows → 7 live.
        ds.delete("id in (1, 2)").await.unwrap();
        // v3: restore v1 → 9 live again.
        let mut restored = ds.checkout_version(1).await.unwrap();
        restored.restore().await.unwrap();
    });

    // JSON view: from v2 (7 live) to v3 (9 live) → +2 added, exit 1.
    let v = diff_json(&["--from", "2", "--to", "3"], &path, 1);
    assert_eq!(v["identical"], serde_json::json!(false));
    assert_eq!(v["rows"]["from"], serde_json::json!(7));
    assert_eq!(v["rows"]["to"], serde_json::json!(9));
    assert_eq!(v["rows"]["added"], serde_json::json!(2));
    assert_eq!(v["rows"]["deleted"], serde_json::json!(0));
    assert_eq!(v["rows"]["net"], serde_json::json!(2));

    // Human view must not claim "No differences".
    let out = run_diff(&["--from", "2", "--to", "3"], &path);
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("No differences."),
        "restore reported identical:\n{stdout}"
    );
}

#[test]
fn diff_index_creation_is_reported() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    let uri = path.to_string_lossy().into_owned();
    runtime().block_on(async {
        let iter = RecordBatchIterator::new(
            vec![Ok(diff_batch(vec![1, 2], vec!["a", "b"]))],
            diff_schema(),
        );
        let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
        // v2: create a scalar index on `id`.
        ds.create_index(
            &["id"],
            IndexType::BTree,
            Some("idx_id".to_string()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
    });

    let v = diff_json(&["--from", "1", "--to", "2"], &path, 1);
    let created = v["indices"]["created"].as_array().unwrap();
    assert_eq!(created, &vec![serde_json::json!("idx_id")]);
    assert_eq!(v["indices"]["dropped"], serde_json::json!([]));
}

#[test]
fn diff_cross_branch_endpoints_error_with_exit_two() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    let uri = path.to_string_lossy().into_owned();
    runtime().block_on(async {
        let iter = RecordBatchIterator::new(
            vec![Ok(diff_batch(vec![1, 2], vec!["a", "b"]))],
            diff_schema(),
        );
        let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
        let iter =
            RecordBatchIterator::new(vec![Ok(diff_batch(vec![3], vec!["c"]))], diff_schema());
        ds.append(iter, None).await.unwrap();
        // Tag on main, and a branch `dev` off v2 with its own tag.
        ds.tags().create("main-tag", 2u64).await.unwrap();
        let dev = ds.create_branch("dev", 2u64, None).await.unwrap();
        dev.tags().create("dev-tag", ("dev", 2u64)).await.unwrap();
    });

    // from-tag on main, to-tag on dev → cross-branch comparison is rejected.
    let out = run_diff(&["--from-tag", "main-tag", "--to-tag", "dev-tag"], &path);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("different branches"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn diff_tag_on_wrong_branch_errors_with_exit_two() {
    let tmp = tempdir();
    let path = tmp.path().join("ds");
    let uri = path.to_string_lossy().into_owned();
    runtime().block_on(async {
        let iter = RecordBatchIterator::new(
            vec![Ok(diff_batch(vec![1, 2], vec!["a", "b"]))],
            diff_schema(),
        );
        let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
        let iter =
            RecordBatchIterator::new(vec![Ok(diff_batch(vec![3], vec!["c"]))], diff_schema());
        ds.append(iter, None).await.unwrap();
        ds.tags().create("main-tag", 2u64).await.unwrap();
        ds.create_branch("dev", 2u64, None).await.unwrap();
    });

    // `main-tag` lives on main; asking for it under --branch dev must error.
    let out = run_diff(
        &["--from-tag", "main-tag", "--to", "2", "--branch", "dev"],
        &path,
    );
    assert_eq!(out.status.code(), Some(2));
}

// -------------------- diff dataset-vs-dataset (issue #13) --------------------

/// Run `lance-cli <args>` with no implicit trailing dataset path (the two-positional
/// dataset-vs-dataset diff needs full control over the argument vector).
fn run_bin(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lance-cli"))
        .args(args)
        .output()
        .expect("spawn lance-cli binary")
}

/// Write a single-batch Lance dataset under `dir`, returning its path as a
/// `String` (the form the two-positional CLI wants).
fn write_batch_dataset(dir: &Path, name: &str, batch: RecordBatch) -> String {
    let uri = dir.join(name).to_string_lossy().into_owned();
    runtime().block_on(async {
        let schema = batch.schema();
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    });
    uri
}

/// A dataset with an `id: Int32, value: Utf8` schema and the given rows.
fn write_idval(dir: &Path, name: &str, ids: Vec<i32>, vals: Vec<&str>) -> String {
    write_batch_dataset(dir, name, diff_batch(ids, vals))
}

/// A dataset with a `meta: Struct<{ id }>` column, where `id` is `Int64` when
/// `id_i64`, else `Int32`. Used to exercise structural nested-type comparison.
fn write_nested(dir: &Path, name: &str, id_i64: bool) -> String {
    let (id_field, id_arr): (Field, ArrayRef) = if id_i64 {
        (
            Field::new("id", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
        )
    } else {
        (
            Field::new("id", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1i32, 2, 3])),
        )
    };
    let fields = Fields::from(vec![id_field]);
    let meta = StructArray::new(fields.clone(), vec![id_arr], None);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "meta",
        DataType::Struct(fields),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(meta)]).unwrap();
    write_batch_dataset(dir, name, batch)
}

/// Run `lance-cli diff A B <extra…> --format jsonl`, assert the exit code, and parse.
fn dsdiff_json(a: &str, b: &str, extra: &[&str], expect_code: i32) -> serde_json::Value {
    let mut args = vec!["diff", a, b];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--format", "jsonl"]);
    let out = run_bin(&args);
    assert_eq!(
        out.status.code(),
        Some(expect_code),
        "exit code mismatch; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "dataset diff jsonl not parseable ({e}); stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

#[test]
fn dsdiff_identical_datasets_exit_zero() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b = write_idval(tmp.path(), "b.lance", vec![1, 2, 3], vec!["x", "y", "z"]);

    let out = run_bin(&["diff", &a, &b]);
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("No differences."),
        "expected 'No differences.' in:\n{stdout}"
    );

    // jsonl view of an identical pair: identical=true, empty deltas.
    let v = dsdiff_json(&a, &b, &[], 0);
    assert_eq!(v["identical"], serde_json::json!(true));
    assert_eq!(v["rows"]["net"], serde_json::json!(0));
    assert_eq!(v["schema"]["added"], serde_json::json!([]));
    assert_eq!(v["schema"]["removed"], serde_json::json!([]));
    assert_eq!(v["schema"]["retyped"], serde_json::json!([]));
    assert_eq!(v["metadata"]["added"], serde_json::json!([]));
}

#[test]
fn dsdiff_schema_and_rowcount_difference_jsonl_fields() {
    let tmp = tempdir();
    // A: id, value; 3 rows.
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    // B: id, value, extra; 2 rows.
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("extra", DataType::Float64, true),
    ]));
    let b_batch = RecordBatch::try_new(
        b_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["p", "q"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();
    let b = write_batch_dataset(tmp.path(), "b.lance", b_batch);

    let v = dsdiff_json(&a, &b, &[], 1);
    // Documented, stable field names.
    assert_eq!(v["left"], serde_json::json!(a));
    assert_eq!(v["right"], serde_json::json!(b));
    assert_eq!(v["identical"], serde_json::json!(false));
    assert_eq!(v["rows"]["left"], serde_json::json!(3));
    assert_eq!(v["rows"]["right"], serde_json::json!(2));
    assert_eq!(v["rows"]["net"], serde_json::json!(-1));
    let added = v["schema"]["added"].as_array().unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["name"], serde_json::json!("extra"));
    assert!(added[0]["type"].as_str().unwrap().starts_with("Float64"));
    assert_eq!(v["schema"]["removed"], serde_json::json!([]));
    assert_eq!(v["schema"]["retyped"], serde_json::json!([]));
    // Metadata object is always present with its three arrays.
    assert!(v["metadata"]["added"].is_array());
    assert!(v["metadata"]["removed"].is_array());
    assert!(v["metadata"]["changed"].is_array());
}

#[test]
fn dsdiff_rowcount_only_difference() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b = write_idval(
        tmp.path(),
        "b.lance",
        vec![1, 2, 3, 4, 5],
        vec!["x", "y", "z", "p", "q"],
    );

    let v = dsdiff_json(&a, &b, &[], 1);
    assert_eq!(v["rows"]["left"], serde_json::json!(3));
    assert_eq!(v["rows"]["right"], serde_json::json!(5));
    assert_eq!(v["rows"]["net"], serde_json::json!(2));
    // Schema is unchanged even though the row counts differ.
    assert_eq!(v["schema"]["added"], serde_json::json!([]));
    assert_eq!(v["schema"]["removed"], serde_json::json!([]));
    assert_eq!(v["schema"]["retyped"], serde_json::json!([]));
}

#[test]
fn dsdiff_retyped_column_is_reported() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    // B: same names, but `id` is Int64.
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let b_batch = RecordBatch::try_new(
        b_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();
    let b = write_batch_dataset(tmp.path(), "b.lance", b_batch);

    let v = dsdiff_json(&a, &b, &[], 1);
    let retyped = v["schema"]["retyped"].as_array().unwrap();
    assert_eq!(retyped.len(), 1);
    assert_eq!(retyped[0]["name"], serde_json::json!("id"));
    assert!(retyped[0]["from"].as_str().unwrap().starts_with("Int32"));
    assert!(retyped[0]["to"].as_str().unwrap().starts_with("Int64"));
}

#[test]
fn dsdiff_nested_type_difference_compared_structurally() {
    let tmp = tempdir();
    let a = write_nested(tmp.path(), "a.lance", true); // meta.id: Int64
    let b = write_nested(tmp.path(), "b.lance", false); // meta.id: Int32

    let v = dsdiff_json(&a, &b, &[], 1);
    let retyped = v["schema"]["retyped"].as_array().unwrap();
    assert_eq!(retyped.len(), 1, "expected one retyped column: {v}");
    assert_eq!(retyped[0]["name"], serde_json::json!("meta"));
    // The nested label recurses through the struct, mentioning both inner types.
    assert!(retyped[0]["from"].as_str().unwrap().contains("Int64"));
    assert!(retyped[0]["to"].as_str().unwrap().contains("Int32"));
}

#[test]
fn dsdiff_projection_scopes_the_comparison() {
    let tmp = tempdir();
    // A: id, value; B: id, value, extra. Same 3 rows.
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("extra", DataType::Float64, true),
    ]));
    let b_batch = RecordBatch::try_new(
        b_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    let b = write_batch_dataset(tmp.path(), "b.lance", b_batch);

    // Unscoped: `extra` makes them differ.
    assert_eq!(run_bin(&["diff", &a, &b]).status.code(), Some(1));

    // Scoped to the shared columns: identical -> exit 0.
    let out = run_bin(&["diff", &a, &b, "--columns", "id,value"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("No differences."));
}

#[test]
fn dsdiff_projection_absent_on_one_side_errors() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b = write_idval(tmp.path(), "b.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    // `extra` exists on neither: strict per-side resolution rejects it (exit 2).
    let out = run_bin(&["diff", &a, &b, "--columns", "extra"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout should be empty on error");
    // The error must name which dataset rejected the column (left is resolved
    // first, so it is the one reported) rather than a bare "unknown column".
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(&format!("in {a}:")) && stderr.contains("unknown column 'extra'"),
        "error should name the dataset side: {stderr}"
    );
}

#[test]
fn dsdiff_rejects_lance_version_selectors() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b = write_idval(tmp.path(), "b.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    // A second dataset with a Lance selector is a mode conflict -> exit 2.
    let out = run_bin(&["diff", &a, &b, "--from", "1"]);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cannot combine a second dataset"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn dsdiff_missing_dataset_exit_two() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let missing = tmp.path().join("nope.lance");
    let out = run_bin(&["diff", &a, missing.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout should be empty on error");
}

#[test]
fn dsdiff_unsupported_format_exit_two() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let b = write_idval(tmp.path(), "b.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    let out = run_bin(&["diff", &a, &b, "--format", "csv"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("diff supports only"),
        "unexpected stderr"
    );
}

#[test]
fn diff_single_dataset_without_selector_errors() {
    let tmp = tempdir();
    let a = write_idval(tmp.path(), "a.lance", vec![1, 2, 3], vec!["x", "y", "z"]);
    // One dataset, no --from/--from-tag: lance-cli can't tell which comparison.
    let out = run_bin(&["diff", &a]);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("needs either a second dataset"),
        "unexpected stderr: {stderr}"
    );
}

// -------------------- --with-row-id / --with-row-addr (#21) --------------------
//
// These drive the real binary end-to-end (parsing jsonl stdout) so they exercise
// clap parsing, the projection interaction, and the dataset's scan/take paths.

/// Parse a successful command's jsonl stdout into one JSON object per row. The
/// `preserve_order` serde_json feature keeps object keys in column order, so the
/// key sequence doubles as an assertion on column position.
fn jsonl_rows(out: &std::process::Output) -> Vec<serde_json::Value> {
    assert!(
        out.status.success(),
        "command failed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).expect("valid jsonl row"))
        .collect()
}

fn row_keys(row: &serde_json::Value) -> Vec<String> {
    row.as_object()
        .expect("row is a json object")
        .keys()
        .cloned()
        .collect()
}

fn u64_field(row: &serde_json::Value, name: &str) -> u64 {
    row[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} missing/not u64 in {row}"))
}

#[test]
fn with_row_id_appends_rowid_last_and_counts_from_zero() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(&["head", "-n", "5", "--with-row-id"], &p));
    assert_eq!(rows.len(), 5);
    // Column position: `_rowid` is appended *after* the schema columns.
    assert_eq!(row_keys(&rows[0]), vec!["id", "name", "score", "_rowid"]);
    // Fresh single-fragment dataset: row ids equal the row offsets 0..5.
    let ids: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowid")).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

#[test]
fn both_flags_append_rowid_then_rowaddr() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(
        &["head", "-n", "1", "--with-row-id", "--with-row-addr"],
        &p,
    ));
    // Order is projected columns, then `_rowid`, then `_rowaddr`.
    assert_eq!(
        row_keys(&rows[0]),
        vec!["id", "name", "score", "_rowid", "_rowaddr"]
    );
}

#[test]
fn row_ids_consistent_across_commands_and_non_contiguous_after_deletion() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple_with_deletions(&tmp, "s").await });

    // After deleting ids 2 and 4, the survivors are ids 1, 3, 5.
    let head = jsonl_rows(&run_cli(&["head", "-n", "3", "--with-row-id"], &p));
    let take = jsonl_rows(&run_cli(
        &["take", "--indices", "0,1,2", "--with-row-id"],
        &p,
    ));

    let pairs = |rows: &[serde_json::Value]| -> Vec<(i64, u64)> {
        rows.iter()
            .map(|r| (r["id"].as_i64().unwrap(), u64_field(r, "_rowid")))
            .collect()
    };
    let head_pairs = pairs(&head);
    // Same rows via a different command produce the same `_rowid`s.
    assert_eq!(head_pairs, pairs(&take));

    let row_ids: Vec<u64> = head_pairs.iter().map(|(_, r)| *r).collect();
    // The deletion left a gap: the surviving row ids are non-contiguous. Three
    // rows spanning a range wider than 2 proves at least one id was removed.
    assert!(
        row_ids.windows(2).all(|w| w[0] < w[1]),
        "row ids should be strictly increasing: {row_ids:?}"
    );
    let span = row_ids.last().unwrap() - row_ids.first().unwrap();
    assert!(
        span > (row_ids.len() as u64 - 1),
        "expected a gap in {row_ids:?}"
    );
}

#[test]
fn with_row_id_survives_column_projection() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(
        &["head", "-n", "2", "--columns", "id", "--with-row-id"],
        &p,
    ));
    // Only the projected column, then the appended pseudo-column.
    assert_eq!(row_keys(&rows[0]), vec!["id", "_rowid"]);
    let ids: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowid")).collect();
    assert_eq!(ids, vec![0, 1]);
}

#[test]
fn with_row_id_survives_exclude_of_other_columns() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(
        &[
            "head",
            "-n",
            "1",
            "--exclude-columns",
            "name",
            "--with-row-id",
        ],
        &p,
    ));
    assert_eq!(row_keys(&rows[0]), vec!["id", "score", "_rowid"]);
}

#[test]
fn excluding_rowid_while_flagged_is_a_clean_error() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &[
            "head",
            "--format",
            "csv",
            "--exclude-columns",
            "_rowid",
            "--with-row-id",
        ],
        &p,
    );
    assert_clean_failure(&out, "cannot exclude the system column '_rowid'");
}

#[test]
fn tail_with_row_id_reports_trailing_row_ids() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(&["tail", "-n", "2", "--with-row-id"], &p));
    let ids: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowid")).collect();
    // Last two of five rows → offsets 3 and 4.
    assert_eq!(ids, vec![3, 4]);
    assert_eq!(row_keys(&rows[0]), vec!["id", "name", "score", "_rowid"]);
}

#[test]
fn sample_with_row_id_matches_each_rows_offset() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(
        &["sample", "-n", "3", "--seed", "1", "--with-row-id"],
        &p,
    ));
    assert_eq!(rows.len(), 3);
    // In this fixture id `i` sits at offset `i - 1`; the sampled `_rowid` must be
    // that row's own offset regardless of which rows the sampler drew.
    for row in &rows {
        let id = row["id"].as_i64().unwrap();
        assert_eq!(u64_field(row, "_rowid"), (id - 1) as u64);
    }
}

#[test]
fn take_with_row_addr_reports_addresses() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(
        &["take", "--indices", "0,2", "--with-row-addr"],
        &p,
    ));
    assert_eq!(row_keys(&rows[0]), vec!["id", "name", "score", "_rowaddr"]);
    // Single fragment 0: the row address equals the row offset.
    let addrs: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowaddr")).collect();
    assert_eq!(addrs, vec![0, 2]);
}

#[test]
fn cat_with_row_id_covers_every_row() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let rows = jsonl_rows(&run_cli(&["cat", "--with-row-id"], &p));
    let ids: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowid")).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

#[test]
fn take_with_nested_projection_and_row_id() {
    // Exercises `assemble_take_output`: a nested dotted projection is flattened
    // to leaf columns *and* the pseudo-column is appended in canonical position.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_struct(&tmp, "st").await });
    let rows = jsonl_rows(&run_cli(
        &[
            "take",
            "--indices",
            "0,2",
            "--columns",
            "meta.user.id,id",
            "--with-row-id",
        ],
        &p,
    ));
    // Nested leaf, then the other projected column, then `_rowid` last.
    assert_eq!(row_keys(&rows[0]), vec!["meta.user.id", "id", "_rowid"]);
    let row_ids: Vec<u64> = rows.iter().map(|r| u64_field(r, "_rowid")).collect();
    assert_eq!(row_ids, vec![0, 2]);
    // The flattened nested leaf still carries the right values (ids 10, 30).
    let user_ids: Vec<i64> = rows
        .iter()
        .map(|r| r["meta.user.id"].as_i64().unwrap())
        .collect();
    assert_eq!(user_ids, vec![10, 30]);
}

#[test]
fn row_id_is_stable_across_versions() {
    // Row ids assigned in v1 are not renumbered by a later append (the new rows
    // land in a fresh fragment), so reading either version reports the same
    // `_rowid` for the v1 rows.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple_two_versions(&tmp, "s").await });

    let v1 = jsonl_rows(&run_cli(&["cat", "--version", "1", "--with-row-id"], &p));
    let v2 = jsonl_rows(&run_cli(&["cat", "--version", "2", "--with-row-id"], &p));

    let pairs = |rows: &[serde_json::Value]| -> Vec<(i64, u64)> {
        rows.iter()
            .map(|r| (r["id"].as_i64().unwrap(), u64_field(r, "_rowid")))
            .collect()
    };
    let v1_pairs = pairs(&v1);
    assert_eq!(v1_pairs.len(), 5);
    // Every (id, _rowid) pair from v1 reappears unchanged among v2's rows.
    let v2_pairs = pairs(&v2);
    for pair in &v1_pairs {
        assert!(
            v2_pairs.contains(pair),
            "v1 pair {pair:?} missing from v2 {v2_pairs:?}"
        );
    }
}

#[test]
fn row_id_columns_render_in_csv() {
    // UInt64 pseudo-columns must pass CSV schema validation and render as plain
    // integers, with the header carrying them in appended order.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_simple(&tmp, "s").await });
    let out = run_cli(
        &[
            "head",
            "-n",
            "1",
            "--format",
            "csv",
            "--with-row-id",
            "--with-row-addr",
        ],
        &p,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut lines = stdout.lines();
    assert_eq!(lines.next().unwrap(), "id,name,score,_rowid,_rowaddr");
    assert_eq!(lines.next().unwrap(), "1,alice,10.5,0,0");
}
