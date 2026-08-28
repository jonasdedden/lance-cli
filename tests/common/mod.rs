//! Shared fixtures for integration tests. Each test binary uses a different
//! subset of these helpers, so silence dead-code warnings per binary.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use tempfile::TempDir;

/// Build an arrow schema that exercises the common primitive types plus one list.
pub fn fixture_schema() -> SchemaRef {
    let list_field = Field::new_list(
        "tags",
        Arc::new(Field::new("item", DataType::Utf8, true)),
        true,
    );
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("data", DataType::Binary, true),
        list_field,
    ]))
}

/// Schema that only contains CSV-safe types.
pub fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

pub fn simple_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec![
        Some("alice"),
        Some("bob"),
        None,
        Some("dan"),
        Some("eve"),
    ]);
    let scores = Float64Array::from(vec![
        Some(10.5),
        None,
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(-1.25),
    ]);
    RecordBatch::try_new(
        simple_schema(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )
    .unwrap()
}

pub fn full_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec![Some("alice"), None, Some("carol")]);
    let scores = Float64Array::from(vec![Some(1.0), Some(2.5), None]);
    let flags = BooleanArray::from(vec![Some(true), Some(false), None]);
    let ts = TimestampMicrosecondArray::from(vec![
        Some(1_700_000_000_000_000),
        None,
        Some(1_700_000_001_000_000),
    ]);
    let data = BinaryArray::from_opt_vec(vec![Some(b"\x00\xff".as_ref()), None, Some(b"hi")]);

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("a");
    tags_builder.values().append_value("b");
    tags_builder.append(true);
    tags_builder.append(false);
    tags_builder.values().append_value("c");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    RecordBatch::try_new(
        fixture_schema(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(scores),
            Arc::new(flags),
            Arc::new(ts),
            Arc::new(data),
            Arc::new(tags),
        ],
    )
    .unwrap()
}

/// Write a dataset containing `simple_batch()` to a fresh Lance directory inside `tmp`.
pub async fn write_simple(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    let batch = simple_batch();
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance_cli::dataset::write_dataset(&path, iter)
        .await
        .unwrap();
    path
}

/// Write `simple_batch()` (ids 1..=5 in one fragment) and then delete the rows
/// with `id` 2 and 4, so the surviving rows (ids 1, 3, 5) have *non-contiguous*
/// `_rowid`s. Fixture for the row-id stability-across-deletion tests.
pub async fn write_simple_with_deletions(tmp: &TempDir, name: &str) -> PathBuf {
    let path = write_simple(tmp, name).await;
    let mut ds = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    ds.delete("id = 2 OR id = 4").await.unwrap();
    path
}

/// Write `simple_batch()` (ids 1..=5) as version 1, then append ids 6, 7, 8 as
/// version 2. The append lands in a new fragment, so the version-1 rows keep
/// their addresses — fixture for asserting `_rowid` stability across `--version`.
pub async fn write_simple_two_versions(tmp: &TempDir, name: &str) -> PathBuf {
    let path = write_simple(tmp, name).await;
    let mut ds = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let ids = Int32Array::from(vec![6, 7, 8]);
    let names = StringArray::from(vec![Some("fred"), Some("gwen"), Some("hank")]);
    let scores = Float64Array::from(vec![Some(6.0), Some(7.0), Some(8.0)]);
    let batch = RecordBatch::try_new(
        simple_schema(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )
    .unwrap();
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    ds.append(iter, None).await.unwrap();
    path
}

/// Write the full (all types) dataset.
pub async fn write_full(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    let batch = full_batch();
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance_cli::dataset::write_dataset(&path, iter)
        .await
        .unwrap();
    path
}

/// Schema with primitives + one top-level Binary column (no nested types).
/// Useful for testing --binary-format end-to-end without tripping CSV's
/// reject-nested-types rule.
pub fn with_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Binary, true),
    ]))
}

pub fn with_binary_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let data = BinaryArray::from_opt_vec(vec![Some(b"\x00\xff".as_ref()), None, Some(b"hi")]);
    RecordBatch::try_new(with_binary_schema(), vec![Arc::new(ids), Arc::new(data)]).unwrap()
}

pub async fn write_with_binary(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    let batch = with_binary_batch();
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance_cli::dataset::write_dataset(&path, iter)
        .await
        .unwrap();
    path
}

/// Fields of the nested `user` struct inside `meta`.
fn user_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ])
}

/// Fields of the top-level `meta` struct column.
fn meta_fields() -> Fields {
    Fields::from(vec![
        Field::new("user", DataType::Struct(user_fields()), true),
        Field::new("source", DataType::Utf8, true),
    ])
}

/// Schema exercising nested structs (`meta.user.id`, …), a non-struct scalar
/// (`score`) for bad-path tests, and a family of `emb_*` columns for globs.
pub fn struct_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Float64, true),
        Field::new("emb_0", DataType::Float64, true),
        Field::new("emb_1", DataType::Float64, true),
        Field::new("emb_2", DataType::Float64, true),
        Field::new("meta", DataType::Struct(meta_fields()), true),
    ]))
}

pub fn struct_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let scores = Float64Array::from(vec![Some(1.0), Some(2.5), None]);
    let emb0 = Float64Array::from(vec![Some(0.1), Some(0.2), Some(0.3)]);
    let emb1 = Float64Array::from(vec![Some(1.1), Some(1.2), Some(1.3)]);
    let emb2 = Float64Array::from(vec![Some(2.1), Some(2.2), Some(2.3)]);

    let user = StructArray::new(
        user_fields(),
        vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
        ],
        None,
    );
    let meta = StructArray::new(
        meta_fields(),
        vec![
            Arc::new(user),
            Arc::new(StringArray::from(vec![Some("web"), Some("api"), None])),
        ],
        None,
    );

    RecordBatch::try_new(
        struct_schema(),
        vec![
            Arc::new(ids),
            Arc::new(scores),
            Arc::new(emb0),
            Arc::new(emb1),
            Arc::new(emb2),
            Arc::new(meta),
        ],
    )
    .unwrap()
}

/// Write the nested-struct dataset to a fresh Lance directory inside `tmp`.
pub async fn write_struct(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    let batch = struct_batch();
    let schema = batch.schema();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance_cli::dataset::write_dataset(&path, iter)
        .await
        .unwrap();
    path
}

pub fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}
