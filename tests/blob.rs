//! End-to-end integration tests for the `blob` command.
//!
//! Two fixture shapes are exercised:
//! - A plain `LargeBinary` column (`payload`) with a known byte pattern and a
//!   null cell, to prove byte-identical extraction via both stdout and `-o`.
//! - A Lance blob-encoded column (`lance-encoding:blob` field metadata) whose
//!   payload is read through the streaming blob API.
//!
//! Extraction is driven through the real `lance-cli` binary so the stdout/`-o`,
//! TTY-guard and exit-code behaviour is covered exactly as a user sees it.
//! (The tests run piped, so stdout is never a terminal and the guard's happy
//! path is what executes; the guard predicate itself is unit-tested in
//! `src/commands/blob.rs`.)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use arrow_array::{Int32Array, LargeBinaryArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset as LanceInner;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Lance's blob field-metadata key (`lance_arrow::BLOB_META_KEY`), inlined to
/// avoid a dev-dependency on `lance-arrow` just for this constant.
const BLOB_META_KEY: &str = "lance-encoding:blob";

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// Distinct known payloads including embedded NUL and high bytes, so a
/// byte-identical comparison actually exercises binary-safe I/O.
fn payloads() -> Vec<Vec<u8>> {
    vec![
        b"\x00\x01\x02\xff\xfePNG-ish".to_vec(),
        (0u16..512).map(|n| (n % 256) as u8).collect(),
        b"last".to_vec(),
    ]
}

/// Write a dataset with an `id` column and a plain `LargeBinary` `payload`
/// column. Row 1 is null; rows 0/2/3 carry `payloads()`.
async fn write_binary_fixture(tmp: &TempDir, name: &str) -> PathBuf {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::LargeBinary, true),
    ]));
    let p = payloads();
    let payload = LargeBinaryArray::from_opt_vec(vec![
        Some(p[0].as_slice()),
        None,
        Some(p[1].as_slice()),
        Some(p[2].as_slice()),
    ]);
    let ids = Int32Array::from(vec![0, 1, 2, 3]);
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(payload)]).unwrap();

    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();
    let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
    LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    path
}

/// Write a dataset with a Lance blob-encoded column: a `LargeBinary` field
/// tagged with the `lance-encoding:blob` metadata. Row 1 is null. Payloads
/// mirror the plain fixture so the streaming path is checked against known
/// bytes.
async fn write_blob_fixture(tmp: &TempDir, name: &str) -> PathBuf {
    let mut meta = HashMap::new();
    meta.insert(BLOB_META_KEY.to_string(), "true".to_string());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("blob", DataType::LargeBinary, true).with_metadata(meta),
    ]));
    let p = payloads();
    let blob = LargeBinaryArray::from_opt_vec(vec![
        Some(p[0].as_slice()),
        None,
        Some(p[1].as_slice()),
        Some(p[2].as_slice()),
    ]);
    let ids = UInt64Array::from(vec![0, 1, 2, 3]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(blob)]).unwrap();

    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();
    let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
    LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    path
}

/// A ~2.5 MiB deterministic payload. Larger than the command's 1 MiB streaming
/// chunk, so extracting it exercises the multi-chunk read/write loop (three
/// chunks: 1 MiB, 1 MiB, ~0.5 MiB, then EOF). Generated from a formula so the
/// byte-identical comparison stays exact without embedding the bytes.
fn large_payload() -> Vec<u8> {
    (0..2_621_440u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

/// Write a blob-encoded dataset whose single row carries [`large_payload`].
async fn write_large_blob_fixture(tmp: &TempDir, name: &str) -> PathBuf {
    let mut meta = HashMap::new();
    meta.insert(BLOB_META_KEY.to_string(), "true".to_string());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("blob", DataType::LargeBinary, true).with_metadata(meta),
    ]));
    let payload = large_payload();
    let blob = LargeBinaryArray::from_opt_vec(vec![Some(payload.as_slice())]);
    let ids = UInt64Array::from(vec![0u64]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(blob)]).unwrap();

    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();
    let iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
    LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    path
}

/// Spawn the real binary: `lance-cli blob <args...> <path>`.
fn run_blob(args: &[&str], path: &Path) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lance-cli"))
        .arg("blob")
        .args(args)
        .arg(path)
        .output()
        .expect("spawn lance-cli binary")
}

// ------------------------------ plain binary -------------------------------

#[test]
fn binary_column_stdout_is_byte_identical() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let out = run_blob(&["--column", "payload", "--index", "0"], &p);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.stdout, payloads()[0]);
}

#[test]
fn binary_column_output_file_is_byte_identical() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let dest = tmp.path().join("out.bin");
    let out = run_blob(
        &[
            "--column",
            "payload",
            "--index",
            "2",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty(), "nothing should go to stdout with -o");
    let written = std::fs::read(&dest).unwrap();
    assert_eq!(written, payloads()[1]);
}

#[test]
fn binary_negative_index_is_last_row() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let out = run_blob(&["--column", "payload", "--index", "-1"], &p);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.stdout, payloads()[2]);
}

#[test]
fn null_cell_errors_and_writes_no_file() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let dest = tmp.path().join("should-not-exist.bin");
    let out = run_blob(
        &[
            "--column",
            "payload",
            "--index",
            "1",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("is null"), "stderr: {stderr}");
    assert!(!dest.exists(), "no file must be created on a null cell");
}

#[test]
fn non_binary_column_errors() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let out = run_blob(&["--column", "id", "--index", "0"], &p);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not a binary column") || stderr.contains("blob cannot extract"),
        "stderr: {stderr}"
    );
}

#[test]
fn out_of_range_index_errors() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let out = run_blob(&["--column", "payload", "--index", "99"], &p);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("out of range"), "stderr: {stderr}");
}

#[test]
fn unknown_column_errors() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let out = run_blob(&["--column", "nope", "--index", "0"], &p);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unknown column"), "stderr: {stderr}");
}

// --------------------------- blob-encoded column ---------------------------

#[test]
fn blob_column_stdout_is_byte_identical() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_blob_fixture(&tmp, "blobds").await });
    let out = run_blob(&["--column", "blob", "--index", "0"], &p);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.stdout, payloads()[0]);
}

#[test]
fn blob_column_output_file_streams_byte_identical() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_blob_fixture(&tmp, "blobds").await });
    let dest = tmp.path().join("blob-out.bin");
    // Index 2 is the 512-byte payload — larger, so the chunked streaming loop
    // runs more than a trivial single read even though the payload is small.
    let out = run_blob(
        &[
            "--column",
            "blob",
            "--index",
            "2",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let written = std::fs::read(&dest).unwrap();
    assert_eq!(written, payloads()[1]);
}

#[test]
fn blob_column_negative_index() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_blob_fixture(&tmp, "blobds").await });
    let out = run_blob(&["--column", "blob", "--index", "-1"], &p);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.stdout, payloads()[2]);
}

#[test]
fn blob_column_null_cell_errors() {
    let tmp = tempdir();
    let p = runtime().block_on(async { write_blob_fixture(&tmp, "blobds").await });
    let dest = tmp.path().join("nope.bin");
    let out = run_blob(
        &[
            "--column",
            "blob",
            "--index",
            "1",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("is null"), "stderr: {stderr}");
    assert!(!dest.exists());
}

#[test]
fn blob_column_multi_mib_streams_byte_identical() {
    // Locks in the multi-chunk streaming path: a ~2.5 MiB blob payload must
    // extract byte-for-byte, proving the 1 MiB chunk loop reads and writes every
    // chunk (and terminates) rather than truncating at the first read.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_large_blob_fixture(&tmp, "bigblob").await });
    let dest = tmp.path().join("big-out.bin");
    let out = run_blob(
        &[
            "--column",
            "blob",
            "--index",
            "0",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let written = std::fs::read(&dest).unwrap();
    let expected = large_payload();
    assert_eq!(written.len(), expected.len());
    assert_eq!(written, expected);
}

// ------------------------------ global flags -------------------------------

#[test]
fn format_flag_is_rejected() {
    // `blob` emits raw bytes, not rows, so `--format` is a hard error (the same
    // precedent `rowcount`/`schema` follow), not a silent no-op.
    let tmp = tempdir();
    let p = runtime().block_on(async { write_binary_fixture(&tmp, "bin").await });
    let dest = tmp.path().join("out.bin");
    let out = run_blob(
        &[
            "--column",
            "payload",
            "--index",
            "0",
            "--format",
            "csv",
            "-o",
            dest.to_str().unwrap(),
        ],
        &p,
    );
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not applicable to 'blob'"),
        "stderr: {stderr}"
    );
    // The flag is rejected before any extraction, so no output file is created.
    assert!(!dest.exists());
}
