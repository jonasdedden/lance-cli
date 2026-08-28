//! Integration tests for Arrow IPC stream output (`--format ipc`).
//!
//! The round-trip tests write a real Lance dataset, stream it out through the
//! IPC writer exactly as `cat`/`head` do, then read it back with the `arrow`
//! crate's `StreamReader` and assert the schema and batches survive losslessly
//! (including nested list, binary, and timestamp columns). The rejection tests
//! drive `dispatch` and assert `--format ipc` is refused where it does not
//! apply and when combined with the value-rendering flags.

mod common;

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use futures::StreamExt;
use lance_cli::cli::{BinaryFormat, Cli, Command, FilterArg, Format, RowIdArgs, VersionArgs};
use lance_cli::commands::dispatch;
use lance_cli::dataset::{self, ScanOptions};
use lance_cli::output::make_writer;
use lance_cli::output::table::TableStyle;
use tokio::runtime::Runtime;

use common::{tempdir, write_full};

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A `Cli` with everything defaulted except `format` and `command`, so the
/// rejection tests read as just the two axes under test.
fn cli(format: Format, command: Command) -> Cli {
    Cli {
        format: Some(format),
        binary_format: BinaryFormat::None,
        columns: None,
        exclude_columns: None,
        max_list_items: None,
        max_cell_width: None,
        float_precision: None,
        no_progress: false,
        command,
    }
}

/// Scan a dataset fully into memory, returning its projected batches. The IPC
/// writer emits exactly these batches, so reading the stream back must
/// reproduce them byte-for-byte.
async fn scan_all(path: &std::path::Path) -> (arrow_schema::SchemaRef, Vec<RecordBatch>) {
    let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();
    let schema = ds.arrow_schema();
    let mut stream = ds.scan(&ScanOptions::default()).await.unwrap();
    let mut batches = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b.unwrap());
    }
    (schema, batches)
}

/// Write `batches` (with `schema`) as an IPC stream, then read them back with
/// the arrow `StreamReader`, returning the reader's schema and decoded batches.
fn ipc_round_trip(
    schema: &arrow_schema::SchemaRef,
    batches: &[RecordBatch],
) -> (arrow_schema::SchemaRef, Vec<RecordBatch>) {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Ipc,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut buf),
        );
        w.start(schema).unwrap();
        for b in batches {
            w.write_batch(b).unwrap();
        }
        w.finish().unwrap();
    }
    let reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
    let out_schema = reader.schema();
    let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    (out_schema, out)
}

#[test]
fn ipc_round_trips_full_fixture_including_nested_binary_and_timestamps() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_full(&tmp, "full").await;
        let (schema, batches) = scan_all(&path).await;
        assert!(!batches.is_empty(), "fixture should yield rows");

        let (out_schema, out) = ipc_round_trip(&schema, &batches);
        assert_eq!(out_schema, schema, "schema must survive the IPC round-trip");
        assert_eq!(out, batches, "batches must survive the IPC round-trip");
    });
}

#[test]
fn ipc_empty_result_is_a_valid_readable_stream() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = write_full(&tmp, "full").await;
        let (schema, _) = scan_all(&path).await;

        // No batches at all — the equivalent of `head -n 0` / an empty match.
        let (out_schema, out) = ipc_round_trip(&schema, &[]);
        assert_eq!(out_schema, schema, "empty stream still carries the schema");
        assert!(out.is_empty(), "empty stream decodes to zero batches");
    });
}

#[test]
fn ipc_rejected_on_metadata_and_summary_commands() {
    runtime().block_on(async {
        // stats and freq compute their own row shapes; the issue scopes IPC to
        // the raw row-producing commands, so they reject it for now.
        let stats = dispatch(cli(
            Format::Ipc,
            Command::Stats {
                input: "does-not-matter".to_string(),
                filter: FilterArg::default(),
                version: VersionArgs::default(),
            },
        ))
        .await;
        assert!(matches!(
            stats,
            Err(lance_cli::error::Error::IpcNotApplicable { command: "stats" })
        ));

        let freq = dispatch(cli(
            Format::Ipc,
            Command::Freq {
                input: "does-not-matter".to_string(),
                column: "id".to_string(),
                limit: None,
                sort: lance_cli::cli::FreqSort::Count,
                filter: FilterArg::default(),
                version: VersionArgs::default(),
            },
        ))
        .await;
        assert!(matches!(
            freq,
            Err(lance_cli::error::Error::IpcNotApplicable { command: "freq" })
        ));

        let versions = dispatch(cli(
            Format::Ipc,
            Command::Versions {
                input: "does-not-matter".to_string(),
                branch: None,
                tagged_only: false,
            },
        ))
        .await;
        assert!(matches!(
            versions,
            Err(lance_cli::error::Error::IpcNotApplicable {
                command: "versions"
            })
        ));
    });
}

#[test]
fn ipc_rejected_on_diff() {
    runtime().block_on(async {
        let res = dispatch(cli(
            Format::Ipc,
            Command::Diff {
                input: "does-not-matter".to_string(),
                other: None,
                from: Some(0),
                from_tag: None,
                to: None,
                to_tag: None,
                branch: None,
            },
        ))
        .await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::DiffFormatUnsupported { format: "ipc" })
        ));
    });
}

#[test]
fn ipc_rejects_value_rendering_flags() {
    runtime().block_on(async {
        // --binary-format
        let mut c = cli(
            Format::Ipc,
            Command::Cat {
                inputs: vec!["does-not-matter".to_string()],
                filter: FilterArg::default(),
                row_ids: RowIdArgs::default(),
                version: VersionArgs::default(),
            },
        );
        c.binary_format = BinaryFormat::Hex;
        let res = dispatch(c).await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::IpcRenderingFlag {
                flag: "--binary-format"
            })
        ));

        // --float-precision
        let mut c = cli(
            Format::Ipc,
            Command::Cat {
                inputs: vec!["does-not-matter".to_string()],
                filter: FilterArg::default(),
                row_ids: RowIdArgs::default(),
                version: VersionArgs::default(),
            },
        );
        c.float_precision = Some(2);
        let res = dispatch(c).await;
        assert!(matches!(
            res,
            Err(lance_cli::error::Error::IpcRenderingFlag {
                flag: "--float-precision"
            })
        ));
    });
}
