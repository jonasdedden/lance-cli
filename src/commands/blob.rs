//! The `blob` command: extract one cell's binary/blob payload to a file or
//! stdout.
//!
//! Two extraction paths, chosen automatically:
//! - **Plain binary columns** (`Binary`/`LargeBinary`/`FixedSizeBinary`/
//!   `BinaryView`) are read with a single-row `Dataset::take` and the cell's
//!   bytes written out. These already materialize through a normal scan, so the
//!   whole cell is held in memory (one cell is small by construction).
//! - **Lance blob-encoded columns** (`lance-encoding:blob` field metadata) are
//!   read through the Lance blob API and *streamed* in bounded chunks, so a
//!   multi-GB payload never needs to be materialized at once.
//!
//! The global `--format`, `--columns`/`--exclude-columns` and `--binary-format`
//! flags do not apply here (the output is raw bytes, not row-shaped).
//! `--format` is a hard error, handled by `command_ignoring_format` in the
//! dispatcher — the same precedent `rowcount`/`schema` follow for commands that
//! don't emit rows. `--columns`/`--binary-format` are silently ignored, matching
//! how metadata commands treat inapplicable projection/rendering flags.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::Result;
use crate::cli::VersionArgs;
use crate::dataset::{self, BlobReader};
use crate::error::Error;
use crate::indices;
use crate::row_id::RowIds;

/// Bytes pulled per streaming read from a blob-encoded cell. Bounds peak memory
/// for arbitrarily large payloads.
const STREAM_CHUNK: usize = 1 << 20; // 1 MiB

/// A resolved single-cell payload, ready to write.
enum Payload {
    /// Fully materialized bytes from a plain binary column.
    Bytes(Vec<u8>),
    /// A streaming reader over a Lance blob-encoded cell.
    Blob(BlobReader),
}

pub async fn run(
    input: &str,
    column: &str,
    index: i64,
    output: Option<&Path>,
    version: &VersionArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    let schema = ds.arrow_schema();

    // Validate the column exists up front for a precise error, independent of
    // which extraction path we take.
    let field = schema.field_with_name(column).map_err(|_| {
        let available = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        Error::UnknownColumn {
            name: column.to_string(),
            available,
        }
    })?;

    let rowcount = ds.count_rows(None).await?;
    let idx = indices::resolve_index(index, rowcount)?;

    // Route blob-encoded columns to the streaming API; everything else goes
    // through the plain-binary `take` path. The blob check comes first because a
    // blob column's arrow type is (Large)Binary too — its metadata is what
    // distinguishes it.
    let payload = if ds.is_blob_column(column) {
        match ds.open_blob(column, idx).await? {
            Some(reader) => Payload::Blob(reader),
            None => {
                return Err(Error::NullBlobCell {
                    column: column.to_string(),
                    index: idx,
                });
            }
        }
    } else {
        // Plain binary column: validate the type, then take the single row.
        ensure_binary_type(column, field.data_type())?;
        let batch = ds
            .take(&[idx], Some(&[column.to_string()]), RowIds::default())
            .await?;
        let array = batch.column(0);
        match binary_cell_bytes(array, 0) {
            Some(bytes) => Payload::Bytes(bytes),
            None => {
                return Err(Error::NullBlobCell {
                    column: column.to_string(),
                    index: idx,
                });
            }
        }
    };

    write_payload(output, payload).await
}

/// Reject columns that aren't one of the four binary arrow types. Blob-encoded
/// columns never reach this check (they route to the streaming path first).
fn ensure_binary_type(column: &str, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok(()),
        other => Err(Error::NotBinaryColumn {
            column: column.to_string(),
            data_type: other.to_string(),
        }),
    }
}

/// Copy the bytes of one binary cell out of `array`, or `None` when the cell is
/// null. The column type was already validated by [`ensure_binary_type`].
fn binary_cell_bytes(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if array.is_null(row) {
        return None;
    }
    let bytes: &[u8] = match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(row),
        DataType::LargeBinary => array.as_binary::<i64>().value(row),
        DataType::BinaryView => array.as_binary_view().value(row),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row),
        // Unreachable: `ensure_binary_type` gates the four arms above.
        _ => return None,
    };
    Some(bytes.to_vec())
}

/// Dispatch the payload to `-o <file>` (via a temp file + atomic rename) or to
/// stdout (refused when stdout is a terminal).
async fn write_payload(output: Option<&Path>, payload: Payload) -> Result<()> {
    match output {
        None => {
            // Guard against dumping raw bytes into an interactive terminal.
            guard_not_terminal(std::io::stdout().is_terminal())?;
            let mut stdout = tokio::io::stdout();
            stream_into(&mut stdout, payload).await.map_err(Error::Io)?;
            stdout.flush().await.map_err(Error::Io)?;
            Ok(())
        }
        Some(path) => write_to_file(path, payload).await,
    }
}

/// Write to a sibling temp file, then rename it over `path` once the whole
/// payload is on disk. A failure mid-write removes the temp file, so an errored
/// extraction never leaves a partial or empty file at `path`. (Null-cell and
/// out-of-range errors are caught before this point, so the temp file is only
/// ever created for a payload that exists.)
async fn write_to_file(path: &Path, payload: Payload) -> Result<()> {
    let temp = temp_path(path);
    let mut file = tokio::fs::File::create(&temp)
        .await
        .map_err(|source| Error::BlobOutput {
            path: path.to_path_buf(),
            source,
        })?;

    let result = async {
        stream_into(&mut file, payload).await?;
        file.flush().await?;
        file.sync_all().await
    }
    .await;

    match result {
        Ok(()) => tokio::fs::rename(&temp, path)
            .await
            .map_err(|source| Error::BlobOutput {
                path: path.to_path_buf(),
                source,
            }),
        Err(source) => {
            // Best-effort cleanup; the original write error is what matters.
            let _ = tokio::fs::remove_file(&temp).await;
            Err(Error::BlobOutput {
                path: path.to_path_buf(),
                source,
            })
        }
    }
}

/// Build a temp path alongside `path` (same directory, so the rename stays on
/// one filesystem and is atomic). The pid keeps concurrent extractions from
/// colliding on the same target.
fn temp_path(path: &Path) -> PathBuf {
    let mut os = path.to_path_buf().into_os_string();
    os.push(format!(".lance-cli-blob-{}.tmp", std::process::id()));
    PathBuf::from(os)
}

/// Write `payload` into `w`, streaming blob payloads chunk-by-chunk so peak
/// memory stays bounded regardless of payload size.
async fn stream_into<W: AsyncWrite + Unpin>(w: &mut W, payload: Payload) -> std::io::Result<()> {
    match payload {
        Payload::Bytes(bytes) => w.write_all(&bytes).await,
        Payload::Blob(mut reader) => {
            loop {
                let chunk = reader
                    .read_chunk(STREAM_CHUNK)
                    .await
                    // A read failure from the object store surfaces as an IO
                    // error to the caller's write context.
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if chunk.is_empty() {
                    break;
                }
                w.write_all(&chunk).await?;
            }
            Ok(())
        }
    }
}

/// The TTY guard as a pure predicate so it is unit-testable without a real
/// terminal: refuses to emit raw bytes when stdout is interactive.
fn guard_not_terminal(is_terminal: bool) -> Result<()> {
    if is_terminal {
        Err(Error::BlobToTty)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tty_guard_refuses_terminal() {
        assert!(matches!(guard_not_terminal(true), Err(Error::BlobToTty)));
    }

    #[test]
    fn tty_guard_allows_non_terminal() {
        assert!(guard_not_terminal(false).is_ok());
    }

    #[test]
    fn non_binary_type_is_rejected() {
        assert!(matches!(
            ensure_binary_type("id", &DataType::Int32),
            Err(Error::NotBinaryColumn { .. })
        ));
    }

    #[test]
    fn binary_types_are_accepted() {
        for dt in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(4),
        ] {
            assert!(ensure_binary_type("payload", &dt).is_ok(), "{dt:?}");
        }
    }

    #[test]
    fn temp_path_is_a_sibling() {
        let p = Path::new("/tmp/out.png");
        let t = temp_path(p);
        assert_eq!(t.parent(), p.parent());
        assert_ne!(t, p);
        assert!(
            t.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("out.png")
        );
    }
}
