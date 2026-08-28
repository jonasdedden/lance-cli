use std::path::Path;

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{
    make_stdout_writer, prepare_row_id_columns, project_arrow_schema, schemas_match,
};
use crate::commands::progress::ScanProgress;
use crate::dataset::{self, ScanOptions};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::projection;
use futures::StreamExt;

use crate::row_id::{self, RowIds};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    inputs: &[String],
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    row_ids: RowIds,
    version: &VersionArgs,
    show_progress: bool,
) -> Result<()> {
    if inputs.is_empty() {
        return Err(Error::EmptyInputs);
    }
    // Expand shell-unexpanded glob patterns before opening anything, so a
    // no-match pattern errors here (exit 2) rather than surfacing as an obscure
    // open failure.
    let inputs = expand_inputs(inputs)?;

    let mut opened = Vec::with_capacity(inputs.len());
    for path in &inputs {
        opened.push(dataset::open(path, Some(version)).await?);
    }

    let first_schema = opened[0].arrow_schema();
    let columns = prepare_row_id_columns(columns, exclude, row_ids)?;
    let projection = projection::resolve(&first_schema, columns.as_deref(), exclude)?;
    let projected_schema = project_arrow_schema(first_schema.as_ref(), projection.as_deref());
    let projected_schema = row_id::extend_schema(&projected_schema, row_ids);

    for (ds, path) in opened.iter().zip(inputs.iter()).skip(1) {
        let other = ds.arrow_schema();
        if let Err(field) = schemas_match(&first_schema, &other) {
            return Err(Error::SchemaMismatch {
                left: inputs[0].clone(),
                right: path.clone(),
                field,
            });
        }
    }

    let options = ScanOptions {
        projection: projection.as_deref(),
        filter,
        row_ids,
    };

    // Progress: `cat` always scans. With no filter the row total is cheap to
    // learn (Lance answers `count_rows` from metadata), so show a bar with an
    // ETA; a filter makes the surviving-row total unknown up front, so fall back
    // to a rows-scanned spinner.
    let total = if show_progress && filter.is_none() {
        let mut total = 0u64;
        for ds in &opened {
            total += ds.count_rows(None).await?;
        }
        Some(total)
    } else {
        None
    };
    let progress = ScanProgress::new(show_progress, total);

    // Open every scan first: the scan validates the predicate eagerly, so a
    // bad `--where` errors here, before we emit the output header to stdout.
    let mut streams = Vec::with_capacity(opened.len());
    for ds in &opened {
        streams.push(progress.wrap(ds.scan(&options).await?));
    }

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;
    for mut stream in streams {
        while let Some(batch) = stream.next().await {
            writer.write_batch(&batch?)?;
        }
    }
    writer.finish()?;
    progress.finish();
    Ok(())
}

/// Metacharacters that make an input a glob pattern rather than a literal path.
fn is_glob_pattern(input: &str) -> bool {
    input.contains(['*', '?', '['])
}

/// Expand shell-unexpanded glob patterns in `cat`'s inputs.
///
/// Rules, applied per input:
/// * Remote URIs (anything with a `scheme://`) pass through untouched — glob
///   expansion is local-filesystem only.
/// * A literal path that already exists is used as-is, even when it contains
///   glob metacharacters (a dataset directory really named `weird[1].lance`).
/// * Otherwise a scheme-less input containing `*`/`?`/`[` is expanded against
///   the local filesystem; matches are sorted lexicographically for
///   deterministic concatenation order, and a no-match pattern is an error.
/// * A scheme-less input without glob metacharacters passes through so the
///   normal open path produces its usual not-found / unknown-format error.
fn expand_inputs(inputs: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(inputs.len());
    for input in inputs {
        if dataset::has_scheme(input) || Path::new(input).exists() || !is_glob_pattern(input) {
            out.push(input.clone());
            continue;
        }

        let entries = glob::glob(input).map_err(|e| Error::InvalidGlobPattern {
            pattern: input.clone(),
            message: e.to_string(),
        })?;
        let mut matched: Vec<String> = Vec::new();
        for entry in entries {
            let path = entry.map_err(|e| Error::InvalidGlobPattern {
                pattern: input.clone(),
                message: e.to_string(),
            })?;
            matched.push(path.to_string_lossy().into_owned());
        }
        if matched.is_empty() {
            return Err(Error::InputGlobNoMatch {
                pattern: input.clone(),
            });
        }
        // Lexicographic order so concatenation is deterministic regardless of
        // the filesystem's directory iteration order.
        matched.sort();
        out.extend(matched);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create empty marker files/dirs under `dir` and return the dir path.
    fn touch(dir: &Path, name: &str) {
        std::fs::create_dir_all(dir.join(name)).unwrap();
    }

    #[test]
    fn glob_expands_multiple_matches_in_lexicographic_order() {
        let tmp = tempfile::tempdir().unwrap();
        // Create out of lexical order to prove the sort, not the FS order.
        touch(tmp.path(), "part_c.lance");
        touch(tmp.path(), "part_a.lance");
        touch(tmp.path(), "part_b.lance");
        let pattern = tmp
            .path()
            .join("part_*.lance")
            .to_string_lossy()
            .into_owned();

        let out = expand_inputs(&[pattern]).unwrap();
        let names: Vec<_> = out
            .iter()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["part_a.lance", "part_b.lance", "part_c.lance"]);
    }

    #[test]
    fn glob_no_match_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let pattern = tmp
            .path()
            .join("nomatch_*.lance")
            .to_string_lossy()
            .into_owned();
        let err = expand_inputs(std::slice::from_ref(&pattern)).unwrap_err();
        assert!(
            matches!(err, Error::InputGlobNoMatch { pattern: ref p } if *p == pattern),
            "expected InputGlobNoMatch, got {err:?}"
        );
    }

    #[test]
    fn literal_path_with_glob_chars_that_exists_is_used_as_is() {
        let tmp = tempfile::tempdir().unwrap();
        // A directory whose real name contains glob metacharacters.
        touch(tmp.path(), "weird[1].lance");
        let literal = tmp
            .path()
            .join("weird[1].lance")
            .to_string_lossy()
            .into_owned();

        let out = expand_inputs(std::slice::from_ref(&literal)).unwrap();
        // Passed through verbatim rather than treated as a `[1]` character class.
        assert_eq!(out, vec![literal]);
    }

    #[test]
    fn remote_uris_pass_through_untouched() {
        // Contains a `*`, but a remote URI must never be globbed locally.
        let uris = vec![
            "s3://bucket/part_*.lance".to_string(),
            "gs://analytics/events.lance".to_string(),
        ];
        let out = expand_inputs(&uris).unwrap();
        assert_eq!(out, uris);
    }

    #[test]
    fn scheme_less_non_glob_missing_path_passes_through() {
        // No glob chars and does not exist: passed through so `open` reports it.
        let out = expand_inputs(&["/no/such/dataset.lance".to_string()]).unwrap();
        assert_eq!(out, vec!["/no/such/dataset.lance".to_string()]);
    }
}
