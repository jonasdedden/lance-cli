use std::io::Read as _;
use std::path::Path;

use futures::StreamExt;
use lance_index::vector::DIST_COL;

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::make_stdout_writer;
use crate::dataset::{self, VectorSearchParams};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::projection;

/// Where the query vector comes from. Exactly one is set (enforced by clap's
/// `query_vector` arg group).
pub enum QuerySource<'a> {
    /// Inline JSON array passed via `--vector`.
    Inline(&'a str),
    /// `--vector-file <path>`, or `--vector-file -` for stdin.
    File(&'a Path),
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    column: &str,
    source: QuerySource<'_>,
    k: usize,
    nprobes: Option<usize>,
    refine_factor: Option<u32>,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    version: &VersionArgs,
) -> Result<()> {
    let vector = parse_query_vector(source)?;

    let ds = dataset::open(input, Some(version)).await?;

    // `_distance` is always appended by the search, so tolerate the user naming
    // it explicitly in `--columns` (it isn't a real dataset column and would
    // otherwise trip the projection resolver). Drop it here; the search
    // re-adds it in the right place.
    let filtered_columns: Option<Vec<String>> = columns.map(|cols| {
        cols.iter()
            .filter(|c| c.as_str() != DIST_COL)
            .cloned()
            .collect()
    });
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, filtered_columns.as_deref(), exclude)?;

    let params = VectorSearchParams {
        column,
        vector: &vector,
        k,
        nprobes,
        refine_factor,
        projection: projection.as_deref(),
    };
    let result = ds.search(&params).await?;

    if !result.used_index {
        eprintln!(
            "note: no ANN index on column '{column}'; using flat (brute-force) KNN, which may be slow on large datasets"
        );
    }

    let mut writer = make_stdout_writer(format, render);
    writer.start(&result.schema)?;
    let mut stream = result.stream;
    while let Some(batch) = stream.next().await {
        writer.write_batch(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}

/// Read (from inline string, file, or stdin) and parse the query vector as a
/// JSON array of numbers, carried as f32 for handing to Lance.
fn parse_query_vector(source: QuerySource<'_>) -> Result<Vec<f32>> {
    let raw = match source {
        QuerySource::Inline(s) => s.to_string(),
        QuerySource::File(p) if p.as_os_str() == "-" => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
        QuerySource::File(p) => {
            std::fs::read_to_string(p).map_err(|source| Error::VectorFileRead {
                path: p.to_path_buf(),
                source,
            })?
        }
    };

    let values: Vec<f64> =
        serde_json::from_str(raw.trim()).map_err(|e| Error::VectorParse(e.to_string()))?;
    Ok(values.into_iter().map(|v| v as f32).collect())
}
