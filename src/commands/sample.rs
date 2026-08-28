use arrow_array::RecordBatch;
use futures::StreamExt;
use rand::SeedableRng;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{make_stdout_writer, prepare_row_id_columns, project_arrow_schema};
use crate::commands::progress::ScanProgress;
use crate::dataset::{self, LanceDataset, ScanOptions};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::projection;
use crate::row_id::{self, RowIds};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    limit: u64,
    seed: Option<u64>,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    row_ids: RowIds,
    version: &VersionArgs,
    show_progress: bool,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    let arrow_schema = ds.arrow_schema();
    let columns = prepare_row_id_columns(columns, exclude, row_ids)?;
    let projection = projection::resolve(&arrow_schema, columns.as_deref(), exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());
    let projected_schema = row_id::extend_schema(&projected_schema, row_ids);

    // Sample fully before emitting the header (both paths materialise their
    // result), so error paths — an oversize sample or an invalid `--where` —
    // leave stdout untouched.
    // Progress: the unfiltered fast path is a metadata `count_rows` + a single
    // `take` (no scan), so only the filtered reservoir path gets an indicator.
    // The matching-row total is unknown up front, so use a rows-scanned spinner.
    let progress = ScanProgress::new(show_progress && filter.is_some(), None);
    let output = match filter {
        // Without a filter the row count is known, so shuffle positional
        // indices and `take` the chosen rows in one shot.
        None => sample_by_index(ds.as_ref(), limit, seed, projection.as_deref(), row_ids).await?,
        // With a filter we can't know how many rows match without scanning, and
        // positional indices no longer address the matching rows — reservoir
        // sample over the filtered stream instead.
        Some(pred) => {
            sample_by_reservoir(
                ds.as_ref(),
                limit,
                seed,
                projection.as_deref(),
                pred,
                &progress,
                row_ids,
            )
            .await?
        }
    };
    progress.finish();

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;
    if let Some(batch) = output {
        writer.write_batch(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

/// Build the RNG the sampler draws from: deterministic when a seed is given,
/// otherwise seeded from the thread RNG. `ChaCha20Rng` in both arms keeps the
/// type uniform and `Send`.
fn make_rng(seed: Option<u64>) -> ChaCha20Rng {
    match seed {
        Some(s) => ChaCha20Rng::seed_from_u64(s),
        None => ChaCha20Rng::from_rng(&mut rand::rng()),
    }
}

/// No-filter fast path: pick `limit` distinct positions from `0..rowcount` and
/// materialise them with a single `take`.
async fn sample_by_index(
    ds: &LanceDataset,
    limit: u64,
    seed: Option<u64>,
    projection: Option<&[String]>,
    row_ids: RowIds,
) -> Result<Option<RecordBatch>> {
    let rowcount = ds.count_rows(None).await?;
    if limit > rowcount {
        return Err(Error::SampleTooLarge {
            requested: limit,
            rowcount,
        });
    }
    if limit == 0 {
        return Ok(None);
    }

    let mut pool: Vec<u64> = (0..rowcount).collect();
    let mut rng = make_rng(seed);
    pool.shuffle(&mut rng);
    pool.truncate(limit as usize);

    let batch = ds.take(&pool, projection, row_ids).await?;
    Ok(Some(batch))
}

/// Filtered path: reservoir-sample (Algorithm R) `limit` rows in a single pass
/// over the matching rows. The reservoir holds one-row slices, so peak memory
/// is bounded by `limit` (plus the source batches those slices reference)
/// rather than the whole filtered result set.
async fn sample_by_reservoir(
    ds: &LanceDataset,
    limit: u64,
    seed: Option<u64>,
    projection: Option<&[String]>,
    filter: &str,
    progress: &ScanProgress,
    row_ids: RowIds,
) -> Result<Option<RecordBatch>> {
    if limit == 0 {
        return Ok(None);
    }

    let options = ScanOptions {
        projection,
        filter: Some(filter),
        row_ids,
    };
    let mut stream = progress.wrap(ds.scan(&options).await?);

    let cap = limit as usize;
    let mut rng = make_rng(seed);
    let mut reservoir: Vec<RecordBatch> = Vec::with_capacity(cap);
    let mut seen: u64 = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for r in 0..batch.num_rows() {
            if reservoir.len() < cap {
                reservoir.push(batch.slice(r, 1));
            } else {
                // Keep the incoming row with probability cap/(seen+1); only
                // slice it out of the batch when it is actually retained.
                let j = rng.random_range(0..=seen);
                if (j as usize) < cap {
                    reservoir[j as usize] = batch.slice(r, 1);
                }
            }
            seen += 1;
        }
    }

    if limit > seen {
        return Err(Error::SampleTooLarge {
            requested: limit,
            rowcount: seen,
        });
    }

    if reservoir.is_empty() {
        return Ok(None);
    }
    let schema = reservoir[0].schema();
    let combined = arrow::compute::concat_batches(&schema, &reservoir)?;
    Ok(Some(combined))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset;
    use crate::test_support::{collect_ids, write_int_fragments};

    // ids 0..=9 across three fragments so the reservoir spans multiple batches.
    const FRAGMENTS: &[&[i32]] = &[&[0, 1, 2, 3], &[4, 5, 6], &[7, 8, 9]];

    #[tokio::test]
    async fn samples_only_matching_rows_and_is_reproducible() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        // Matching (even) ids are [0, 2, 4, 6, 8].
        let a = sample_by_reservoir(
            ds.as_ref(),
            3,
            Some(42),
            None,
            "id % 2 = 0",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap()
        .unwrap();
        let b = sample_by_reservoir(
            ds.as_ref(),
            3,
            Some(42),
            None,
            "id % 2 = 0",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap()
        .unwrap();
        let ids_a = collect_ids(std::slice::from_ref(&a));
        assert_eq!(ids_a, collect_ids(std::slice::from_ref(&b)), "reproducible");
        assert_eq!(ids_a.len(), 3);
        for id in ids_a {
            assert!(
                [0, 2, 4, 6, 8].contains(&id),
                "sampled non-matching id {id}"
            );
        }
    }

    #[tokio::test]
    async fn covers_all_matching_when_limit_equals_match_count() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let out = sample_by_reservoir(
            ds.as_ref(),
            5,
            Some(1),
            None,
            "id % 2 = 0",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut ids = collect_ids(std::slice::from_ref(&out));
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 2, 4, 6, 8]);
    }

    #[tokio::test]
    async fn errors_when_sample_larger_than_match_count() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        // Only one row matches, so a sample of 3 is impossible.
        let err = sample_by_reservoir(
            ds.as_ref(),
            3,
            Some(1),
            None,
            "id = 0",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            Error::SampleTooLarge {
                requested: 3,
                rowcount: 1
            }
        ));
    }

    #[tokio::test]
    async fn empty_match_yields_no_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        // Zero matches with a positive limit is "too large" (0 available rows).
        let err = sample_by_reservoir(
            ds.as_ref(),
            1,
            Some(1),
            None,
            "id > 100",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            Error::SampleTooLarge {
                requested: 1,
                rowcount: 0
            }
        ));
    }
}
