use std::collections::VecDeque;

use arrow_array::RecordBatch;
use futures::StreamExt;

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{make_stdout_writer, prepare_row_id_columns, project_arrow_schema};
use crate::commands::progress::ScanProgress;
use crate::dataset::{self, LanceDataset, ScanOptions};
use crate::output::RenderOptions;
use crate::projection;
use crate::row_id::{self, RowIds};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    limit: u64,
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

    // Do all fail-able work (counting, scanning, buffering) before emitting the
    // header, so error paths — including an invalid `--where` — leave stdout
    // untouched.
    // Progress: the unfiltered fast path is a metadata `count_rows` + a single
    // `take` (no scan), so only the filtered streaming path gets an indicator.
    // The surviving-row total is unknown up front, so use a rows-scanned spinner.
    let progress = ScanProgress::new(show_progress && filter.is_some(), None);
    let batches = if limit == 0 {
        Vec::new()
    } else {
        match filter {
            // Without a filter the row count is known up front, so we can jump
            // straight to the last `N` rows with a positional `take`.
            None => tail_by_take(ds.as_ref(), limit, projection.as_deref(), row_ids).await?,
            // With a filter, positional indices no longer line up with the
            // matching rows, so stream the filtered rows and keep the tail.
            Some(pred) => {
                tail_by_stream(
                    ds.as_ref(),
                    limit,
                    projection.as_deref(),
                    pred,
                    &progress,
                    row_ids,
                )
                .await?
            }
        }
    };
    progress.finish();

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;
    for batch in &batches {
        writer.write_batch(batch)?;
    }
    writer.finish()?;
    Ok(())
}

/// Fast path: no filter, so `count_rows` + a single `take` of the trailing
/// indices is exact and cheap.
async fn tail_by_take(
    ds: &LanceDataset,
    limit: u64,
    projection: Option<&[String]>,
    row_ids: RowIds,
) -> Result<Vec<RecordBatch>> {
    let rowcount = ds.count_rows(None).await?;
    let take_count = limit.min(rowcount);
    if take_count == 0 {
        return Ok(Vec::new());
    }
    let start = rowcount - take_count;
    let indices: Vec<u64> = (start..rowcount).collect();
    let batch = ds.take(&indices, projection, row_ids).await?;
    Ok(vec![batch])
}

/// Filtered path: stream the matching rows, retaining only enough trailing
/// batches to cover the last `limit` rows. Memory is bounded to roughly
/// `limit` rows plus one batch rather than the whole (filtered) result set.
async fn tail_by_stream(
    ds: &LanceDataset,
    limit: u64,
    projection: Option<&[String]>,
    filter: &str,
    progress: &ScanProgress,
    row_ids: RowIds,
) -> Result<Vec<RecordBatch>> {
    let options = ScanOptions {
        projection,
        filter: Some(filter),
        row_ids,
    };
    let mut stream = progress.wrap(ds.scan(&options).await?);

    let mut buffered: VecDeque<RecordBatch> = VecDeque::new();
    let mut buffered_rows: u64 = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        buffered_rows += batch.num_rows() as u64;
        buffered.push_back(batch);
        // Drop leading batches that can't hold any of the last `limit` rows.
        while let Some(front) = buffered.front() {
            let front_rows = front.num_rows() as u64;
            if buffered_rows - front_rows >= limit {
                buffered_rows -= front_rows;
                buffered.pop_front();
            } else {
                break;
            }
        }
    }

    // Slice the first retained batch when the buffer overshoots, then keep the
    // trailing `limit` rows.
    let mut skip = buffered_rows - limit.min(buffered_rows);
    let mut out = Vec::with_capacity(buffered.len());
    for batch in buffered {
        let rows = batch.num_rows() as u64;
        if skip >= rows {
            skip -= rows;
            continue;
        }
        out.push(batch.slice(skip as usize, (rows - skip) as usize));
        skip = 0;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset;
    use crate::test_support::{collect_ids, write_int_fragments};

    // ids 0..=9 spread over three fragments → the filtered scan yields multiple
    // batches, exercising the batch-eviction loop and cross-batch slicing.
    const FRAGMENTS: &[&[i32]] = &[&[0, 1, 2, 3], &[4, 5, 6], &[7, 8, 9]];

    #[tokio::test]
    async fn keeps_last_matching_rows_across_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        // Even ids are [0, 2, 4, 6, 8]; the last three are [4, 6, 8].
        let batches = tail_by_stream(
            ds.as_ref(),
            3,
            None,
            "id % 2 = 0",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap();
        assert_eq!(collect_ids(&batches), vec![4, 6, 8]);
    }

    #[tokio::test]
    async fn limit_exceeding_matches_returns_all_matching() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let batches = tail_by_stream(
            ds.as_ref(),
            100,
            None,
            "id >= 7",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap();
        assert_eq!(collect_ids(&batches), vec![7, 8, 9]);
    }

    #[tokio::test]
    async fn empty_match_returns_no_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ds", FRAGMENTS).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let batches = tail_by_stream(
            ds.as_ref(),
            3,
            None,
            "id > 100",
            &ScanProgress::disabled(),
            RowIds::default(),
        )
        .await
        .unwrap();
        assert!(collect_ids(&batches).is_empty());
    }
}
