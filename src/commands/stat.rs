//! `stat` — a one-screen dataset health summary.
//!
//! Unlike the per-column `stats` command (which scans the data), `stat` answers
//! "how is this dataset doing?" purely from manifest metadata: rows, deletions,
//! fragment spread, on-disk size, and version/branch/tag/index counts. It makes
//! **no** `scan()` call — every number comes from `LanceCapabilities` surfaces
//! that read the manifest — so it stays instant regardless of dataset size.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::common::{human_bytes, make_stdout_writer};
use crate::dataset::{self, FragmentInfo, IndexInfo, VersionInfo};
use crate::output::RenderOptions;

/// Fragment count at/above which the "many small fragments" hint may fire.
const MANY_FRAGMENTS: u64 = 100;
/// A fragment holding fewer physical rows than this is "small" for the hint.
/// Lance's default target is ~1M rows/fragment; 100k is a conservative tenth.
const SMALL_FRAGMENT_ROWS: u64 = 100_000;
/// Deleted-row ratio (deleted / physical) at/above which compaction is hinted.
const DELETED_RATIO_HINT: f64 = 0.10;

/// A dataset-level health summary in raw form. All numbers are unformatted;
/// jsonl emits them verbatim while table/csv format them for display.
#[derive(Debug, Clone)]
struct DatasetStat {
    path: String,
    manifest_version: u64,
    /// Live rows = physical rows minus tombstoned (deleted) rows.
    rows: u64,
    physical_rows: u64,
    deleted_rows: u64,
    columns: u64,
    num_fragments: u64,
    /// Fragment physical-row spread; `None` only when there are no fragments.
    fragment_min_rows: Option<u64>,
    fragment_max_rows: Option<u64>,
    fragment_median_rows: Option<u64>,
    /// Summed on-disk data-file size in bytes; `None` under `--no-size`.
    data_size_bytes: Option<u64>,
    num_versions: u64,
    latest_version_timestamp: Option<DateTime<Utc>>,
    num_branches: u64,
    num_tags: u64,
    /// `(name, index_type)` for every index, in listing order.
    indices: Vec<(String, String)>,
}

impl DatasetStat {
    /// Fraction of physical rows that are tombstoned, in `0.0..=1.0`. Zero when
    /// the dataset has no physical rows (ratio is undefined; reported as 0).
    fn deleted_ratio(&self) -> f64 {
        if self.physical_rows == 0 {
            0.0
        } else {
            self.deleted_rows as f64 / self.physical_rows as f64
        }
    }
}

pub async fn run(
    input: &str,
    version: &VersionArgs,
    no_size: bool,
    format: Format,
    render: RenderOptions,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;

    // All lookups are metadata-only and independent, so fire them concurrently.
    // `list_versions` is scoped to the selected branch so the version count
    // respects `--branch`; the other surfaces reflect the checked-out version.
    let (fragments, versions, branches, tags, indices) = futures::join!(
        ds.list_fragments(!no_size),
        ds.list_versions(version.branch.as_deref(), false),
        ds.list_branches(),
        ds.list_tags(),
        ds.list_indices(),
    );
    let fragments = fragments?;
    let versions = versions?;
    let branches = branches?;
    let tags = tags?;
    let indices = indices?;

    let stat = build_stat(
        input.to_string(),
        ds.manifest_version(),
        ds.arrow_schema().fields().len() as u64,
        &fragments,
        &versions,
        branches.len() as u64,
        tags.len() as u64,
        &indices,
        !no_size,
    );

    match format {
        Format::Jsonl => println!("{}", stat_json(&stat)),
        _ => print_table(&stat, format, render)?,
    }

    // In table mode, trail with a conservative plain-language compaction hint.
    if format == Format::Table
        && let Some(hint) = compaction_hint(&stat)
    {
        println!("note: {hint}");
    }
    Ok(())
}

/// Aggregate the metadata surfaces into a `DatasetStat`. Pure and side-effect
/// free so the numbers can be asserted directly in tests.
#[allow(clippy::too_many_arguments)]
fn build_stat(
    path: String,
    manifest_version: u64,
    columns: u64,
    fragments: &[FragmentInfo],
    versions: &[VersionInfo],
    num_branches: u64,
    num_tags: u64,
    indices: &[IndexInfo],
    with_size: bool,
) -> DatasetStat {
    let physical_rows: u64 = fragments.iter().map(|f| f.physical_rows).sum();
    let deleted_rows: u64 = fragments.iter().map(|f| f.deleted_rows).sum();
    let rows = physical_rows.saturating_sub(deleted_rows);

    let mut frag_rows: Vec<u64> = fragments.iter().map(|f| f.physical_rows).collect();
    frag_rows.sort_unstable();
    let (fragment_min_rows, fragment_max_rows, fragment_median_rows) = if frag_rows.is_empty() {
        (None, None, None)
    } else {
        (
            Some(frag_rows[0]),
            Some(frag_rows[frag_rows.len() - 1]),
            Some(median_u64(&frag_rows)),
        )
    };

    // Invariant: when `with_size` is set, `list_fragments(true)` populates
    // `size` for every fragment, so `filter_map` drops nothing and the sum is
    // complete. The debug assertion keeps a silent under-sum from creeping in
    // if that invariant ever breaks.
    let data_size_bytes = with_size.then(|| {
        debug_assert!(
            fragments.iter().all(|f| f.size.is_some()),
            "list_fragments(true) returned a fragment without a size"
        );
        fragments.iter().filter_map(|f| f.size).sum()
    });

    let latest_version_timestamp = versions
        .iter()
        .max_by_key(|v| v.version)
        .map(|v| v.timestamp);

    DatasetStat {
        path,
        manifest_version,
        rows,
        physical_rows,
        deleted_rows,
        columns,
        num_fragments: fragments.len() as u64,
        fragment_min_rows,
        fragment_max_rows,
        fragment_median_rows,
        data_size_bytes,
        num_versions: versions.len() as u64,
        latest_version_timestamp,
        num_branches,
        num_tags,
        indices: indices
            .iter()
            .map(|i| (i.name.clone(), i.index_type.clone()))
            .collect(),
    }
}

/// Median of a *sorted* non-empty slice, rounded up to the nearest whole row on
/// an even-length tie (so a `[3800, 3801]` pair reports `3801`).
fn median_u64(sorted: &[u64]) -> u64 {
    let n = sorted.len();
    if n.is_multiple_of(2) {
        (sorted[n / 2 - 1] + sorted[n / 2]).div_ceil(2)
    } else {
        sorted[n / 2]
    }
}

/// A conservative "should I compact?" hint, or `None` when nothing stands out.
/// Fires on many small fragments and/or a high deleted-row ratio.
fn compaction_hint(s: &DatasetStat) -> Option<String> {
    let mut reasons: Vec<&str> = Vec::new();
    if s.num_fragments >= MANY_FRAGMENTS
        && s.fragment_median_rows
            .is_some_and(|m| m < SMALL_FRAGMENT_ROWS)
    {
        reasons.push("many small fragments");
    }
    if s.deleted_ratio() >= DELETED_RATIO_HINT {
        reasons.push("high deleted-row ratio");
    }
    (!reasons.is_empty()).then(|| format!("{}; compaction would likely help", reasons.join(", ")))
}

/// Build the single stable-schema jsonl object with raw numeric values. Field
/// set is documented in the README and must stay stable for scripting.
fn stat_json(s: &DatasetStat) -> Value {
    json!({
        "path": s.path,
        "format": "lance",
        "manifest_version": s.manifest_version,
        "rows": s.rows,
        "physical_rows": s.physical_rows,
        "deleted_rows": s.deleted_rows,
        "deleted_ratio": s.deleted_ratio(),
        "columns": s.columns,
        "fragments": s.num_fragments,
        "fragment_min_rows": s.fragment_min_rows,
        "fragment_max_rows": s.fragment_max_rows,
        "fragment_median_rows": s.fragment_median_rows,
        "data_size_bytes": s.data_size_bytes,
        "versions": s.num_versions,
        "latest_version_timestamp": s
            .latest_version_timestamp
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
        "branches": s.num_branches,
        "tags": s.num_tags,
        "num_indices": s.indices.len(),
        "indices": s
            .indices
            .iter()
            .map(|(name, ty)| json!({ "name": name, "type": ty }))
            .collect::<Vec<_>>(),
        "compaction_hint": compaction_hint(s),
    })
}

/// Render the summary as a two-column key/value table (also used for csv, which
/// mirrors the table's human-readable pairs — use jsonl for raw numbers).
fn print_table(s: &DatasetStat, format: Format, render: RenderOptions) -> Result<()> {
    let rows = table_rows(s);
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let metric_col: Arc<dyn Array> = Arc::new(StringArray::from(
        rows.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
    ));
    let value_col: Arc<dyn Array> = Arc::new(StringArray::from(
        rows.iter().map(|(_, v)| v.as_str()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![metric_col, value_col])?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}

/// The ordered `(metric, value)` pairs shown in table/csv output.
fn table_rows(s: &DatasetStat) -> Vec<(String, String)> {
    vec![
        ("path".into(), s.path.clone()),
        (
            "format".into(),
            format!("lance (manifest version {})", s.manifest_version),
        ),
        ("rows".into(), group_digits(s.rows)),
        (
            "deleted rows".into(),
            format!(
                "{}  ({:.1}%)",
                group_digits(s.deleted_rows),
                s.deleted_ratio() * 100.0
            ),
        ),
        ("columns".into(), group_digits(s.columns)),
        ("fragments".into(), fragments_display(s)),
        (
            "data size".into(),
            match s.data_size_bytes {
                Some(bytes) => human_bytes(bytes),
                None => "n/a (--no-size)".into(),
            },
        ),
        ("versions".into(), versions_display(s)),
        ("branches".into(), group_digits(s.num_branches)),
        ("tags".into(), group_digits(s.num_tags)),
        ("indices".into(), indices_display(s)),
    ]
}

fn fragments_display(s: &DatasetStat) -> String {
    match (
        s.fragment_min_rows,
        s.fragment_max_rows,
        s.fragment_median_rows,
    ) {
        (Some(min), Some(max), Some(median)) => format!(
            "{}  (min {} rows, max {} rows, median {})",
            group_digits(s.num_fragments),
            group_digits(min),
            group_digits(max),
            group_digits(median),
        ),
        // No fragments: just the (zero) count.
        _ => group_digits(s.num_fragments),
    }
}

fn versions_display(s: &DatasetStat) -> String {
    match s.latest_version_timestamp {
        Some(ts) => format!(
            "{}  (latest {})",
            group_digits(s.num_versions),
            ts.to_rfc3339_opts(SecondsFormat::Secs, true),
        ),
        None => group_digits(s.num_versions),
    }
}

fn indices_display(s: &DatasetStat) -> String {
    if s.indices.is_empty() {
        return group_digits(0);
    }
    let list = s
        .indices
        .iter()
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}  ({list})", group_digits(s.indices.len() as u64))
}

/// Format an integer with `_` thousands separators (e.g. `1204000` → `1_204_000`)
/// for readable table/csv output. jsonl keeps raw digits.
fn group_digits(n: u64) -> String {
    let digits = n.to_string();
    let len = digits.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push('_');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frag(id: u64, physical: u64, deleted: u64, size: Option<u64>) -> FragmentInfo {
        FragmentInfo {
            id,
            physical_rows: physical,
            deleted_rows: deleted,
            num_files: 1,
            files: vec![format!("{id}.lance")],
            size,
        }
    }

    fn version(v: u64, ts: &str) -> VersionInfo {
        VersionInfo {
            version: v,
            timestamp: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            tag: None,
            message: None,
        }
    }

    fn index(name: &str, ty: &str) -> IndexInfo {
        IndexInfo {
            name: name.into(),
            index_type: ty.into(),
            uuid: "u".into(),
            columns: vec!["c".into()],
            dataset_version: 1,
            created_at: None,
        }
    }

    fn sample_stat() -> DatasetStat {
        let fragments = [
            frag(0, 100, 0, Some(1024)),
            frag(1, 300, 10, Some(2048)),
            frag(2, 200, 0, Some(1024)),
        ];
        let versions = [
            version(1, "2026-07-10T00:00:00Z"),
            version(2, "2026-07-18T09:12:44Z"),
        ];
        let indices = [index("emb_idx", "IVF_PQ"), index("id_idx", "BTREE")];
        build_stat(
            "/data/ds.lance".into(),
            2,
            14,
            &fragments,
            &versions,
            3,
            5,
            &indices,
            true,
        )
    }

    #[test]
    fn build_stat_aggregates_fragment_metadata() {
        let s = sample_stat();
        assert_eq!(s.physical_rows, 600);
        assert_eq!(s.deleted_rows, 10);
        assert_eq!(s.rows, 590);
        assert_eq!(s.num_fragments, 3);
        assert_eq!(s.fragment_min_rows, Some(100));
        assert_eq!(s.fragment_max_rows, Some(300));
        // Sorted [100, 200, 300] → median 200.
        assert_eq!(s.fragment_median_rows, Some(200));
        assert_eq!(s.data_size_bytes, Some(4096));
        assert_eq!(s.columns, 14);
        assert_eq!(s.num_versions, 2);
        assert_eq!(s.num_branches, 3);
        assert_eq!(s.num_tags, 5);
        assert_eq!(s.indices.len(), 2);
        // Latest is the highest-numbered version, not the last listed.
        assert_eq!(
            s.latest_version_timestamp
                .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
            Some("2026-07-18T09:12:44Z".to_string()),
        );
    }

    #[test]
    fn deleted_ratio_is_deleted_over_physical() {
        let s = sample_stat();
        // 10 / 600 = 0.01666…
        assert!((s.deleted_ratio() - 10.0 / 600.0).abs() < 1e-12);
    }

    #[test]
    fn empty_dataset_has_no_fragment_spread() {
        let s = build_stat("/empty.lance".into(), 1, 2, &[], &[], 1, 0, &[], true);
        assert_eq!(s.rows, 0);
        assert_eq!(s.physical_rows, 0);
        assert_eq!(s.deleted_rows, 0);
        assert_eq!(s.num_fragments, 0);
        assert_eq!(s.fragment_min_rows, None);
        assert_eq!(s.fragment_max_rows, None);
        assert_eq!(s.fragment_median_rows, None);
        // No physical rows → ratio is defined as 0, not NaN.
        assert_eq!(s.deleted_ratio(), 0.0);
        // Size was requested → empty sum is Some(0), not None.
        assert_eq!(s.data_size_bytes, Some(0));
        assert!(compaction_hint(&s).is_none());
    }

    #[test]
    fn no_size_leaves_size_absent() {
        let fragments = [frag(0, 100, 0, None)];
        let s = build_stat("/x.lance".into(), 1, 1, &fragments, &[], 1, 0, &[], false);
        assert_eq!(s.data_size_bytes, None);
        // jsonl reflects the absence as an explicit null.
        assert_eq!(stat_json(&s)["data_size_bytes"], Value::Null);
    }

    #[test]
    fn median_even_length_rounds_up() {
        assert_eq!(median_u64(&[100, 200]), 150);
        assert_eq!(median_u64(&[3800, 3801]), 3801);
        assert_eq!(median_u64(&[10, 20, 30]), 20);
        assert_eq!(median_u64(&[5]), 5);
    }

    #[test]
    fn group_digits_inserts_underscores() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(312), "312");
        assert_eq!(group_digits(12_400), "12_400");
        assert_eq!(group_digits(1_204_000), "1_204_000");
    }

    #[test]
    fn compaction_hint_fires_on_deleted_ratio() {
        let fragments = [frag(0, 100, 20, None)]; // 20% deleted
        let s = build_stat("/x.lance".into(), 1, 1, &fragments, &[], 1, 0, &[], false);
        let hint = compaction_hint(&s).expect("hint expected");
        assert!(hint.contains("high deleted-row ratio"), "{hint}");
        assert!(hint.contains("compaction would likely help"), "{hint}");
    }

    #[test]
    fn compaction_hint_fires_on_many_small_fragments() {
        let fragments: Vec<FragmentInfo> =
            (0..MANY_FRAGMENTS).map(|i| frag(i, 10, 0, None)).collect();
        let s = build_stat("/x.lance".into(), 1, 1, &fragments, &[], 1, 0, &[], false);
        let hint = compaction_hint(&s).expect("hint expected");
        assert!(hint.contains("many small fragments"), "{hint}");
    }

    #[test]
    fn compaction_hint_silent_on_healthy_dataset() {
        // Few large fragments, no deletions → no hint.
        let fragments = [frag(0, 500_000, 0, None), frag(1, 500_000, 0, None)];
        let s = build_stat("/x.lance".into(), 1, 1, &fragments, &[], 1, 0, &[], false);
        assert!(compaction_hint(&s).is_none());
    }

    #[test]
    fn jsonl_object_has_stable_field_set() {
        let s = sample_stat();
        let v = stat_json(&s);
        let obj = v.as_object().expect("json object");

        for key in [
            "path",
            "format",
            "manifest_version",
            "rows",
            "physical_rows",
            "deleted_rows",
            "deleted_ratio",
            "columns",
            "fragments",
            "fragment_min_rows",
            "fragment_max_rows",
            "fragment_median_rows",
            "data_size_bytes",
            "versions",
            "latest_version_timestamp",
            "branches",
            "tags",
            "num_indices",
            "indices",
            "compaction_hint",
        ] {
            assert!(obj.contains_key(key), "missing jsonl field: {key}");
        }
        assert_eq!(obj.len(), 20, "unexpected jsonl field count");

        // Raw numeric values, not human-formatted strings.
        assert_eq!(v["rows"], json!(590));
        assert_eq!(v["format"], json!("lance"));
        assert_eq!(v["data_size_bytes"], json!(4096));
        assert_eq!(v["num_indices"], json!(2));
        assert_eq!(v["indices"][0]["name"], json!("emb_idx"));
        assert_eq!(v["indices"][0]["type"], json!("IVF_PQ"));
        // Healthy sample → hint is null.
        assert_eq!(v["compaction_hint"], Value::Null);
    }

    #[test]
    fn table_rows_cover_every_metric() {
        let s = sample_stat();
        let rows = table_rows(&s);
        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "path",
                "format",
                "rows",
                "deleted rows",
                "columns",
                "fragments",
                "data size",
                "versions",
                "branches",
                "tags",
                "indices",
            ]
        );
        // Spot-check the composed display cells.
        let map: std::collections::HashMap<_, _> = rows.iter().cloned().collect();
        assert_eq!(map["rows"], "590");
        assert_eq!(map["format"], "lance (manifest version 2)");
        assert_eq!(
            map["fragments"],
            "3  (min 100 rows, max 300 rows, median 200)"
        );
        assert_eq!(map["indices"], "2  (emb_idx IVF_PQ, id_idx BTREE)");
        assert!(map["versions"].contains("latest 2026-07-18T09:12:44Z"));
    }

    #[test]
    fn no_size_table_shows_placeholder() {
        let fragments = [frag(0, 100, 0, None)];
        let s = build_stat("/x.lance".into(), 1, 1, &fragments, &[], 1, 0, &[], false);
        let map: std::collections::HashMap<_, _> = table_rows(&s).into_iter().collect();
        assert_eq!(map["data size"], "n/a (--no-size)");
    }
}
