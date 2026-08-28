//! `freq` — value counts for a single column.
//!
//! Streams the column through a projected scan, accumulating one count per
//! distinct value in a `HashMap`, then renders `value / count / percent` rows
//! through the same metadata-table pathway the Lance listing commands use (so
//! `--format table|jsonl|csv` all work).
//!
//! Each value is keyed by its CSV rendering (`output::value`), so the display
//! string and the map key are one and the same and match how the value would
//! appear in `cat --format csv`. NULLs are counted separately and surface as an
//! explicit `NULL` row. When `-n/--limit` truncates the list, the dropped
//! values' mass is folded into a trailing `<other>` row.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;

use crate::Result;
use crate::cli::{Format, FreqSort, VersionArgs};
use crate::commands::common::make_stdout_writer;
use crate::commands::progress::ScanProgress;
use crate::dataset::{self, LanceDataset, ScanOptions};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::output::value::csv_cell;

/// Distinct-value ceiling. Accumulation is O(cardinality) in memory, so a
/// high-cardinality column (ids, UUIDs, free text) would otherwise silently
/// balloon RAM. Past this many distinct values we bail with a helpful error.
const MAX_DISTINCT: usize = 1_000_000;

/// Literal shown in the `value` column for the null category.
const NULL_LABEL: &str = "NULL";
/// Literal shown in the `value` column for the truncated remainder.
const OTHER_LABEL: &str = "<other>";

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    column: &str,
    limit: Option<u64>,
    sort: FreqSort,
    format: Format,
    render: RenderOptions,
    filter: Option<&str>,
    version: &VersionArgs,
    show_progress: bool,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;

    // Progress: freq scans the whole (optionally filtered) column. Without a
    // filter the row total is a cheap metadata `count_rows`, so show a bar with
    // an ETA; with a filter the surviving-row total is unknown, so use a
    // rows-scanned spinner.
    let total = if show_progress && filter.is_none() {
        Some(ds.count_rows(None).await?)
    } else {
        None
    };
    let progress = ScanProgress::new(show_progress, total);

    // Compute the full table before touching stdout: validation, an invalid
    // `--where`, or the cardinality guard must fail before the writer emits a
    // header (mirrors the other buffered commands' stdout hygiene).
    let batch = compute(
        ds.as_ref(),
        &progress,
        column,
        limit,
        sort,
        filter,
        render,
        MAX_DISTINCT,
    )
    .await?;
    progress.finish();

    let mut writer = make_stdout_writer(format, render);
    writer.start(&batch.schema())?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}

/// Open-free core: validate the column, accumulate counts over the (optionally
/// filtered) scan, and materialise the `value / count / percent` batch. Split
/// out from `run` so tests can exercise it and render it in every format.
#[allow(clippy::too_many_arguments)]
async fn compute(
    ds: &LanceDataset,
    progress: &ScanProgress,
    column: &str,
    limit: Option<u64>,
    sort: FreqSort,
    filter: Option<&str>,
    render: RenderOptions,
    max_distinct: usize,
) -> Result<RecordBatch> {
    let schema = ds.arrow_schema();
    let field = schema
        .field_with_name(column)
        .map_err(|_| Error::UnknownColumn {
            name: column.to_string(),
            available: schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
        })?;
    validate_freq_type(column, field.data_type())?;
    // The scan knows the column type, so ordering can key on the value's real
    // (numeric / lexical) order rather than its rendered string.
    let ordering = OrderMode::of(field.data_type());

    let counts = accumulate(ds, progress, column, filter, render, max_distinct).await?;
    build_batch(counts, sort, limit, ordering)
}

/// Accumulated value counts for one column.
struct Counts {
    /// Rendered value -> occurrences.
    present: HashMap<String, u64>,
    /// Number of null rows.
    null: u64,
    /// Total rows scanned (`null` + sum of `present`). Used as the percent base.
    total: u64,
}

/// Stream the single projected column and tally occurrences.
async fn accumulate(
    ds: &LanceDataset,
    progress: &ScanProgress,
    column: &str,
    filter: Option<&str>,
    render: RenderOptions,
    max_distinct: usize,
) -> Result<Counts> {
    let projection = [column.to_string()];
    let options = ScanOptions {
        projection: Some(&projection),
        filter,
        ..Default::default()
    };
    let mut stream = progress.wrap(ds.scan(&options).await?);

    let mut present: HashMap<String, u64> = HashMap::new();
    let mut null: u64 = 0;
    let mut total: u64 = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let array = batch.column(0);
        for row in 0..array.len() {
            total += 1;
            match csv_cell(array.as_ref(), row, render)? {
                None => null += 1,
                Some(key) => match present.get_mut(&key) {
                    Some(c) => *c += 1,
                    None => {
                        if present.len() >= max_distinct {
                            return Err(Error::CardinalityExceeded {
                                column: column.to_string(),
                                limit: max_distinct,
                            });
                        }
                        present.insert(key, 1);
                    }
                },
            }
        }
    }

    Ok(Counts {
        present,
        null,
        total,
    })
}

/// One output row's value identity, kept distinct from its rendered string so
/// the null category orders deterministically (always last) without colliding
/// with a literal `"NULL"` string value.
enum EntryKey {
    Present(String),
    Null,
}

/// Turn the accumulated counts into the `value / count / percent` batch,
/// applying the requested sort and optional `-n` truncation.
fn build_batch(
    counts: Counts,
    sort: FreqSort,
    limit: Option<u64>,
    ordering: OrderMode,
) -> Result<RecordBatch> {
    let Counts {
        present,
        null,
        total,
    } = counts;

    let mut entries: Vec<(EntryKey, u64)> = present
        .into_iter()
        .map(|(k, c)| (EntryKey::Present(k), c))
        .collect();
    if null > 0 {
        entries.push((EntryKey::Null, null));
    }

    entries.sort_by(|a, b| cmp_entries(a, b, sort, ordering));

    // Truncate to the top N, folding everything dropped into `<other>`.
    let mut other: u64 = 0;
    if let Some(n) = limit {
        let n = n as usize;
        if entries.len() > n {
            for (_, c) in entries.drain(n..) {
                other += c;
            }
        }
    }

    let cap = entries.len() + usize::from(other > 0);
    let mut values: Vec<String> = Vec::with_capacity(cap);
    let mut count_col: Vec<u64> = Vec::with_capacity(cap);
    let mut percent_col: Vec<String> = Vec::with_capacity(cap);

    for (key, count) in &entries {
        values.push(match key {
            EntryKey::Present(s) => s.clone(),
            EntryKey::Null => NULL_LABEL.to_string(),
        });
        count_col.push(*count);
        percent_col.push(percent(*count, total));
    }
    if other > 0 {
        values.push(OTHER_LABEL.to_string());
        count_col.push(other);
        percent_col.push(percent(other, total));
    }

    let schema = freq_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(values)),
            Arc::new(UInt64Array::from(count_col)),
            Arc::new(StringArray::from(percent_col)),
        ],
    )?;
    Ok(batch)
}

/// How a column's values order against each other. Values are keyed in the
/// count map by their CSV rendering, but ordering a rendered string
/// lexicographically is wrong for numbers (`"10" < "2"`, `"-1" < "-2"`), so the
/// mode says how to interpret the rendering when comparing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderMode {
    /// Signed/unsigned integers: compare the parsed integer value.
    Integer,
    /// Floating point: compare numerically, NaN last.
    Float,
    /// Decimals: compare the parsed decimal magnitude (may exceed f64 range).
    Decimal,
    /// Strings, booleans, and temporal types: the rendering already sorts
    /// correctly lexicographically (ISO-8601 is chronological, bools `false` <
    /// `true`), so compare the strings directly.
    Lexical,
}

impl OrderMode {
    fn of(ty: &DataType) -> Self {
        use DataType::*;
        match ty {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Self::Integer,
            Float16 | Float32 | Float64 => Self::Float,
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _) => {
                Self::Decimal
            }
            Dictionary(_, value_ty) => Self::of(value_ty),
            _ => Self::Lexical,
        }
    }
}

/// Compare two entries under the active sort. `count` sorts by frequency
/// descending with the value as a deterministic tie-break; `value` sorts purely
/// by value. Both place NULL last.
fn cmp_entries(
    a: &(EntryKey, u64),
    b: &(EntryKey, u64),
    sort: FreqSort,
    ordering: OrderMode,
) -> Ordering {
    match sort {
        FreqSort::Count => b.1.cmp(&a.1).then_with(|| cmp_value(&a.0, &b.0, ordering)),
        FreqSort::Value => cmp_value(&a.0, &b.0, ordering),
    }
}

/// Value ordering: present values ascending by their typed key, NULL always
/// last. Distinct values with an equal typed key (e.g. `-0.0`/`0.0`, or
/// decimals that round together) fall back to the rendered string so the total
/// order — and therefore the output — stays deterministic.
fn cmp_value(a: &EntryKey, b: &EntryKey, ordering: OrderMode) -> Ordering {
    match (a, b) {
        (EntryKey::Present(x), EntryKey::Present(y)) => {
            cmp_rendered(x, y, ordering).then_with(|| x.cmp(y))
        }
        (EntryKey::Present(_), EntryKey::Null) => Ordering::Less,
        (EntryKey::Null, EntryKey::Present(_)) => Ordering::Greater,
        (EntryKey::Null, EntryKey::Null) => Ordering::Equal,
    }
}

/// Compare two rendered values under `ordering`. Numeric modes parse the
/// rendering back into a comparable value; `Lexical` compares strings directly.
fn cmp_rendered(a: &str, b: &str, ordering: OrderMode) -> Ordering {
    match ordering {
        OrderMode::Integer => match (a.parse::<i128>(), b.parse::<i128>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            _ => a.cmp(b),
        },
        OrderMode::Float => cmp_f64(parse_rendered_f64(a), parse_rendered_f64(b)),
        OrderMode::Decimal => cmp_decimal_str(a, b),
        OrderMode::Lexical => a.cmp(b),
    }
}

/// Parse a CSV-rendered float back to `f64`, honouring the `NaN`/`inf`/`-inf`
/// spellings the renderer emits.
fn parse_rendered_f64(s: &str) -> f64 {
    match s {
        "NaN" => f64::NAN,
        "inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().unwrap_or(f64::NAN),
    }
}

/// Total order over `f64` with all NaNs sorted last (and equal to each other).
fn cmp_f64(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// Compare two CSV-rendered decimal strings (`[-]int[.frac]`) by numeric value,
/// without going through `f64` (decimals can exceed its range/precision).
fn cmp_decimal_str(a: &str, b: &str) -> Ordering {
    let (a_neg, a_mag) = a.strip_prefix('-').map_or((false, a), |r| (true, r));
    let (b_neg, b_mag) = b.strip_prefix('-').map_or((false, b), |r| (true, r));
    match (a_neg, b_neg) {
        (false, true) => Ordering::Greater,
        (true, false) => Ordering::Less,
        (false, false) => cmp_decimal_magnitude(a_mag, b_mag),
        // Both negative: larger magnitude is the smaller number.
        (true, true) => cmp_decimal_magnitude(a_mag, b_mag).reverse(),
    }
}

/// Compare the magnitudes of two non-negative decimal strings.
fn cmp_decimal_magnitude(a: &str, b: &str) -> Ordering {
    let (a_int, a_frac) = a.split_once('.').unwrap_or((a, ""));
    let (b_int, b_frac) = b.split_once('.').unwrap_or((b, ""));
    cmp_int_digits(a_int, b_int).then_with(|| cmp_frac_digits(a_frac, b_frac))
}

/// Compare two integer-digit strings numerically (ignoring leading zeros).
fn cmp_int_digits(a: &str, b: &str) -> Ordering {
    let a = a.trim_start_matches('0');
    let b = b.trim_start_matches('0');
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// Compare two fractional-digit strings numerically (right-padding the shorter
/// with zeros so equal place values line up).
fn cmp_frac_digits(a: &str, b: &str) -> Ordering {
    let width = a.len().max(b.len());
    let a_padded = format!("{a:0<width$}");
    let b_padded = format!("{b:0<width$}");
    a_padded.cmp(&b_padded)
}

/// Format a percentage of `total` with one decimal place, e.g. `45.6%`.
fn percent(count: u64, total: u64) -> String {
    if total == 0 {
        return "0.0%".to_string();
    }
    format!("{:.1}%", count as f64 / total as f64 * 100.0)
}

/// Schema of the emitted table. `value` and `percent` are strings so the column
/// can hold heterogeneous rendered values plus the `NULL`/`<other>` labels.
fn freq_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("percent", DataType::Utf8, false),
    ]))
}

/// Accept only primitive columns that render losslessly under CSV conventions;
/// reject binary, nested, and other non-primitive types with a clear error.
/// This is the CSV allowlist minus the binary family (and `Utf8View`, which the
/// CSV renderer does not handle).
fn validate_freq_type(col: &str, ty: &DataType) -> Result<()> {
    use DataType::*;
    match ty {
        Null
        | Boolean
        | Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Utf8
        | LargeUtf8
        | Date32
        | Date64
        | Time32(_)
        | Time64(_)
        | Timestamp(_, _)
        | Decimal32(_, _)
        | Decimal64(_, _)
        | Decimal128(_, _)
        | Decimal256(_, _) => Ok(()),
        Dictionary(_, value_ty) => validate_freq_type(col, value_ty),
        other => Err(Error::UnsupportedFreqType {
            column: col.to_string(),
            data_type: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::{Path, PathBuf};

    use arrow_array::RecordBatchIterator;
    use lance::Dataset as InnerLance;

    use super::*;
    use crate::output::make_writer;
    use crate::output::table::TableStyle;

    fn label_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)]))
    }

    fn label_batch(vals: &[Option<&str>]) -> RecordBatch {
        RecordBatch::try_new(
            label_schema(),
            vec![Arc::new(StringArray::from(vals.to_vec()))],
        )
        .unwrap()
    }

    /// Write a multi-fragment `label: Utf8` dataset (one fragment per slice) so
    /// accumulation crosses batch boundaries.
    async fn write_labels(dir: &Path, name: &str, fragments: &[&[Option<&str>]]) -> PathBuf {
        let path = dir.join(name);
        let uri = path.to_string_lossy().into_owned();
        let (first, rest) = fragments.split_first().expect("at least one fragment");
        let iter =
            RecordBatchIterator::new(vec![Ok(label_batch(first))].into_iter(), label_schema());
        let mut ds = InnerLance::write(iter, uri.as_str(), None).await.unwrap();
        for frag in rest {
            let iter =
                RecordBatchIterator::new(vec![Ok(label_batch(frag))].into_iter(), label_schema());
            ds.append(iter, None).await.unwrap();
        }
        path
    }

    /// Extract the batch as `(value, count, percent)` triples in row order.
    fn rows(batch: &RecordBatch) -> Vec<(String, u64, String)> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let pcts = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    values.value(i).to_string(),
                    counts.value(i),
                    pcts.value(i).to_string(),
                )
            })
            .collect()
    }

    fn render(batch: &RecordBatch, format: Format) -> String {
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = make_writer(
                format,
                RenderOptions::default(),
                TableStyle::Plain,
                Cursor::new(&mut out),
            );
            w.start(&batch.schema()).unwrap();
            w.write_batch(batch).unwrap();
            w.finish().unwrap();
        }
        String::from_utf8(out).unwrap()
    }

    // label distribution across three fragments:
    //   spam x4, ham x3, NULL x2, eggs x1  (total 10)
    const FRAGMENTS: &[&[Option<&str>]] = &[
        &[Some("spam"), Some("ham"), None, Some("spam")],
        &[Some("ham"), Some("spam"), None],
        &[Some("eggs"), Some("spam"), Some("ham")],
    ];

    async fn open_labels(dir: &Path) -> Arc<LanceDataset> {
        let path = write_labels(dir, "ds", FRAGMENTS).await;
        dataset::open(path.to_str().unwrap(), None).await.unwrap()
    }

    #[tokio::test]
    async fn known_distribution_sorted_by_count_with_null_row() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&batch),
            vec![
                ("spam".to_string(), 4, "40.0%".to_string()),
                ("ham".to_string(), 3, "30.0%".to_string()),
                ("NULL".to_string(), 2, "20.0%".to_string()),
                ("eggs".to_string(), 1, "10.0%".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn sort_by_value_orders_ascending_null_last() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Value,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        let values: Vec<String> = rows(&batch).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(values, vec!["eggs", "ham", "spam", "NULL"]);
    }

    #[tokio::test]
    async fn tie_break_is_deterministic_by_value() {
        // Two values with equal counts must always come out in the same (value
        // ascending) order regardless of hash iteration order.
        let tmp = tempfile::tempdir().unwrap();
        // bravo x2, alpha x2 -> alpha before bravo on the count tie-break.
        let frags: &[&[Option<&str>]] = &[
            &[Some("bravo"), Some("alpha")],
            &[Some("alpha"), Some("bravo")],
        ];
        let path = write_labels(tmp.path(), "tie", frags).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        let values: Vec<String> = rows(&batch).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(values, vec!["alpha", "bravo"]);
    }

    #[tokio::test]
    async fn limit_folds_remainder_into_other_row() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            Some(2),
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        // Top 2 by count are spam(4) and ham(3); remaining NULL(2)+eggs(1)=3.
        assert_eq!(
            rows(&batch),
            vec![
                ("spam".to_string(), 4, "40.0%".to_string()),
                ("ham".to_string(), 3, "30.0%".to_string()),
                ("<other>".to_string(), 3, "30.0%".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn limit_at_or_above_cardinality_has_no_other_row() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        // 4 distinct rows (incl. NULL); a limit of 4 shows all, none folded.
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            Some(4),
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        let values: Vec<String> = rows(&batch).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(values, vec!["spam", "ham", "NULL", "eggs"]);
        assert!(!values.contains(&"<other>".to_string()));
    }

    #[tokio::test]
    async fn where_filter_composes_over_subset() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        // Only the 4 "spam" rows survive the filter.
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            Some("label = 'spam'"),
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&batch),
            vec![("spam".to_string(), 4, "100.0%".to_string())]
        );
    }

    #[tokio::test]
    async fn empty_dataset_yields_header_only() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        // A filter that matches nothing gives an empty result set.
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            Some("label = 'nope'"),
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(render(&batch, Format::Csv), "value,count,percent\n");
        assert_eq!(render(&batch, Format::Jsonl), "");
    }

    #[tokio::test]
    async fn renders_in_all_three_formats() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();

        let csv = render(&batch, Format::Csv);
        assert_eq!(csv.lines().next().unwrap(), "value,count,percent");
        assert!(csv.contains("spam,4,40.0%"));
        assert!(csv.contains("NULL,2,20.0%"));

        let jsonl = render(&batch, Format::Jsonl);
        let first: serde_json::Value = serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
        assert_eq!(first["value"], "spam");
        assert_eq!(first["count"], 4);
        assert_eq!(first["percent"], "40.0%");

        let table = render(&batch, Format::Table);
        assert!(table.contains("| value"), "table header missing:\n{table}");
        assert!(table.contains("| spam"), "table row missing:\n{table}");
    }

    #[tokio::test]
    async fn integer_column_counts_and_temporal_ok() {
        // Non-string primitives are fair game and render with CSV conventions.
        use crate::test_support::write_int_fragments;
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ints", &[&[1, 1, 2], &[2, 2, 3]]).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "id",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&batch),
            vec![
                ("2".to_string(), 3, "50.0%".to_string()),
                ("1".to_string(), 2, "33.3%".to_string()),
                ("3".to_string(), 1, "16.7%".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn nested_column_is_rejected() {
        // Build a dataset with a List<Utf8> column and confirm freq rejects it.
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let tmp = tempfile::tempdir().unwrap();
        let list_field = Field::new_list(
            "tags",
            Arc::new(Field::new("item", DataType::Utf8, true)),
            true,
        );
        let schema = Arc::new(Schema::new(vec![list_field]));
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("x");
        b.append(true);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(b.finish())]).unwrap();
        let path = tmp.path().join("nested");
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        crate::dataset::write_dataset(&path, iter).await.unwrap();
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let err = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "tags",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::UnsupportedFreqType { .. }));
    }

    #[tokio::test]
    async fn binary_column_is_rejected() {
        use arrow_array::{BinaryArray, Int32Array};
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(BinaryArray::from_opt_vec(vec![Some(b"hi".as_ref())])),
            ],
        )
        .unwrap();
        let path = tmp.path().join("bin");
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        crate::dataset::write_dataset(&path, iter).await.unwrap();
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let err = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "data",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::UnsupportedFreqType { .. }));
    }

    #[tokio::test]
    async fn unknown_column_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let ds = open_labels(tmp.path()).await;
        let err = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "nope",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::UnknownColumn { .. }));
    }

    #[tokio::test]
    async fn integer_sort_value_is_numeric_not_lexical() {
        // Regression: sorting the rendered string would give -1,-2,10,2,9.
        use crate::test_support::write_int_fragments;
        let tmp = tempfile::tempdir().unwrap();
        let path = write_int_fragments(tmp.path(), "ints", &[&[9, 10, 10, 2], &[-1, -2, -2]]).await;
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "id",
            None,
            FreqSort::Value,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        let values: Vec<String> = rows(&batch).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(values, vec!["-2", "-1", "2", "9", "10"]);
    }

    #[tokio::test]
    async fn float_sort_value_is_numeric_with_nan_last() {
        use arrow_array::Float64Array;
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let vals = Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(1.5),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
        let path = tmp.path().join("floats");
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        crate::dataset::write_dataset(&path, iter).await.unwrap();
        let ds = dataset::open(path.to_str().unwrap(), None).await.unwrap();

        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "x",
            None,
            FreqSort::Value,
            None,
            RenderOptions::default(),
            MAX_DISTINCT,
        )
        .await
        .unwrap();
        // Numeric order with NaN last; ±0.0 stay two distinct rows (-0 before 0
        // on the string tie-break).
        let values: Vec<String> = rows(&batch).into_iter().map(|(v, _, _)| v).collect();
        assert_eq!(values, vec!["-inf", "-0", "0", "1.5", "inf", "NaN"]);
    }

    #[tokio::test]
    async fn cardinality_guard_trips_past_limit() {
        let tmp = tempfile::tempdir().unwrap();
        // open_labels has three distinct present values (spam/ham/eggs); a cap of
        // two is exceeded when the third distinct value arrives.
        let ds = open_labels(tmp.path()).await;
        let err = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            2,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::CardinalityExceeded { limit: 2, .. }));
    }

    #[tokio::test]
    async fn cardinality_guard_allows_exactly_the_limit() {
        let tmp = tempfile::tempdir().unwrap();
        // Exactly three distinct present values; NULL is not counted toward the
        // present-cardinality cap, so a cap of three succeeds.
        let ds = open_labels(tmp.path()).await;
        let batch = compute(
            ds.as_ref(),
            &ScanProgress::disabled(),
            "label",
            None,
            FreqSort::Count,
            None,
            RenderOptions::default(),
            3,
        )
        .await
        .unwrap();
        // spam/ham/eggs + the NULL row.
        assert_eq!(batch.num_rows(), 4);
    }

    #[test]
    fn decimal_comparator_orders_numerically() {
        use Ordering::{Equal, Greater, Less};
        // Negatives: the value closer to zero is the larger number.
        assert_eq!(cmp_decimal_str("-0.20", "-0.10"), Less);
        assert_eq!(cmp_decimal_str("-0.10", "-0.20"), Greater);
        // Sign dominates magnitude.
        assert_eq!(cmp_decimal_str("-0.01", "0.00"), Less);
        // Leading-zero magnitudes compare by value, not string length/lexically.
        assert_eq!(cmp_decimal_str("9.00", "10.00"), Less);
        assert_eq!(cmp_decimal_magnitude("9.00", "10.00"), Less);
        // No-fraction integers.
        assert_eq!(cmp_decimal_str("9", "10"), Less);
        assert_eq!(cmp_decimal_str("10", "10"), Equal);
        // Fraction padding: trailing zeros are value-neutral.
        assert_eq!(cmp_frac_digits("5", "50"), Equal);
        assert_eq!(cmp_frac_digits("5", "45"), Greater);
        assert_eq!(cmp_decimal_str("1.5", "1.50"), Equal);
        assert_eq!(cmp_decimal_str("1.5", "1.45"), Greater);
        // Beyond f64 precision: only a non-float comparison can distinguish these.
        assert_eq!(
            cmp_decimal_str("-123456789012345678.91", "-123456789012345678.90"),
            Less
        );
        assert_eq!(
            cmp_decimal_str("123456789012345678.90", "123456789012345678.91"),
            Less
        );
    }
}
