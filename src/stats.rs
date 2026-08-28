//! Streaming per-column summary statistics (the `stats` command engine).
//!
//! [`compute`] folds a dataset's scan batches into one [`ColumnStats`] per
//! column. Memory is independent of the row count: each column keeps a handful
//! of scalars plus a distinct-value set that is *capped* (see [`DISTINCT_CAP`]),
//! so a high-cardinality column can never blow up the accumulator.
//!
//! Type coverage:
//! - **Numeric** (all int/uint widths, `Float16/32/64`): count, nulls, min, max,
//!   mean, sample stddev, distinct.
//! - **Temporal** (`Date32/64`, `Time32/64`, `Timestamp` incl. timezone,
//!   `Duration`): count, nulls, min, max, distinct.
//! - **String** (`Utf8`, `LargeUtf8`, `Utf8View`): count, nulls, lexicographic
//!   min/max, distinct.
//! - **Boolean**: count, nulls, min, max, distinct.
//! - **Everything else** (binary, decimal, nested, dictionary, interval, null):
//!   count and nulls only — never an error.
//!
//! NaN handling mirrors the rest of the crate: `NaN` is skipped for min/max (so
//! the reported range is the real numeric range) but flows through mean/stddev,
//! which therefore report `NaN` when the column contains any `NaN`.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, Float64Array, RecordBatch, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::StreamExt as _;

use crate::Result;
use crate::commands::progress::ScanProgress;
use crate::dataset::{ColumnStats, LanceDataset, ScanOptions};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::output::value::table_cell;

/// Maximum number of distinct values tracked exactly per column. Once a column
/// exceeds this, distinct tracking stops and the count is reported as
/// `>DISTINCT_CAP`, keeping accumulator memory bounded regardless of row count.
pub const DISTINCT_CAP: usize = 10_000;

/// Compute per-column statistics for `ds`, restricted to `projection` columns
/// (all columns when `None`) and to rows matching `filter` (all rows when
/// `None`). Rows are streamed; memory is independent of the row count.
pub async fn compute(
    ds: &LanceDataset,
    progress: &ScanProgress,
    projection: Option<&[String]>,
    filter: Option<&str>,
) -> Result<Vec<ColumnStats>> {
    let options = ScanOptions {
        projection,
        filter,
        ..Default::default()
    };

    // Build one accumulator per projected column, up front, so that an empty
    // dataset (or a fully filtered-out one) still yields a row per column.
    let projected_schema = projected_schema(ds.arrow_schema().as_ref(), projection)?;
    let mut accs: Vec<Accumulator> = projected_schema
        .fields()
        .iter()
        .map(|f| Accumulator::new(f.name().clone(), f.data_type().clone()))
        .collect();

    let mut stream = progress.wrap(ds.scan(&options).await?);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Accumulators are indexed positionally, so the scan must yield columns
        // in the projected order. Guard that invariant in debug builds: a future
        // change that reorders projected columns would otherwise silently
        // misattribute statistics.
        debug_assert!(
            accs.iter()
                .zip(batch.schema().fields())
                .all(|(acc, field)| acc.name == *field.name()),
            "scan batch columns are not in projected order: expected {:?}, got {:?}",
            accs.iter().map(|a| &a.name).collect::<Vec<_>>(),
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
        );
        for (idx, acc) in accs.iter_mut().enumerate() {
            acc.update(batch.column(idx).as_ref());
        }
    }

    Ok(accs.into_iter().map(Accumulator::finish).collect())
}

/// The input schema projected to `columns` (all columns when `None`), used to
/// seed one accumulator per column before any batch is read.
///
/// `compute` is public, so an unknown column name is a caller error rather than
/// a validated precondition: it returns [`Error::UnknownColumn`] instead of
/// panicking. (The CLI still validates up front via `projection::resolve`.)
fn projected_schema(schema: &Schema, columns: Option<&[String]>) -> Result<SchemaRef> {
    match columns {
        None => Ok(Arc::new(schema.clone())),
        Some(cols) => {
            let mut fields = Vec::with_capacity(cols.len());
            for name in cols {
                let field = schema
                    .field_with_name(name)
                    .map_err(|_| Error::UnknownColumn {
                        name: name.clone(),
                        available: schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                    })?;
                fields.push(field.clone());
            }
            Ok(Arc::new(Schema::new(fields)))
        }
    }
}

/// Fixed output schema of the `stats` command.
pub fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("column", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("nulls", DataType::UInt64, false),
        Field::new("min", DataType::Utf8, true),
        Field::new("max", DataType::Utf8, true),
        Field::new("mean", DataType::Float64, true),
        Field::new("stddev", DataType::Float64, true),
        Field::new("distinct", DataType::Utf8, true),
    ]))
}

/// Render computed statistics as a single `RecordBatch` matching [`output_schema`],
/// one row per column. This is the metadata-table pathway used by
/// `versions`/`branches`, so `--format table|jsonl|csv` all work.
pub fn to_record_batch(stats: &[ColumnStats]) -> Result<RecordBatch> {
    let schema = output_schema();
    let column: ArrayRef = Arc::new(StringArray::from(
        stats.iter().map(|s| s.column.as_str()).collect::<Vec<_>>(),
    ));
    let ty: ArrayRef = Arc::new(StringArray::from(
        stats
            .iter()
            .map(|s| s.data_type.as_str())
            .collect::<Vec<_>>(),
    ));
    let count: ArrayRef = Arc::new(UInt64Array::from(
        stats.iter().map(|s| s.count).collect::<Vec<_>>(),
    ));
    let nulls: ArrayRef = Arc::new(UInt64Array::from(
        stats.iter().map(|s| s.nulls).collect::<Vec<_>>(),
    ));
    let min: ArrayRef = Arc::new(StringArray::from(
        stats.iter().map(|s| s.min.clone()).collect::<Vec<_>>(),
    ));
    let max: ArrayRef = Arc::new(StringArray::from(
        stats.iter().map(|s| s.max.clone()).collect::<Vec<_>>(),
    ));
    let mean: ArrayRef = Arc::new(Float64Array::from(
        stats.iter().map(|s| s.mean).collect::<Vec<_>>(),
    ));
    let stddev: ArrayRef = Arc::new(Float64Array::from(
        stats.iter().map(|s| s.stddev).collect::<Vec<_>>(),
    ));
    let distinct: ArrayRef = Arc::new(StringArray::from(
        stats.iter().map(|s| s.distinct.clone()).collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(
        schema,
        vec![column, ty, count, nulls, min, max, mean, stddev, distinct],
    )?)
}

// --------------------------- accumulator internals --------------------------

/// Per-column running statistics.
struct Accumulator {
    name: String,
    data_type: DataType,
    non_null: u64,
    nulls: u64,
    stat: Stat,
}

/// Type-specific running state. A column has exactly one variant for its whole
/// lifetime (columns are homogeneously typed).
enum Stat {
    Signed {
        min: Option<i64>,
        max: Option<i64>,
        moments: Moments,
        distinct: Distinct,
    },
    Unsigned {
        min: Option<u64>,
        max: Option<u64>,
        moments: Moments,
        distinct: Distinct,
    },
    Float {
        min: Option<f64>,
        max: Option<f64>,
        moments: Moments,
        distinct: Distinct,
    },
    Str {
        min: Option<String>,
        max: Option<String>,
        distinct: Distinct,
    },
    Bool {
        min: Option<bool>,
        max: Option<bool>,
        distinct: Distinct,
    },
    /// Temporal columns: min/max tracked as the raw arrow integer; formatting
    /// defers to the shared value formatter via the column's `DataType`.
    Temporal {
        min: Option<i64>,
        max: Option<i64>,
        distinct: Distinct,
    },
    /// Binary / decimal / nested / dictionary / interval / null: count + nulls only.
    Other,
}

impl Accumulator {
    fn new(name: String, data_type: DataType) -> Self {
        let stat = Stat::for_type(&data_type);
        Self {
            name,
            data_type,
            non_null: 0,
            nulls: 0,
            stat,
        }
    }

    fn update(&mut self, array: &dyn Array) {
        let nulls = array.null_count();
        self.nulls += nulls as u64;
        self.non_null += (array.len() - nulls) as u64;
        self.stat.update(array);
    }

    fn finish(self) -> ColumnStats {
        let data_type = format!("{:?}", self.data_type);
        let (min, max, mean, stddev, distinct) = self.stat.finish(&self.data_type);
        ColumnStats {
            column: self.name,
            data_type,
            count: self.non_null,
            nulls: self.nulls,
            min,
            max,
            mean,
            stddev,
            distinct,
        }
    }
}

impl Stat {
    fn for_type(dt: &DataType) -> Self {
        use DataType::*;
        match dt {
            Int8 | Int16 | Int32 | Int64 => Stat::Signed {
                min: None,
                max: None,
                moments: Moments::default(),
                distinct: Distinct::default(),
            },
            UInt8 | UInt16 | UInt32 | UInt64 => Stat::Unsigned {
                min: None,
                max: None,
                moments: Moments::default(),
                distinct: Distinct::default(),
            },
            Float16 | Float32 | Float64 => Stat::Float {
                min: None,
                max: None,
                moments: Moments::default(),
                distinct: Distinct::default(),
            },
            Utf8 | LargeUtf8 | Utf8View => Stat::Str {
                min: None,
                max: None,
                distinct: Distinct::default(),
            },
            Boolean => Stat::Bool {
                min: None,
                max: None,
                distinct: Distinct::default(),
            },
            Date32 | Date64 | Time32(_) | Time64(_) | Timestamp(_, _) | Duration(_) => {
                Stat::Temporal {
                    min: None,
                    max: None,
                    distinct: Distinct::default(),
                }
            }
            _ => Stat::Other,
        }
    }

    fn update(&mut self, array: &dyn Array) {
        match self {
            Stat::Signed {
                min,
                max,
                moments,
                distinct,
            } => {
                for v in signed_values(array) {
                    update_ord(min, max, v);
                    moments.push(v as f64);
                    distinct.add(DistinctKey::Int(v));
                }
            }
            Stat::Unsigned {
                min,
                max,
                moments,
                distinct,
            } => {
                for v in unsigned_values(array) {
                    update_ord(min, max, v);
                    moments.push(v as f64);
                    distinct.add(DistinctKey::UInt(v));
                }
            }
            Stat::Float {
                min,
                max,
                moments,
                distinct,
            } => {
                for v in float_values(array) {
                    if !v.is_nan() {
                        if min.is_none_or(|m| v < m) {
                            *min = Some(v);
                        }
                        if max.is_none_or(|m| v > m) {
                            *max = Some(v);
                        }
                    }
                    moments.push(v);
                    distinct.add(DistinctKey::Float(canonical_float_bits(v)));
                }
            }
            Stat::Str { min, max, distinct } => {
                for_each_string(array, |s| {
                    if min.as_deref().is_none_or(|m| s < m) {
                        *min = Some(s.to_string());
                    }
                    if max.as_deref().is_none_or(|m| s > m) {
                        *max = Some(s.to_string());
                    }
                    distinct.add(DistinctKey::Str(s.to_string()));
                });
            }
            Stat::Bool { min, max, distinct } => {
                let arr = array.as_boolean();
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let v = arr.value(i);
                    update_ord(min, max, v);
                    distinct.add(DistinctKey::Bool(v));
                }
            }
            Stat::Temporal { min, max, distinct } => {
                for v in temporal_values(array) {
                    update_ord(min, max, v);
                    distinct.add(DistinctKey::Int(v));
                }
            }
            Stat::Other => {}
        }
    }

    #[allow(clippy::type_complexity)]
    fn finish(
        self,
        dt: &DataType,
    ) -> (
        Option<String>,
        Option<String>,
        Option<f64>,
        Option<f64>,
        Option<String>,
    ) {
        match self {
            Stat::Signed {
                min,
                max,
                moments,
                distinct,
            } => (
                min.map(|v| v.to_string()),
                max.map(|v| v.to_string()),
                moments.mean(),
                moments.stddev(),
                Some(distinct.finish()),
            ),
            Stat::Unsigned {
                min,
                max,
                moments,
                distinct,
            } => (
                min.map(|v| v.to_string()),
                max.map(|v| v.to_string()),
                moments.mean(),
                moments.stddev(),
                Some(distinct.finish()),
            ),
            Stat::Float {
                min,
                max,
                moments,
                distinct,
            } => (
                min.map(format_float),
                max.map(format_float),
                moments.mean(),
                moments.stddev(),
                Some(distinct.finish()),
            ),
            Stat::Str { min, max, distinct } => (min, max, None, None, Some(distinct.finish())),
            Stat::Bool { min, max, distinct } => (
                min.map(|v| v.to_string()),
                max.map(|v| v.to_string()),
                None,
                None,
                Some(distinct.finish()),
            ),
            Stat::Temporal { min, max, distinct } => (
                min.and_then(|v| format_temporal(dt, v)),
                max.and_then(|v| format_temporal(dt, v)),
                None,
                None,
                Some(distinct.finish()),
            ),
            Stat::Other => (None, None, None, None, None),
        }
    }
}

/// Update a running (min, max) with a non-null comparable value.
fn update_ord<T: PartialOrd + Copy>(min: &mut Option<T>, max: &mut Option<T>, v: T) {
    if min.is_none_or(|m| v < m) {
        *min = Some(v);
    }
    if max.is_none_or(|m| v > m) {
        *max = Some(v);
    }
}

// ------------------------------- moments (Welford) --------------------------

/// Streaming mean/variance via Welford's algorithm: numerically stable and
/// trivially combinable value-by-value across batches. `NaN` inputs propagate
/// into `mean`/`m2`, so any `NaN` in the column yields `NaN` statistics.
#[derive(Default)]
struct Moments {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Moments {
    fn push(&mut self, x: f64) {
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    fn mean(&self) -> Option<f64> {
        (self.count > 0).then_some(self.mean)
    }

    /// Sample standard deviation (ddof = 1). Undefined for fewer than two values.
    ///
    /// Values are accumulated in `f64`, so for integer magnitudes near `2^63`
    /// the result is f64-precision-limited (the same limitation as numpy).
    fn stddev(&self) -> Option<f64> {
        (self.count >= 2).then(|| (self.m2 / (self.count as f64 - 1.0)).sqrt())
    }
}

// ------------------------------- distinct tracking --------------------------

#[derive(Hash, PartialEq, Eq)]
enum DistinctKey {
    Int(i64),
    UInt(u64),
    /// Canonicalised float bits (all NaNs and both zeros collapse).
    Float(u64),
    Str(String),
    Bool(bool),
}

/// A capped exact distinct-value counter. Once more than [`DISTINCT_CAP`]
/// distinct values are seen, tracking stops and the set is dropped, so memory
/// stays bounded and the reported count becomes `>DISTINCT_CAP`.
#[derive(Default)]
struct Distinct {
    set: HashSet<DistinctKey>,
    capped: bool,
}

impl Distinct {
    fn add(&mut self, key: DistinctKey) {
        if self.capped {
            return;
        }
        self.set.insert(key);
        if self.set.len() > DISTINCT_CAP {
            self.capped = true;
            self.set = HashSet::new();
        }
    }

    fn finish(&self) -> String {
        if self.capped {
            format!(">{DISTINCT_CAP}")
        } else {
            self.set.len().to_string()
        }
    }
}

/// Collapse all `NaN` bit patterns to one key and `-0.0`/`+0.0` to one key, so
/// distinct counts match value equality rather than bit equality.
fn canonical_float_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0.0_f64.to_bits()
    } else {
        v.to_bits()
    }
}

// ------------------------------- value extraction ---------------------------

/// Non-null signed-integer values, widened to `i64`.
fn signed_values(array: &dyn Array) -> Vec<i64> {
    match array.data_type() {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Int64 => array.as_primitive::<Int64Type>().iter().flatten().collect(),
        _ => Vec::new(),
    }
}

/// Non-null unsigned-integer values, widened to `u64`.
fn unsigned_values(array: &dyn Array) -> Vec<u64> {
    match array.data_type() {
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .iter()
            .flatten()
            .map(u64::from)
            .collect(),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .iter()
            .flatten()
            .map(u64::from)
            .collect(),
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .iter()
            .flatten()
            .map(u64::from)
            .collect(),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .iter()
            .flatten()
            .collect(),
        _ => Vec::new(),
    }
}

/// Non-null float values, widened to `f64` (`Float16` via `f32`).
fn float_values(array: &dyn Array) -> Vec<f64> {
    match array.data_type() {
        DataType::Float16 => array
            .as_primitive::<Float16Type>()
            .iter()
            .flatten()
            .map(|v| f64::from(f32::from(v)))
            .collect(),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .flatten()
            .map(f64::from)
            .collect(),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .collect(),
        _ => Vec::new(),
    }
}

/// Non-null temporal values as their raw arrow integer (widened to `i64`).
fn temporal_values(array: &dyn Array) -> Vec<i64> {
    match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<arrow_array::types::Date32Type>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Date64 => array
            .as_primitive::<arrow_array::types::Date64Type>()
            .iter()
            .flatten()
            .collect(),
        DataType::Time32(TimeUnit::Second) => array
            .as_primitive::<Time32SecondType>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_primitive::<Time32MillisecondType>()
            .iter()
            .flatten()
            .map(i64::from)
            .collect(),
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_primitive::<Time64MicrosecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_primitive::<Time64NanosecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Duration(TimeUnit::Second) => array
            .as_primitive::<arrow_array::types::DurationSecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Duration(TimeUnit::Millisecond) => array
            .as_primitive::<arrow_array::types::DurationMillisecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Duration(TimeUnit::Microsecond) => array
            .as_primitive::<arrow_array::types::DurationMicrosecondType>()
            .iter()
            .flatten()
            .collect(),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<arrow_array::types::DurationNanosecondType>()
            .iter()
            .flatten()
            .collect(),
        _ => Vec::new(),
    }
}

/// Apply `f` to every non-null string value across the three string layouts.
fn for_each_string(array: &dyn Array, mut f: impl FnMut(&str)) {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    f(arr.value(i));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    f(arr.value(i));
                }
            }
        }
        DataType::Utf8View => {
            let arr = array.as_string_view();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    f(arr.value(i));
                }
            }
        }
        _ => {}
    }
}

// ------------------------------- formatting ---------------------------------

/// Format a float min/max the way the CSV/table renderers do (`NaN` never
/// reaches here since it is excluded from min/max; only finite/inf values do).
fn format_float(v: f64) -> String {
    if v.is_infinite() {
        if v > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        v.to_string()
    }
}

/// Format a temporal min/max by reconstructing a one-element array of the
/// column's exact type and deferring to the shared value formatter, so dates,
/// times, timestamps (with timezone), and durations render identically to how
/// they appear in row output.
fn format_temporal(dt: &DataType, v: i64) -> Option<String> {
    let array: ArrayRef = match dt {
        DataType::Date32 => Arc::new(Date32Array::from(vec![v as i32])),
        DataType::Date64 => Arc::new(Date64Array::from(vec![v])),
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(vec![v as i32])),
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(vec![v as i32]))
        }
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(vec![v])),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from(vec![v])),
        DataType::Timestamp(unit, tz) => timestamp_array(*unit, tz.as_deref(), v),
        DataType::Duration(TimeUnit::Second) => Arc::new(DurationSecondArray::from(vec![v])),
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(vec![v]))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(vec![v]))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(vec![v]))
        }
        _ => return None,
    };
    // Stat min/max are pre-rendered to strings here, so no truncation or
    // float-precision option applies; use plain defaults.
    table_cell(array.as_ref(), 0, RenderOptions::default()).ok()
}

fn timestamp_array(unit: TimeUnit, tz: Option<&str>, v: i64) -> ArrayRef {
    match unit {
        TimeUnit::Second => build_ts(TimestampSecondArray::from(vec![v]), tz),
        TimeUnit::Millisecond => build_ts(TimestampMillisecondArray::from(vec![v]), tz),
        TimeUnit::Microsecond => build_ts(TimestampMicrosecondArray::from(vec![v]), tz),
        TimeUnit::Nanosecond => build_ts(TimestampNanosecondArray::from(vec![v]), tz),
    }
}

/// Attach the timezone (when present) to a freshly built timestamp array.
fn build_ts<A>(array: A, tz: Option<&str>) -> ArrayRef
where
    A: Array + WithTimezone + 'static,
{
    match tz {
        Some(tz) => Arc::new(array.with_tz(tz)),
        None => Arc::new(array),
    }
}

/// Tiny helper trait so `build_ts` can call `with_timezone` generically across
/// the four timestamp array widths.
trait WithTimezone: Sized {
    fn with_tz(self, tz: &str) -> Self;
}

macro_rules! impl_with_timezone {
    ($($ty:ty),+ $(,)?) => {
        $(impl WithTimezone for $ty {
            fn with_tz(self, tz: &str) -> Self {
                self.with_timezone(tz.to_string())
            }
        })+
    };
}

impl_with_timezone!(
    TimestampSecondArray,
    TimestampMillisecondArray,
    TimestampMicrosecondArray,
    TimestampNanosecondArray,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moments_mean_and_sample_stddev() {
        let mut m = Moments::default();
        for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            m.push(x);
        }
        assert_eq!(m.mean(), Some(5.0));
        // sample stddev of that classic dataset is sqrt(32/7).
        let s = m.stddev().unwrap();
        assert!((s - (32.0_f64 / 7.0).sqrt()).abs() < 1e-12);
    }

    #[test]
    fn moments_single_value_has_no_stddev() {
        let mut m = Moments::default();
        m.push(42.0);
        assert_eq!(m.mean(), Some(42.0));
        assert_eq!(m.stddev(), None);
    }

    #[test]
    fn moments_empty() {
        let m = Moments::default();
        assert_eq!(m.mean(), None);
        assert_eq!(m.stddev(), None);
    }

    #[test]
    fn nan_propagates_into_mean() {
        let mut m = Moments::default();
        m.push(1.0);
        m.push(f64::NAN);
        assert!(m.mean().unwrap().is_nan());
        assert!(m.stddev().unwrap().is_nan());
    }

    #[test]
    fn distinct_exact_below_cap() {
        let mut d = Distinct::default();
        for v in [1_i64, 1, 2, 3, 3, 3] {
            d.add(DistinctKey::Int(v));
        }
        assert_eq!(d.finish(), "3");
    }

    #[test]
    fn distinct_caps_and_reports_marker() {
        let mut d = Distinct::default();
        for v in 0..(DISTINCT_CAP as i64 + 50) {
            d.add(DistinctKey::Int(v));
        }
        assert_eq!(d.finish(), format!(">{DISTINCT_CAP}"));
        assert!(d.set.is_empty(), "capped set is dropped to bound memory");
    }

    #[test]
    fn distinct_collapses_nan_and_zero() {
        let mut d = Distinct::default();
        d.add(DistinctKey::Float(canonical_float_bits(f64::NAN)));
        d.add(DistinctKey::Float(canonical_float_bits(-f64::NAN)));
        d.add(DistinctKey::Float(canonical_float_bits(0.0)));
        d.add(DistinctKey::Float(canonical_float_bits(-0.0)));
        assert_eq!(d.finish(), "2");
    }
}
