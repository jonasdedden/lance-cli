//! `lance-cli diff <A> <B>` — schema + row-count diff between two DIFFERENT datasets.
//!
//! The counterpart to the version diff in `commands::version_diff` (which
//! compares two versions of *one* dataset). It is two `open()` calls, a
//! field-by-field schema comparison (shared with the version diff via
//! `commands::diff_common`), an Arrow schema-metadata comparison, and two
//! concurrent `count_rows()` calls.
//!
//! Because the two inputs are different datasets, the version selectors
//! (`--branch`/`--version`/`--tag`) apply to neither side — they would be
//! ambiguous. The dispatch layer rejects them in this mode; see
//! `commands::dispatch`.

use std::collections::BTreeMap;
use std::io::Write;

use serde_json::{Value, json};

use crate::Result;
use crate::cli::Format;
use crate::commands::Outcome;
use crate::commands::common::project_arrow_schema;
use crate::commands::diff_common::{SchemaDelta, build_schema_delta};
use crate::dataset;
use crate::error::Error;
use crate::projection;

/// Run the two-dataset diff.
///
/// `columns`/`exclude` scope the comparison: the projection is resolved against
/// *each* dataset's own schema (strictly, as everywhere else in lance-cli) and
/// only the surviving columns are compared. Schema-level metadata is compared on the
/// full, unprojected schemas (metadata is dataset-level, not per-column).
pub async fn run(
    left: &str,
    right: &str,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    format: Option<Format>,
) -> Result<Outcome> {
    // Like the version diff, this emits a summary shape rather than rows, so
    // only the human default and `jsonl` are meaningful; reject csv/table/ipc
    // (and, as there, RenderOptions is not threaded — the report has no
    // float or list cells).
    let as_json = match format {
        None => false,
        Some(Format::Jsonl) => true,
        Some(Format::Csv) => return Err(Error::DiffFormatUnsupported { format: "csv" }),
        Some(Format::Table) => return Err(Error::DiffFormatUnsupported { format: "table" }),
        Some(Format::Ipc) => return Err(Error::DiffFormatUnsupported { format: "ipc" }),
    };

    // Generic mode: no Lance selectors on either input (enforced in dispatch).
    let left_ds = dataset::open(left, None).await?;
    let right_ds = dataset::open(right, None).await?;

    let left_full = left_ds.arrow_schema();
    let right_full = right_ds.arrow_schema();

    // Scope the schema comparison to the projected columns. Resolving against
    // each schema independently keeps the strict-projection
    // behaviour: a scoped column absent on one side is a clear "unknown column"
    // error rather than a silent mismatch.
    // Wrap resolution errors with the offending side's path: with two inputs a
    // bare "unknown column 'x'" is ambiguous about which dataset rejected it.
    let left_proj =
        projection::resolve(&left_full, columns, exclude).map_err(|e| Error::DiffColumn {
            path: left.to_string(),
            error: Box::new(e),
        })?;
    let right_proj =
        projection::resolve(&right_full, columns, exclude).map_err(|e| Error::DiffColumn {
            path: right.to_string(),
            error: Box::new(e),
        })?;
    let left_schema = project_arrow_schema(left_full.as_ref(), left_proj.as_deref());
    let right_schema = project_arrow_schema(right_full.as_ref(), right_proj.as_deref());

    let schema = build_schema_delta(&left_schema, &right_schema);
    // Metadata is dataset-level; compare it on the full schemas regardless of
    // the column projection.
    let metadata = build_metadata_delta(left_full.metadata(), right_full.metadata());

    // Both counts are independent I/O; run them concurrently.
    let (left_rows, right_rows) =
        tokio::try_join!(left_ds.count_rows(None), right_ds.count_rows(None))?;

    let report = DatasetDiffReport {
        left: left.to_string(),
        right: right.to_string(),
        left_rows,
        right_rows,
        schema,
        metadata,
    };

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if as_json {
        writeln!(out, "{}", report.to_json())?;
    } else {
        report.write_human(&mut out)?;
    }
    out.flush()?;

    Ok(if report.is_identical() {
        Outcome::Success
    } else {
        Outcome::Different
    })
}

/// Difference between the two datasets' Arrow schema-level metadata maps.
struct MetadataDelta {
    /// `(key, value)` for keys present only in the right dataset.
    added: Vec<(String, String)>,
    /// `(key, value)` for keys present only in the left dataset.
    removed: Vec<(String, String)>,
    /// `(key, left_value, right_value)` for keys present in both with different values.
    changed: Vec<(String, String, String)>,
}

impl MetadataDelta {
    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

fn build_metadata_delta(
    left: &std::collections::HashMap<String, String>,
    right: &std::collections::HashMap<String, String>,
) -> MetadataDelta {
    // Order keys deterministically so both renderings are stable.
    let left: BTreeMap<&str, &str> = left.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let right: BTreeMap<&str, &str> = right
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let mut added = Vec::new();
    let mut changed = Vec::new();
    for (k, rv) in &right {
        match left.get(k) {
            None => added.push((k.to_string(), rv.to_string())),
            Some(lv) if lv != rv => changed.push((k.to_string(), lv.to_string(), rv.to_string())),
            Some(_) => {}
        }
    }
    let removed: Vec<(String, String)> = left
        .iter()
        .filter(|(k, _)| !right.contains_key(*k))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    MetadataDelta {
        added,
        removed,
        changed,
    }
}

/// Fully-computed comparison between two datasets.
struct DatasetDiffReport {
    left: String,
    right: String,
    left_rows: u64,
    right_rows: u64,
    schema: SchemaDelta,
    metadata: MetadataDelta,
}

impl DatasetDiffReport {
    /// True when the two datasets have the same (projected) schema, the same
    /// row count, and the same schema metadata. Drives the `diff(1)` exit code.
    fn is_identical(&self) -> bool {
        self.schema.is_empty() && self.metadata.is_empty() && self.left_rows == self.right_rows
    }

    fn write_human<W: Write>(&self, out: &mut W) -> Result<()> {
        writeln!(out, "diff {} {}", self.left, self.right)?;
        writeln!(out)?;

        if self.is_identical() {
            writeln!(out, "No differences.")?;
            return Ok(());
        }

        let net = self.right_rows as i64 - self.left_rows as i64;
        writeln!(
            out,
            "Rows: {} -> {}  (net {net:+})",
            self.left_rows, self.right_rows
        )?;
        writeln!(out)?;

        if self.schema.is_empty() {
            writeln!(out, "Schema changes: none")?;
        } else {
            writeln!(out, "Schema changes:")?;
            self.schema.write_rows(&mut *out)?;
        }
        writeln!(out)?;

        if self.metadata.is_empty() {
            writeln!(out, "Metadata changes: none")?;
        } else {
            writeln!(out, "Metadata changes:")?;
            for (key, value) in &self.metadata.added {
                writeln!(out, "  + {key}: {value}")?;
            }
            for (key, value) in &self.metadata.removed {
                writeln!(out, "  - {key}: {value}")?;
            }
            for (key, from_val, to_val) in &self.metadata.changed {
                writeln!(out, "  ~ {key}: {from_val} -> {to_val}")?;
            }
        }
        Ok(())
    }

    fn to_json(&self) -> Value {
        let metadata = json!({
            "added": self
                .metadata
                .added
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
            "removed": self
                .metadata
                .removed
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
            "changed": self
                .metadata
                .changed
                .iter()
                .map(|(key, from, to)| json!({ "key": key, "from": from, "to": to }))
                .collect::<Vec<_>>(),
        });

        json!({
            "left": self.left,
            "right": self.right,
            "identical": self.is_identical(),
            "rows": {
                "left": self.left_rows,
                "right": self.right_rows,
                "net": self.right_rows as i64 - self.left_rows as i64,
            },
            "schema": self.schema.to_json(),
            "metadata": metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn report(
        left_rows: u64,
        right_rows: u64,
        from: &Arc<Schema>,
        to: &Arc<Schema>,
    ) -> DatasetDiffReport {
        DatasetDiffReport {
            left: "a.lance".to_string(),
            right: "b.lance".to_string(),
            left_rows,
            right_rows,
            schema: build_schema_delta(from, to),
            metadata: build_metadata_delta(from.metadata(), to.metadata()),
        }
    }

    #[test]
    fn identical_datasets_report_no_differences() {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let r = report(5, 5, &s, &s);
        assert!(r.is_identical());
        let mut buf: Vec<u8> = Vec::new();
        r.write_human(&mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("No differences."), "got: {text}");
        assert_eq!(r.to_json()["identical"], json!(true));
    }

    #[test]
    fn rowcount_only_difference_is_not_identical() {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let r = report(5, 8, &s, &s);
        assert!(!r.is_identical());
        let v = r.to_json();
        assert_eq!(v["rows"]["left"], json!(5));
        assert_eq!(v["rows"]["right"], json!(8));
        assert_eq!(v["rows"]["net"], json!(3));
        // Schema is unchanged even though rows differ.
        assert_eq!(v["schema"]["added"], json!([]));
    }

    #[test]
    fn schema_only_difference_is_not_identical() {
        let from = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let to = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let r = report(5, 5, &from, &to);
        assert!(!r.is_identical());
        let v = r.to_json();
        assert_eq!(v["rows"]["net"], json!(0));
        assert_eq!(v["schema"]["added"][0]["name"], json!("extra"));
    }

    #[test]
    fn metadata_difference_alone_is_a_difference() {
        let from = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let to = Arc::new(
            Schema::new(vec![Field::new("id", DataType::Int32, false)])
                .with_metadata(HashMap::from([("owner".to_string(), "team-b".to_string())])),
        );
        let r = report(5, 5, &from, &to);
        assert!(!r.is_identical());
        let v = r.to_json();
        assert_eq!(v["metadata"]["added"][0]["key"], json!("owner"));
        assert_eq!(v["metadata"]["added"][0]["value"], json!("team-b"));
    }

    #[test]
    fn metadata_changed_value_is_reported() {
        let from = Arc::new(
            Schema::new(vec![Field::new("id", DataType::Int32, false)])
                .with_metadata(HashMap::from([("v".to_string(), "1".to_string())])),
        );
        let to = Arc::new(
            Schema::new(vec![Field::new("id", DataType::Int32, false)])
                .with_metadata(HashMap::from([("v".to_string(), "2".to_string())])),
        );
        let d = build_metadata_delta(from.metadata(), to.metadata());
        assert_eq!(
            d.changed,
            vec![("v".to_string(), "1".to_string(), "2".to_string())]
        );
        assert!(d.added.is_empty() && d.removed.is_empty());
    }
}
