//! `lance-cli diff DS --from <ref>` — compare two versions of one dataset.
//!
//! The whole comparison lives in this command layer rather than in
//! [`crate::dataset`]: the two endpoints are opened as ordinary handles via the
//! existing checkout path (`dataset::open` → `apply_checkout`), and the deltas
//! are computed from primitives the dataset already exposes (`arrow_schema`,
//! `list_fragments`, `list_indices`, `list_versions`, `checkout_state`). That
//! makes every delta a pure function of already-collected data, so the
//! comparison logic is unit-tested without touching a dataset.
//!
//! The sibling dataset-vs-dataset mode (`commands::diff`) reuses this module's
//! `SchemaDelta` machinery via `commands::diff_common`; only the
//! endpoint-collection step differs.

use std::collections::BTreeMap;
use std::io::Write;

use arrow_schema::SchemaRef;
use serde_json::{Value, json};

use crate::Result;
use crate::cli::{Format, VersionArgs};
use crate::commands::Outcome;
use crate::commands::diff_common::{SchemaDelta, build_schema_delta};
use crate::dataset::{self, FragmentInfo, IndexInfo, MAIN_BRANCH, VersionInfo};
use crate::error::Error;

/// Version/tag/branch selectors parsed from the `diff` subcommand flags.
#[derive(Debug, Default, Clone)]
pub struct DiffSelectors {
    pub branch: Option<String>,
    pub from_version: Option<u64>,
    pub from_tag: Option<String>,
    pub to_version: Option<u64>,
    pub to_tag: Option<String>,
}

pub async fn run(input: &str, sel: DiffSelectors, format: Option<Format>) -> Result<Outcome> {
    // diff emits its own summary shape, not row-shaped output, so only the
    // default (human) and `jsonl` are meaningful. Reject csv/table/json
    // explicitly rather than silently degrading to the human summary.
    // RenderOptions is deliberately NOT threaded here: the report contains no
    // float or list cells (counts, names, and timestamps only), so the
    // output-control flags (--max-list-items/--max-cell-width/
    // --float-precision) have nothing to act on. Documented in the README.
    let as_json = match format {
        None => false,
        Some(Format::Jsonl) => true,
        Some(Format::Csv) => return Err(Error::DiffFormatUnsupported { format: "csv" }),
        Some(Format::Table) => return Err(Error::DiffFormatUnsupported { format: "table" }),
        Some(Format::Ipc) => return Err(Error::DiffFormatUnsupported { format: "ipc" }),
    };

    // Open the "from" endpoint first so its resolved branch can seed the
    // default "to" endpoint (latest of the same branch).
    let from_args = VersionArgs {
        branch: sel.branch.clone(),
        version: sel.from_version,
        tag: sel.from_tag.clone(),
        as_of: None,
    };
    let from_ds = dataset::open(input, Some(&from_args)).await?;
    let from_state = from_ds.checkout_state();

    let to_args = if sel.to_version.is_some() || sel.to_tag.is_some() {
        VersionArgs {
            branch: sel.branch.clone(),
            version: sel.to_version,
            tag: sel.to_tag.clone(),
            as_of: None,
        }
    } else {
        // Default: latest of the same branch as `from`. Map the implicit main
        // branch back to `None` so we never `checkout_branch("main")`.
        let branch = (from_state.branch != MAIN_BRANCH).then(|| from_state.branch.clone());
        VersionArgs {
            branch,
            version: None,
            tag: None,
            as_of: None,
        }
    };
    let to_ds = dataset::open(input, Some(&to_args)).await?;
    let to_state = to_ds.checkout_state();

    // A diff is only meaningful within one branch's linear history. Tag/branch
    // mismatches on a single endpoint are already caught by `apply_checkout`;
    // this catches the case where the two endpoints resolve to different
    // branches (e.g. `--from-tag` and `--to-tag` on different branches).
    if from_state.branch != to_state.branch {
        return Err(Error::DiffCrossBranch {
            from_branch: from_state.branch,
            to_branch: to_state.branch,
        });
    }

    let from = Endpoint {
        branch: from_state.branch,
        version: from_state.version,
        schema: from_ds.arrow_schema(),
        fragments: from_ds.list_fragments(false).await?,
        indices: from_ds.list_indices().await?,
    };
    let to = Endpoint {
        branch: to_state.branch,
        version: to_state.version,
        schema: to_ds.arrow_schema(),
        fragments: to_ds.list_fragments(false).await?,
        indices: to_ds.list_indices().await?,
    };

    // Version log for the range (from, to]. `list_versions` returns the full
    // branch history regardless of either handle's checkout, so filter here.
    let (lo, hi) = (from.version.min(to.version), from.version.max(to.version));
    let versions_in_range: Vec<VersionInfo> = to_ds
        .list_versions(Some(&to.branch), false)
        .await?
        .into_iter()
        .filter(|v| v.version > lo && v.version <= hi)
        .collect();

    let report = build_report(&from, &to, versions_in_range);

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if as_json {
        writeln!(out, "{}", report.to_json())?;
    } else {
        report.write_human(&mut out, input)?;
    }
    out.flush()?;

    Ok(if report.is_identical() {
        Outcome::Success
    } else {
        Outcome::Different
    })
}

/// One side of the comparison: everything collected from a single opened handle.
struct Endpoint {
    branch: String,
    version: u64,
    schema: SchemaRef,
    fragments: Vec<FragmentInfo>,
    indices: Vec<IndexInfo>,
}

/// Fully-computed comparison between two endpoints. Rendered as either a
/// human-readable summary or a single JSON record.
struct DiffReport {
    from_branch: String,
    from_version: u64,
    to_branch: String,
    to_version: u64,
    rows: RowDelta,
    schema: SchemaDelta,
    fragments: FragmentDelta,
    indices: IndexDelta,
    /// Versions in the range `(from, to]`, oldest first.
    versions: Vec<VersionInfo>,
}

/// Row-count delta, split into added/deleted from fragment metadata (no scan).
struct RowDelta {
    from_rows: u64,
    to_rows: u64,
    /// Live rows (physical − deleted) in fragments present only in `to`, plus
    /// rows un-tombstoned on fragments present in both (e.g. a version restore).
    added: u64,
    /// Live rows in fragments present only in `from`, plus the increase in
    /// tombstones on fragments present in both.
    deleted: u64,
}

struct FragmentDelta {
    /// Fragment ids present only in `to`.
    added: Vec<u64>,
    /// Fragment ids present only in `from`.
    removed: Vec<u64>,
    /// Fragment ids present in both but backed by a different set of data files.
    rewritten: Vec<u64>,
}

struct IndexDelta {
    /// Index names present only in `to`.
    created: Vec<String>,
    /// Index names present only in `from`.
    dropped: Vec<String>,
}

fn build_report(from: &Endpoint, to: &Endpoint, versions: Vec<VersionInfo>) -> DiffReport {
    DiffReport {
        from_branch: from.branch.clone(),
        from_version: from.version,
        to_branch: to.branch.clone(),
        to_version: to.version,
        rows: build_row_delta(&from.fragments, &to.fragments),
        schema: build_schema_delta(&from.schema, &to.schema),
        fragments: build_fragment_delta(&from.fragments, &to.fragments),
        indices: build_index_delta(&from.indices, &to.indices),
        versions,
    }
}

/// Live rows in a fragment: physical rows minus tombstoned rows.
fn live_rows(f: &FragmentInfo) -> u64 {
    f.physical_rows.saturating_sub(f.deleted_rows)
}

fn build_row_delta(from: &[FragmentInfo], to: &[FragmentInfo]) -> RowDelta {
    let from_by_id: BTreeMap<u64, &FragmentInfo> = from.iter().map(|f| (f.id, f)).collect();
    let to_by_id: BTreeMap<u64, &FragmentInfo> = to.iter().map(|f| (f.id, f)).collect();

    let from_rows: u64 = from.iter().map(live_rows).sum();
    let to_rows: u64 = to.iter().map(live_rows).sum();

    // Live rows in fragments present only in `to` are added; live rows in
    // fragments present only in `from` are deleted.
    let added_frag_rows: u64 = to
        .iter()
        .filter(|f| !from_by_id.contains_key(&f.id))
        .map(live_rows)
        .sum();
    let removed_frag_rows: u64 = from
        .iter()
        .filter(|f| !to_by_id.contains_key(&f.id))
        .map(live_rows)
        .sum();

    // Tombstone changes on fragments present in both versions are symmetric: an
    // *increase* in tombstones deletes live rows, a *decrease* adds them back.
    // A decrease happens when a version `restore` un-deletes rows (or on a
    // reversed range). Counting both directions keeps the identity
    // `added - deleted == to_rows - from_rows` exact — dropping the decrease
    // would make a pure un-delete look like "no change".
    let mut new_tombstones = 0u64; // rows freshly tombstoned  -> deleted
    let mut restored_tombstones = 0u64; // rows un-tombstoned    -> added
    for f in to {
        if let Some(old) = from_by_id.get(&f.id) {
            new_tombstones += f.deleted_rows.saturating_sub(old.deleted_rows);
            restored_tombstones += old.deleted_rows.saturating_sub(f.deleted_rows);
        }
    }

    RowDelta {
        from_rows,
        to_rows,
        added: added_frag_rows + restored_tombstones,
        deleted: removed_frag_rows + new_tombstones,
    }
}

fn build_fragment_delta(from: &[FragmentInfo], to: &[FragmentInfo]) -> FragmentDelta {
    let from_by_id: BTreeMap<u64, &FragmentInfo> = from.iter().map(|f| (f.id, f)).collect();
    let to_by_id: BTreeMap<u64, &FragmentInfo> = to.iter().map(|f| (f.id, f)).collect();

    let added: Vec<u64> = to
        .iter()
        .filter(|f| !from_by_id.contains_key(&f.id))
        .map(|f| f.id)
        .collect();
    let removed: Vec<u64> = from
        .iter()
        .filter(|f| !to_by_id.contains_key(&f.id))
        .map(|f| f.id)
        .collect();
    // "Rewritten" = same id in both but a different set of data files. Lance
    // never reuses fragment ids, so compaction shows up as removed+added, not
    // here; this bucket captures in-place changes to a fragment's data files,
    // e.g. a column added via `add_columns`/`merge` appends a new file to the
    // existing fragment. (Tombstone-only changes keep the same files and stay
    // out of this bucket — they surface in the row delta instead.)
    let mut rewritten: Vec<u64> = to
        .iter()
        .filter_map(|f| from_by_id.get(&f.id).map(|old| (f, *old)))
        .filter(|(new, old)| file_set_differs(new, old))
        .map(|(f, _)| f.id)
        .collect();
    rewritten.sort_unstable();

    FragmentDelta {
        added,
        removed,
        rewritten,
    }
}

fn file_set_differs(a: &FragmentInfo, b: &FragmentInfo) -> bool {
    let mut fa: Vec<&str> = a.files.iter().map(String::as_str).collect();
    let mut fb: Vec<&str> = b.files.iter().map(String::as_str).collect();
    fa.sort_unstable();
    fb.sort_unstable();
    fa != fb
}

fn build_index_delta(from: &[IndexInfo], to: &[IndexInfo]) -> IndexDelta {
    let from_names: BTreeMap<&str, &IndexInfo> =
        from.iter().map(|i| (i.name.as_str(), i)).collect();
    let to_names: BTreeMap<&str, &IndexInfo> = to.iter().map(|i| (i.name.as_str(), i)).collect();

    let created: Vec<String> = to
        .iter()
        .filter(|i| !from_names.contains_key(i.name.as_str()))
        .map(|i| i.name.clone())
        .collect();
    let dropped: Vec<String> = from
        .iter()
        .filter(|i| !to_names.contains_key(i.name.as_str()))
        .map(|i| i.name.clone())
        .collect();

    IndexDelta { created, dropped }
}

impl DiffReport {
    /// True when the two versions are content-identical: no schema, fragment,
    /// index or row-level change. Drives the `diff(1)` exit code (0 vs 1).
    fn is_identical(&self) -> bool {
        self.schema.is_empty()
            && self.fragments.added.is_empty()
            && self.fragments.removed.is_empty()
            && self.fragments.rewritten.is_empty()
            && self.indices.created.is_empty()
            && self.indices.dropped.is_empty()
            && self.rows.added == 0
            && self.rows.deleted == 0
            // Belt-and-braces: a live-row count change must never read as
            // identical even if the per-fragment split somehow nets to zero.
            && self.rows.from_rows == self.rows.to_rows
    }

    fn write_human<W: Write>(&self, out: &mut W, input: &str) -> Result<()> {
        writeln!(out, "diff {input}")?;
        writeln!(out, "  from  {} v{}", self.from_branch, self.from_version)?;
        writeln!(out, "  to    {} v{}", self.to_branch, self.to_version)?;
        writeln!(out)?;

        if self.is_identical() {
            writeln!(out, "No differences.")?;
            return Ok(());
        }

        let net = self.rows.to_rows as i64 - self.rows.from_rows as i64;
        writeln!(
            out,
            "Rows: {} -> {}  (net {:+}; +{} added, -{} deleted)",
            self.rows.from_rows, self.rows.to_rows, net, self.rows.added, self.rows.deleted
        )?;
        writeln!(out)?;

        // Schema.
        if self.schema.is_empty() {
            writeln!(out, "Schema changes: none")?;
        } else {
            writeln!(out, "Schema changes:")?;
            self.schema.write_rows(&mut *out)?;
        }
        writeln!(out)?;

        // Fragments.
        writeln!(
            out,
            "Fragments: +{} added, -{} removed, {} rewritten",
            self.fragments.added.len(),
            self.fragments.removed.len(),
            self.fragments.rewritten.len()
        )?;
        if !self.fragments.added.is_empty() {
            writeln!(out, "  added:     {}", fmt_ids(&self.fragments.added))?;
        }
        if !self.fragments.removed.is_empty() {
            writeln!(out, "  removed:   {}", fmt_ids(&self.fragments.removed))?;
        }
        if !self.fragments.rewritten.is_empty() {
            writeln!(out, "  rewritten: {}", fmt_ids(&self.fragments.rewritten))?;
        }
        writeln!(out)?;

        // Indices.
        let index_changed = !(self.indices.created.is_empty() && self.indices.dropped.is_empty());
        if index_changed {
            writeln!(out, "Index changes:")?;
            for name in &self.indices.created {
                writeln!(out, "  + created {name}")?;
            }
            for name in &self.indices.dropped {
                writeln!(out, "  - dropped {name}")?;
            }
        } else {
            writeln!(out, "Index changes: none")?;
        }
        writeln!(out)?;

        // Version log.
        writeln!(
            out,
            "Versions in range (v{}, v{}]:",
            self.from_version.min(self.to_version),
            self.from_version.max(self.to_version)
        )?;
        if self.versions.is_empty() {
            writeln!(out, "  (none)")?;
        } else {
            for v in &self.versions {
                let ts = v.timestamp.format("%Y-%m-%dT%H:%M:%SZ");
                let msg = v.message.as_deref().unwrap_or("");
                writeln!(out, "  v{}  {ts}  {msg}", v.version)?;
            }
        }
        Ok(())
    }

    fn to_json(&self) -> Value {
        let schema = self.schema.to_json();
        let versions = self
            .versions
            .iter()
            .map(|v| {
                json!({
                    "version": v.version,
                    "timestamp": v.timestamp.to_rfc3339(),
                    "message": v.message,
                })
            })
            .collect::<Vec<_>>();

        json!({
            "from": { "branch": self.from_branch, "version": self.from_version },
            "to": { "branch": self.to_branch, "version": self.to_version },
            "identical": self.is_identical(),
            "rows": {
                "from": self.rows.from_rows,
                "to": self.rows.to_rows,
                "net": self.rows.to_rows as i64 - self.rows.from_rows as i64,
                "added": self.rows.added,
                "deleted": self.rows.deleted,
            },
            "schema": schema,
            "fragments": {
                "added": self.fragments.added,
                "removed": self.fragments.removed,
                "rewritten": self.fragments.rewritten,
            },
            "indices": {
                "created": self.indices.created,
                "dropped": self.indices.dropped,
            },
            "versions": versions,
        })
    }
}

fn fmt_ids(ids: &[u64]) -> String {
    let inner = ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn frag(id: u64, physical: u64, deleted: u64, files: &[&str]) -> FragmentInfo {
        FragmentInfo {
            id,
            physical_rows: physical,
            deleted_rows: deleted,
            num_files: files.len() as u64,
            files: files.iter().map(|s| s.to_string()).collect(),
            size: None,
        }
    }

    #[test]
    fn row_delta_splits_append_and_delete() {
        // from: fragment 0 (2 rows). to: fragment 0 gained a tombstone, plus a
        // new fragment 1 with 1 row → +1 added, -1 deleted, net 0.
        let from = vec![frag(0, 2, 0, &["a.lance"])];
        let to = vec![frag(0, 2, 1, &["a.lance"]), frag(1, 1, 0, &["b.lance"])];
        let d = build_row_delta(&from, &to);
        assert_eq!(d.from_rows, 2);
        assert_eq!(d.to_rows, 2);
        assert_eq!(d.added, 1);
        assert_eq!(d.deleted, 1);
    }

    /// Asserts the identity that drives the `diff(1)` exit code: the split must
    /// always reconcile with the net live-row change.
    fn assert_row_invariant(d: &RowDelta) {
        assert_eq!(
            d.added as i64 - d.deleted as i64,
            d.to_rows as i64 - d.from_rows as i64,
            "added - deleted must equal net"
        );
    }

    #[test]
    fn row_delta_counts_restored_tombstones_as_added() {
        // A version `restore` un-deletes rows on a surviving fragment: from had
        // 2 tombstones (7 live), to has 0 (9 live) → +2 added, 0 deleted.
        let from = vec![frag(0, 9, 2, &["a.lance"])];
        let to = vec![frag(0, 9, 0, &["a.lance"])];
        let d = build_row_delta(&from, &to);
        assert_eq!(d.from_rows, 7);
        assert_eq!(d.to_rows, 9);
        assert_eq!(d.added, 2);
        assert_eq!(d.deleted, 0);
        assert_row_invariant(&d);
    }

    #[test]
    fn row_delta_reversed_range_is_symmetric() {
        // The reverse of the append+delete case: fragment 1 disappears (deleted)
        // and fragment 0's tombstone is undone (added).
        let from = vec![frag(0, 2, 1, &["a.lance"]), frag(1, 1, 0, &["b.lance"])];
        let to = vec![frag(0, 2, 0, &["a.lance"])];
        let d = build_row_delta(&from, &to);
        assert_eq!(d.from_rows, 2);
        assert_eq!(d.to_rows, 2);
        assert_eq!(d.added, 1); // fragment 0 un-tombstoned
        assert_eq!(d.deleted, 1); // fragment 1 removed
        assert_row_invariant(&d);
    }

    #[test]
    fn row_delta_pure_append() {
        let from = vec![frag(0, 2, 0, &["a.lance"])];
        let to = vec![frag(0, 2, 0, &["a.lance"]), frag(1, 3, 0, &["b.lance"])];
        let d = build_row_delta(&from, &to);
        assert_eq!(d.added, 3);
        assert_eq!(d.deleted, 0);
        assert_eq!(d.to_rows - d.from_rows, 3);
    }

    #[test]
    fn fragment_delta_added_removed_rewritten() {
        // 0 survives with an extra data file (rewritten); 1 removed; 2 added.
        let from = vec![frag(0, 2, 0, &["0/a.lance"]), frag(1, 1, 0, &["1/a.lance"])];
        let to = vec![
            frag(0, 2, 0, &["0/a.lance", "0/b.lance"]),
            frag(2, 3, 0, &["2/a.lance"]),
        ];
        let d = build_fragment_delta(&from, &to);
        assert_eq!(d.added, vec![2]);
        assert_eq!(d.removed, vec![1]);
        assert_eq!(d.rewritten, vec![0]);
    }

    #[test]
    fn fragment_delta_tombstone_only_is_not_rewritten() {
        let from = vec![frag(0, 2, 0, &["0/a.lance"])];
        let to = vec![frag(0, 2, 1, &["0/a.lance"])];
        let d = build_fragment_delta(&from, &to);
        assert!(d.added.is_empty() && d.removed.is_empty() && d.rewritten.is_empty());
    }

    #[test]
    fn index_delta_created_dropped() {
        let idx = |name: &str| IndexInfo {
            name: name.to_string(),
            uuid: "u".to_string(),
            index_type: "BTree".to_string(),
            columns: vec!["id".to_string()],
            dataset_version: 1,
            created_at: None,
        };
        let from = vec![idx("old")];
        let to = vec![idx("new")];
        let d = build_index_delta(&from, &to);
        assert_eq!(d.created, vec!["new".to_string()]);
        assert_eq!(d.dropped, vec!["old".to_string()]);
    }

    fn endpoint(version: u64, schema: SchemaRef, fragments: Vec<FragmentInfo>) -> Endpoint {
        Endpoint {
            branch: MAIN_BRANCH.to_string(),
            version,
            schema,
            fragments,
            indices: vec![],
        }
    }

    #[test]
    fn identical_endpoints_report_no_differences() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let frags = vec![frag(0, 2, 0, &["a.lance"])];
        let from = endpoint(3, schema.clone(), frags.clone());
        let to = endpoint(3, schema, frags);
        let report = build_report(&from, &to, vec![]);
        assert!(report.is_identical());
        let mut buf: Vec<u8> = Vec::new();
        report.write_human(&mut buf, "ds.lance").unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("No differences."), "got: {text}");
    }

    #[test]
    fn different_endpoints_are_not_identical() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let from = endpoint(1, schema.clone(), vec![frag(0, 2, 0, &["a.lance"])]);
        let to = endpoint(
            2,
            schema,
            vec![frag(0, 2, 0, &["a.lance"]), frag(1, 1, 0, &["b.lance"])],
        );
        let report = build_report(&from, &to, vec![]);
        assert!(!report.is_identical());
        let v = report.to_json();
        assert_eq!(v["identical"], json!(false));
        assert_eq!(v["rows"]["added"], json!(1));
        assert_eq!(v["fragments"]["added"], json!([1]));
    }

    #[test]
    fn restore_endpoints_are_not_identical() {
        // Same fragment id + files, but tombstones removed (a restore). The only
        // change is the live-row count; this must not read as identical.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let from = endpoint(2, schema.clone(), vec![frag(0, 9, 2, &["a.lance"])]);
        let to = endpoint(3, schema, vec![frag(0, 9, 0, &["a.lance"])]);
        let report = build_report(&from, &to, vec![]);
        assert!(!report.is_identical());
        assert_eq!(report.rows.added, 2);
        assert_eq!(report.rows.deleted, 0);
        let mut buf: Vec<u8> = Vec::new();
        report.write_human(&mut buf, "ds.lance").unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            !text.contains("No differences."),
            "restore must not report identical: {text}"
        );
    }
}
