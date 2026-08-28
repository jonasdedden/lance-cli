//! The Lance dataset handle every command is written against.
//!
//! [`LanceDataset`] wraps `lance::Dataset` and is the single point where Lance
//! types are touched: it exposes the row-producing surface (`scan`, `take`,
//! `count_rows`), the catalog surface (versions, branches, tags, indices,
//! fragments), vector search, and blob extraction. Everything above it —
//! commands, projection, output — deals only in Arrow types and the plain
//! structs declared below.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::{
    Array, ArrayRef, Float32Array, RecordBatch, RecordBatchReader, StructArray, make_array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use lance::Dataset as InnerLance;
use lance::dataset::{BlobFile, ProjectionRequest};
use lance_index::DatasetIndexExt as _;
use lance_index::vector::DIST_COL;

use crate::Result;
use crate::cli::VersionArgs;
use crate::error::Error;
use crate::row_id::RowIds;

/// The name Lance's implicit default branch is surfaced under. Lance stores it
/// as `None` internally; this module normalises that to `"main"`.
pub const MAIN_BRANCH: &str = "main";

/// One fragment's data files as `(relative path, size cached in the manifest)`.
/// A `None` size means the manifest did not record one and it has to be fetched
/// from the object store.
type FragmentFiles = Vec<(String, Option<u64>)>;

/// Max in-flight object-store `size` lookups when computing fragment sizes.
/// Fragments are typically backed by a single data file, so this bounds the
/// number of concurrent `head`-style requests to a remote store.
const SIZE_CONCURRENCY: usize = 16;

/// Stream of `RecordBatch` results produced by a scan.
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Options controlling a [`LanceDataset::scan`].
///
/// Passed as a struct (rather than a growing list of positional parameters) so
/// that new knobs can be added without churning every call site. Today it
/// carries a column projection, an optional row predicate, and which system
/// pseudo-columns to append. The borrowed fields keep the struct cheap to `Copy`
/// and construct inline at each command.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScanOptions<'a> {
    /// Columns to include, in the given order. `None` means all columns.
    pub projection: Option<&'a [String]>,
    /// SQL-style predicate. Only matching rows are produced, and the filter is
    /// applied *before* any positional selection the command performs. `None`
    /// means no filtering.
    pub filter: Option<&'a str>,
    /// Which `_rowid` / `_rowaddr` pseudo-columns to append to each batch.
    pub row_ids: RowIds,
}

/// The resolved branch and version of an opened dataset handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutState {
    pub branch: String,
    pub version: u64,
}

/// Parameters for a `lance-cli search` nearest-neighbor query.
#[derive(Debug)]
pub struct VectorSearchParams<'a> {
    /// Vector column to search (a `FixedSizeList` of f16/f32/f64).
    pub column: &'a str,
    /// Query vector. Parsed from JSON and carried as f32 (Lance casts it to the
    /// column's element type); validated against the column width.
    pub vector: &'a [f32],
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// IVF partitions to probe (`None` → Lance default). No effect without an index.
    pub nprobes: Option<usize>,
    /// Refine factor for re-ranking (`None` → no refinement).
    pub refine_factor: Option<u32>,
    /// Output column projection; `_distance` is always appended regardless.
    pub projection: Option<&'a [String]>,
}

/// Outcome of a vector search: the output schema (including the trailing
/// `_distance` column), the row stream, and whether an ANN index was used.
pub struct VectorSearchResult {
    pub schema: SchemaRef,
    pub stream: BatchStream,
    /// `false` when no ANN index covers the column and flat KNN was used.
    pub used_index: bool,
}

/// One row in `lance-cli versions` output.
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: u64,
    pub timestamp: DateTime<Utc>,
    pub tag: Option<String>,
    pub message: Option<String>,
}

/// One row in `lance-cli branches` output.
#[derive(Debug, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub parent_branch: Option<String>,
    pub parent_version: Option<u64>,
    pub created_at: Option<DateTime<Utc>>,
}

/// One row in `lance-cli indices` output.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    /// Index type as Lance reports it (e.g. `BTree`, `IVF_PQ`, `INVERTED`).
    pub index_type: String,
    pub uuid: String,
    pub columns: Vec<String>,
    pub dataset_version: u64,
    pub created_at: Option<DateTime<Utc>>,
}

/// One row in `lance-cli stats` output: summary statistics for a single column.
///
/// Statistics that don't apply to a column's type are `None` and render as a
/// blank cell. `count` is the number of non-null values (as in `df.describe()`),
/// so `count + nulls` is the total number of rows considered.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name.
    pub column: String,
    /// Human-readable arrow type (e.g. `Int32`, `Timestamp(Microsecond, Some("UTC"))`).
    pub data_type: String,
    /// Number of non-null values.
    pub count: u64,
    /// Number of null values.
    pub nulls: u64,
    /// Minimum value, pre-formatted for display. Numeric, temporal, string, and
    /// boolean columns only.
    pub min: Option<String>,
    /// Maximum value, pre-formatted for display. Same type coverage as `min`.
    pub max: Option<String>,
    /// Arithmetic mean. Numeric columns only. `NaN` when the column contains any
    /// `NaN` (matching numpy's plain mean).
    pub mean: Option<f64>,
    /// Sample standard deviation (ddof = 1). Numeric columns with at least two
    /// non-null values only.
    pub stddev: Option<f64>,
    /// Distinct-value count, either exact (e.g. `42`) or a capped marker
    /// (e.g. `>10000`) once cardinality exceeds the tracking cap.
    pub distinct: Option<String>,
}

/// One row in `lance-cli index-stats` output.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub name: String,
    /// Index type as Lance reports it (e.g. `BTree`, `IVF_PQ`).
    pub index_type: String,
    /// Rows currently covered by the index.
    pub indexed_rows: u64,
    /// Rows appended after the index was built and not yet reindexed.
    pub unindexed_rows: u64,
    /// Raw Lance statistics JSON string, passed through for type-specific
    /// internals. Emitted verbatim in `jsonl` output; omitted from table/csv.
    pub detail: String,
}

impl IndexStats {
    /// Fraction of rows covered by the index in `0.0..=1.0`, or `None` when the
    /// index has no rows at all (coverage is undefined).
    pub fn coverage(&self) -> Option<f64> {
        let total = self.indexed_rows + self.unindexed_rows;
        (total > 0).then(|| self.indexed_rows as f64 / total as f64)
    }
}

/// One row in `lance-cli tags` output.
#[derive(Debug, Clone)]
pub struct TagInfo {
    pub name: String,
    pub branch: String,
    pub version: u64,
}

/// One row in `lance-cli fragments` output.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    /// Fragment id, unique and stable within the dataset.
    pub id: u64,
    /// Rows physically stored in the fragment, ignoring deletions.
    pub physical_rows: u64,
    /// Rows tombstoned by the fragment's deletion file (0 when there is none).
    pub deleted_rows: u64,
    /// Number of data files backing the fragment.
    pub num_files: u64,
    /// Relative paths of the fragment's data files.
    pub files: Vec<String>,
    /// Summed on-disk size of the data files in bytes, or `None` when size
    /// computation was skipped (see [`LanceDataset::list_fragments`]).
    pub size: Option<u64>,
}

/// Open the Lance dataset at `input`, optionally checking out a specific
/// branch/version/tag. `input` is either a local path (`/data/foo.lance`, with
/// or without a `file://` prefix) or an object-store URI (`s3://…`, `gs://…`,
/// `az://…`).
///
/// A scheme-less path that is not a Lance dataset directory is rejected here,
/// before Lance is asked to open it, so the error names the real problem
/// instead of surfacing an object-store miss.
pub async fn open(input: &str, version: Option<&VersionArgs>) -> Result<Arc<LanceDataset>> {
    if !has_scheme(input) && !is_lance_dataset(input) {
        return Err(Error::NotLanceDataset {
            path: input.to_string(),
        });
    }
    Ok(Arc::new(LanceDataset::open(input, version).await?))
}

/// True when `input` starts with a URI scheme followed by `://`
/// (e.g. `s3://bucket/…`, `file:///data/…`). A bare Windows drive letter such
/// as `C:\data` has no `//` and is therefore correctly treated as a local path.
///
/// Exposed to the command layer so `cat`'s glob expansion can leave remote URIs
/// untouched (globbing is local-filesystem only).
pub(crate) fn has_scheme(input: &str) -> bool {
    let Some((scheme, _rest)) = input.split_once("://") else {
        return false;
    };
    !scheme.is_empty()
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c: char| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// A Lance dataset is a directory that contains a `_versions/` subfolder.
/// (`_transactions/` is the other typical marker but not always present in
/// freshly written datasets.)
fn is_lance_dataset(input: &str) -> bool {
    let p = Path::new(input);
    p.is_dir() && p.join("_versions").is_dir()
}

/// A handle on one checked-out version of a Lance dataset.
#[derive(Debug)]
pub struct LanceDataset {
    inner: InnerLance,
    origin: String,
    arrow_schema: SchemaRef,
}

impl LanceDataset {
    /// Open the Lance dataset at `input`. `input` is passed to Lance verbatim,
    /// so it may be a local path (with or without a `file://` prefix) or an
    /// object-store URI (`s3://…`, `gs://…`, `az://…`); credentials are taken
    /// from the ambient environment. On failure the error carries `input` and
    /// the underlying object-store cause.
    pub async fn open(input: &str, version: Option<&VersionArgs>) -> Result<Self> {
        let inner = InnerLance::open(input)
            .await
            .map_err(|e| Error::LanceOpen {
                path: input.to_string(),
                source: Box::new(e),
            })?;
        let inner = apply_checkout(inner, version).await?;
        let arrow_schema: SchemaRef = Arc::new(ArrowSchema::from(inner.schema()));
        Ok(Self {
            inner,
            origin: input.to_string(),
            arrow_schema,
        })
    }

    fn projection_request(&self, projection: Option<&[String]>) -> ProjectionRequest {
        match projection {
            Some(cols) => ProjectionRequest::from_columns(cols.iter(), self.inner.schema()),
            None => ProjectionRequest::from_schema(self.inner.schema().clone()),
        }
    }

    /// Fetch and parse the Lance statistics JSON for one index, returning both
    /// the raw string (for pass-through) and the parsed value (for field
    /// extraction). `index_statistics` is the stable public surface Lance
    /// exposes for index type and coverage counts.
    async fn load_index_statistics(&self, name: &str) -> Result<(String, serde_json::Value)> {
        let raw = self
            .inner
            .index_statistics(name)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let value = serde_json::from_str(&raw)?;
        Ok((raw, value))
    }

    /// Parse `predicate` against the dataset schema without running a scan,
    /// mapping any failure to [`Error::InvalidPredicate`]. Used to give a
    /// filtered `count_rows` the same clear error a `scan` would produce.
    fn validate_predicate(&self, predicate: &str) -> Result<()> {
        let mut scanner = self.inner.scan();
        // `filter()` only stores the SQL string today, but map it to
        // `InvalidPredicate` too so a future eager-parsing Lance keeps the
        // context. `get_expr_filter()` forces the parse against the schema.
        scanner
            .filter(predicate)
            .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        scanner
            .get_expr_filter()
            .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        Ok(())
    }

    /// Path or URI the dataset was opened from.
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Logical arrow schema of the dataset.
    pub fn arrow_schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    /// Pretty-printed Lance-native schema (for `schema --type physical`),
    /// optionally projected to a subset of columns.
    pub fn physical_schema_debug(&self, projection: Option<&[String]>) -> Result<String> {
        match projection {
            None => Ok(format!("{:#?}", self.inner.schema())),
            Some(cols) => {
                let projected = self
                    .inner
                    .schema()
                    .project(cols)
                    .map_err(|e| Error::Lance(Box::new(e)))?;
                Ok(format!("{projected:#?}"))
            }
        }
    }

    /// Total row count, optionally restricted to rows matching `filter` (a
    /// SQL-style predicate). Counted through a scalar index when Lance has one
    /// covering the predicate, rather than by scanning.
    pub async fn count_rows(&self, filter: Option<&str>) -> Result<u64> {
        // Validate the predicate up front so a bad `--where` surfaces as an
        // `InvalidPredicate` rather than an opaque count failure. Lance pushes
        // the filter into scalar indices when available, so this stays cheap.
        if let Some(pred) = filter {
            self.validate_predicate(pred)?;
        }
        let n = self
            .inner
            .count_rows(filter.map(str::to_owned))
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        Ok(n as u64)
    }

    /// Stream rows according to `options` (projection + optional filter +
    /// pseudo-columns).
    pub async fn scan(&self, options: &ScanOptions<'_>) -> Result<BatchStream> {
        let mut scanner = self.inner.scan();
        if let Some(cols) = options.projection {
            scanner
                .project(cols)
                .map_err(|e| Error::Lance(Box::new(e)))?;
        }
        if let Some(pred) = options.filter {
            // `filter()` only stores the SQL string; force an eager parse via
            // `get_expr_filter()` so an invalid predicate is reported here with
            // context instead of failing deep inside the stream.
            scanner
                .filter(pred)
                .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
            scanner
                .get_expr_filter()
                .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        }
        // Append the system pseudo-columns last (after any projection). Lance
        // emits `_rowid` before `_rowaddr`; `Scanner::with_row_id`/
        // `with_row_address` add them to the already-set projection plan.
        if options.row_ids.with_row_id {
            scanner.with_row_id();
        }
        if options.row_ids.with_row_addr {
            scanner.with_row_address();
        }
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = stream.map(|r| r.map_err(|e| Error::Lance(Box::new(e))));
        Ok(Box::pin(stream))
    }

    /// Materialise a `RecordBatch` containing only the rows at the given
    /// indices, in the order given. `indices` must all be < `count_rows()`.
    /// `row_ids` selects the `_rowid` / `_rowaddr` pseudo-columns to append,
    /// matching the order and shape the streaming [`Self::scan`] produces.
    pub async fn take(
        &self,
        indices: &[u64],
        projection: Option<&[String]>,
        row_ids: RowIds,
    ) -> Result<RecordBatch> {
        // No pseudo-columns: the original fast path, untouched.
        if !row_ids.any() {
            let req = self.projection_request(projection);
            let batch = self
                .inner
                .take(indices, req)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?;
            // Lance's own `take` returns nested projections as *pruned structs*,
            // whereas the scan path (head/cat/…) returns them as *flat,
            // dotted-named leaf columns*. Flatten here so every command surfaces
            // the same shape (and matches the header built by
            // `project_arrow_schema`).
            return match projection {
                Some(cols) if cols.iter().any(|c| is_nested_path(&self.arrow_schema, c)) => {
                    flatten_nested_projection(&batch, &self.arrow_schema, cols)
                }
                _ => Ok(batch),
            };
        }

        // With pseudo-columns: request the projected columns plus the system
        // columns in one `take` (`ProjectionRequest::from_columns` preserves
        // system columns), then reassemble into the canonical
        // `[projected…, _rowid, _rowaddr]` order so the output matches the
        // streaming scan and the writer header exactly — rather than trusting
        // Lance's internal column placement.
        let base_cols: Vec<String> = match projection {
            Some(cols) => cols.to_vec(),
            None => self
                .arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        let system = row_ids.columns();
        let mut requested = base_cols.clone();
        requested.extend(system.iter().map(|s| (*s).to_string()));
        // `from_columns` calls `Schema::project_preserve_system_columns`, which
        // Lance internally `.unwrap()`s — a bad column name would panic rather
        // than error. That is safe here because every entry is either a real
        // column already validated by the projection resolver, or a `_rowid` /
        // `_rowaddr` name produced by `RowIds` (never user text), so the
        // projection can never fail.
        let req = ProjectionRequest::from_columns(requested.iter(), self.inner.schema());
        let batch = self
            .inner
            .take(indices, req)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        assemble_take_output(&batch, &self.arrow_schema, &base_cols, &system)
    }

    /// Manifest (dataset) version number of the currently checked-out version.
    /// Pure metadata already resident after `open`, so this is infallible and
    /// synchronous. Used by the `stat` command's `format` line.
    pub fn manifest_version(&self) -> u64 {
        self.inner.version().version
    }

    /// List versions on `branch` (defaults to `main` when `None`). When
    /// `tagged_only` is true, drops untagged versions from the result.
    pub async fn list_versions(
        &self,
        branch: Option<&str>,
        tagged_only: bool,
    ) -> Result<Vec<VersionInfo>> {
        // Use a branch-scoped clone so the dataset's own active version isn't disturbed.
        let scoped = match branch {
            Some(b) if b != MAIN_BRANCH => self
                .inner
                .clone()
                .checkout_branch(b)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?,
            _ => self.inner.clone(),
        };
        let target_branch = branch.unwrap_or(MAIN_BRANCH);

        let versions = scoped
            .versions()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;

        // Tags are dataset-wide; group them by version *for this branch* so we
        // can attach a `tag` (or comma-joined tags) to each VersionInfo row.
        let tags = self
            .inner
            .tags()
            .list()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let mut tags_for_version: HashMap<u64, Vec<String>> = HashMap::new();
        for (name, content) in tags {
            let content_branch = content.branch.as_deref().unwrap_or(MAIN_BRANCH);
            if content_branch == target_branch {
                tags_for_version
                    .entry(content.version)
                    .or_default()
                    .push(name);
            }
        }

        let mut out: Vec<VersionInfo> = versions
            .into_iter()
            .map(|v| {
                let mut tag_names = tags_for_version.remove(&v.version).unwrap_or_default();
                tag_names.sort();
                let tag = if tag_names.is_empty() {
                    None
                } else {
                    Some(tag_names.join(","))
                };
                let message = v.metadata.get("message").cloned();
                VersionInfo {
                    version: v.version,
                    timestamp: v.timestamp,
                    tag,
                    message,
                }
            })
            .collect();

        if tagged_only {
            out.retain(|v| v.tag.is_some());
        }
        Ok(out)
    }

    /// List every branch the dataset has, including the default `main`.
    pub async fn list_branches(&self) -> Result<Vec<BranchInfo>> {
        let map = self
            .inner
            .list_branches()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;

        // Lance stores `parent_branch: None` to mean "main" (main is the
        // implicit default branch). Normalise the display so users don't see
        // null on every branch that was forked from main.
        let mut out: Vec<BranchInfo> = map
            .into_iter()
            .map(|(name, content)| BranchInfo {
                name,
                parent_branch: Some(
                    content
                        .parent_branch
                        .unwrap_or_else(|| MAIN_BRANCH.to_string()),
                ),
                parent_version: Some(content.parent_version),
                created_at: unix_seconds_to_utc(content.create_at),
            })
            .collect();

        // `list_branches()` skips the implicit default branch — surface it
        // explicitly so the CLI shows a complete picture. Main has no parent;
        // `created_at` is taken from v1's manifest timestamp as a proxy for
        // "when main came into existence".
        if !out.iter().any(|b| b.name == MAIN_BRANCH) {
            let main_inner = self
                .inner
                .clone()
                .checkout_branch(MAIN_BRANCH)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?;
            let main_created_at = main_inner
                .versions()
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?
                .into_iter()
                .next()
                .map(|v| v.timestamp);
            out.insert(
                0,
                BranchInfo {
                    name: MAIN_BRANCH.to_string(),
                    parent_branch: None,
                    parent_version: None,
                    created_at: main_created_at,
                },
            );
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// List every tag in the dataset, regardless of branch.
    pub async fn list_tags(&self) -> Result<Vec<TagInfo>> {
        let tags = self
            .inner
            .tags()
            .list()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let mut out: Vec<TagInfo> = tags
            .into_iter()
            .map(|(name, content)| TagInfo {
                name,
                branch: content.branch.unwrap_or_else(|| MAIN_BRANCH.to_string()),
                version: content.version,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// List indices defined on the active version of the dataset.
    pub async fn list_indices(&self) -> Result<Vec<IndexInfo>> {
        let indices = self
            .inner
            .load_indices()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let schema = self.inner.schema();

        let mut out = Vec::with_capacity(indices.len());
        for m in indices.iter() {
            let columns = m
                .fields
                .iter()
                .map(|id| {
                    schema
                        .field_by_id(*id)
                        .map(|f| f.name.clone())
                        .unwrap_or_else(|| format!("<field_id={id}>"))
                })
                .collect();
            // The index type isn't on the loaded metadata; it lives in the
            // statistics JSON. Index counts are small, so a call per index is
            // fine (the issue explicitly sanctions this). A single index whose
            // statistics can't be loaded must not fail the whole listing, so
            // degrade that row to `UNKNOWN` rather than propagating the error.
            let index_type = match self.load_index_statistics(&m.name).await {
                Ok((_, stats)) => index_type_of(&stats),
                Err(_) => "UNKNOWN".to_string(),
            };
            out.push(IndexInfo {
                name: m.name.clone(),
                index_type,
                uuid: m.uuid.to_string(),
                columns,
                dataset_version: m.dataset_version,
                created_at: m.created_at,
            });
        }
        Ok(out)
    }

    /// List the physical fragments of the active version of the dataset.
    ///
    /// Row counts, deletion counts and file lists come straight from the
    /// manifest, so this stays fast regardless of dataset size. When
    /// `with_size` is true, each fragment's on-disk byte size is also
    /// computed — from the manifest when the size is cached there, otherwise
    /// via concurrent object-store lookups. Pass `false` to skip that entirely
    /// (leaving `FragmentInfo::size` as `None`) for very remote or huge datasets.
    pub async fn list_fragments(&self, with_size: bool) -> Result<Vec<FragmentInfo>> {
        let fragments = self.inner.get_fragments();

        // Everything except size comes straight from the manifest, so no I/O
        // happens on the common path. We only fall back to a per-fragment await
        // for legacy fragments whose manifest omits the row/deletion counts.
        // Those fallback awaits (`physical_rows()` / `count_deletions()`) are
        // untested by construction: lance 4.0 always populates these fields for
        // freshly written datasets, so the test suite never reaches them.
        let mut out: Vec<FragmentInfo> = Vec::with_capacity(fragments.len());
        for frag in &fragments {
            let meta = frag.metadata();
            let physical_rows = match meta.physical_rows {
                Some(n) => n as u64,
                None => frag
                    .physical_rows()
                    .await
                    .map_err(|e| Error::Lance(Box::new(e)))? as u64,
            };
            let deleted_rows = match &meta.deletion_file {
                None => 0,
                Some(df) => match df.num_deleted_rows {
                    Some(n) => n as u64,
                    None => frag
                        .count_deletions()
                        .await
                        .map_err(|e| Error::Lance(Box::new(e)))? as u64,
                },
            };
            let files: Vec<String> = meta.files.iter().map(|f| f.path.clone()).collect();
            out.push(FragmentInfo {
                id: meta.id,
                physical_rows,
                deleted_rows,
                num_files: files.len() as u64,
                files,
                size: None,
            });
        }

        if with_size {
            let data_dir = self.inner.data_dir();
            let object_store = self.inner.object_store();

            // Collect owned `(relative path, cached size)` specs up front so the
            // concurrent closures below borrow nothing from `fragments` — that
            // keeps the async blocks free of the higher-ranked lifetimes that
            // `buffer_unordered` otherwise can't satisfy.
            let specs: Vec<(usize, FragmentFiles)> = fragments
                .iter()
                .enumerate()
                .map(|(i, frag)| {
                    let files = frag
                        .metadata()
                        .files
                        .iter()
                        .map(|f| (f.path.clone(), f.file_size_bytes.get().map(|n| n.get())))
                        .collect();
                    (i, files)
                })
                .collect();

            // Prefer the size cached in the manifest; only hit the object store
            // for files that don't record it. Requests run concurrently, keyed
            // by index so results can be reassembled after `buffer_unordered`
            // returns them out of order. The object-store `size()` fallback is
            // untested by construction: lance 4.0 records `file_size_bytes` in
            // the manifest for fresh datasets, so tests take the cached branch.
            let sized: Vec<(usize, u64)> = futures::stream::iter(specs)
                .map(|(i, files)| {
                    let data_dir = &data_dir;
                    async move {
                        let mut total = 0u64;
                        for (path, cached) in files {
                            match cached {
                                Some(sz) => total += sz,
                                None => {
                                    let object_path = data_dir.child(path.as_str());
                                    total += object_store
                                        .size(&object_path)
                                        .await
                                        .map_err(|e| Error::Lance(Box::new(e)))?;
                                }
                            }
                        }
                        Ok::<(usize, u64), Error>((i, total))
                    }
                })
                .buffer_unordered(SIZE_CONCURRENCY)
                .try_collect()
                .await?;
            for (i, total) in sized {
                out[i].size = Some(total);
            }
        }

        Ok(out)
    }

    /// Nearest-neighbor vector search over a `FixedSizeList`-of-float column.
    ///
    /// Uses an ANN index when one exists on the column and falls back to flat
    /// (brute-force) KNN otherwise; `VectorSearchResult::used_index` reports
    /// which path was taken. The query vector is validated against the column
    /// width and cast to the column's element type.
    pub async fn search(&self, params: &VectorSearchParams<'_>) -> Result<VectorSearchResult> {
        // Validate the target column ourselves so the error messages are precise
        // (`query has 512 dims, column embedding has 768`) rather than relying on
        // Lance's internal wording.
        let dim = vector_column_dim(&self.arrow_schema, params.column)?;
        if params.vector.len() != dim {
            return Err(Error::VectorDimMismatch {
                query: params.vector.len(),
                column: params.column.to_string(),
                column_dims: dim,
            });
        }

        // Lance coerces from a Float32Array to the column's f16/f32/f64 element
        // type internally, so f32 is the interchange type for the query vector.
        let query = Float32Array::from(params.vector.to_vec());

        let mut scanner = self.inner.scan();
        scanner
            .nearest(params.column, &query, params.k)
            .map_err(|e| Error::Lance(Box::new(e)))?;
        if let Some(n) = params.nprobes {
            scanner.nprobes(n);
        }
        if let Some(factor) = params.refine_factor {
            scanner.refine(factor);
        }

        // Build the projection explicitly and opt out of Lance's deprecated
        // scoring autoprojection (which silently appends `_distance` today but
        // is slated to stop). We always force-include `_distance` ourselves —
        // it is a recognised system column, resolved against the search output
        // schema — so it is present regardless of the user's `--columns`.
        scanner.disable_scoring_autoprojection();
        let mut projection: Vec<String> = match params.projection {
            Some(cols) => cols.to_vec(),
            None => self
                .arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        if !projection.iter().any(|c| c == DIST_COL) {
            projection.push(DIST_COL.to_string());
        }
        scanner
            .project(&projection)
            .map_err(|e| Error::Lance(Box::new(e)))?;

        let used_index = self.column_has_ann_index(params.column).await?;

        // `schema()` reflects the projection plus the trailing `_distance` column.
        let schema = scanner
            .schema()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = stream.map(|r| r.map_err(|e| Error::Lance(Box::new(e))));

        Ok(VectorSearchResult {
            schema,
            stream: Box::pin(stream),
            used_index,
        })
    }

    /// True when `column` is a Lance blob-encoded column (`lance-encoding:blob`
    /// field metadata). Such columns store payloads too large to materialize
    /// through a normal scan/`take`, so the `blob` command reads them via
    /// [`Self::open_blob`] instead. Returns `false` for a missing column too;
    /// the caller validates existence separately against the arrow schema.
    pub fn is_blob_column(&self, column: &str) -> bool {
        self.inner
            .schema()
            .field(column)
            .map(|f| f.is_blob())
            .unwrap_or(false)
    }

    /// Open a streaming reader over the blob payload at row offset `index` in
    /// the blob-encoded `column`. `index` is a resolved (non-negative) offset.
    /// `Ok(None)` means the cell is null (no payload to extract). Bytes are
    /// pulled lazily so multi-GB payloads never need to be held in memory.
    pub async fn open_blob(&self, column: &str, index: u64) -> Result<Option<BlobReader>> {
        // `take_blobs*` is defined on `Arc<Dataset>`; cloning the inner handle is
        // cheap (Lance datasets are internally reference-counted).
        let ds = Arc::new(self.inner.clone());
        let mut blobs = ds
            .take_blobs_by_indices(&[index], column)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        // Null-cell detection. Lance's blob descriptors do not preserve the
        // null-vs-empty distinction: a null cell is encoded as a zero-length
        // payload (`size == 0`), the same as a genuinely empty blob. Depending on
        // the descriptor shape `take_blobs` either drops the row (empty vec) or
        // hands back a zero-length `BlobFile`; both mean "nothing to extract", so
        // filter them out and report `None`. The command turns that into a clear
        // error and writes no (empty) file.
        Ok(blobs.pop().filter(|f| f.size() > 0).map(BlobReader))
    }

    /// Per-index coverage statistics: indexed vs unindexed row counts (which
    /// diverge as rows are appended after an index is built), plus the raw
    /// Lance statistics JSON so callers can pass through type-specific internals
    /// (IVF partitions, PQ sub-vectors, …) without lance-cli understanding them.
    pub async fn index_stats(&self) -> Result<Vec<IndexStats>> {
        let indices = self
            .inner
            .load_indices()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;

        let mut out = Vec::with_capacity(indices.len());
        for m in indices.iter() {
            let (raw, stats) = self.load_index_statistics(&m.name).await?;
            out.push(IndexStats {
                name: m.name.clone(),
                index_type: index_type_of(&stats),
                indexed_rows: stats
                    .get("num_indexed_rows")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                unindexed_rows: stats
                    .get("num_unindexed_rows")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                detail: raw,
            });
        }
        Ok(out)
    }

    /// The `(branch, version)` this handle is currently checked out to.
    ///
    /// Read straight from the loaded manifest (no I/O). Used by `diff` to label
    /// each endpoint and to detect a cross-branch comparison after both handles
    /// have been opened and any tag/branch selectors resolved.
    pub fn checkout_state(&self) -> CheckoutState {
        let manifest = self.inner.manifest();
        CheckoutState {
            // Lance stores `branch: None` for the implicit default branch.
            branch: manifest
                .branch
                .clone()
                .unwrap_or_else(|| MAIN_BRANCH.to_string()),
            version: manifest.version,
        }
    }

    /// True when at least one index covers `column`. Vector columns only ever
    /// carry ANN indices, so index coverage is a reliable "is this indexed"
    /// signal for the flat-KNN stderr note.
    async fn column_has_ann_index(&self, column: &str) -> Result<bool> {
        let indices = self
            .inner
            .load_indices()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let schema = self.inner.schema();
        Ok(indices.iter().any(|m| {
            m.fields
                .iter()
                .filter_map(|id| schema.field_by_id(*id))
                .any(|f| f.name == column)
        }))
    }
}

/// Turn a Lance/DataFusion predicate-parse error into a concise message.
///
/// These errors bake a source location such as
/// `, /home/user/.cargo/registry/.../scanner.rs:422:33` into their `Display`;
/// strip that noise so the user sees only what is wrong with their SQL.
fn predicate_error_message<E: std::error::Error>(err: &E) -> String {
    strip_source_locations(&err.to_string())
}

fn strip_source_locations(msg: &str) -> String {
    let mut s = msg.trim_end();
    // Peel off one or more trailing ", <path>.rs:<line>:<col>" segments.
    while let Some(idx) = s.rfind(", ") {
        if looks_like_source_location(s[idx + 2..].trim()) {
            s = s[..idx].trim_end();
        } else {
            break;
        }
    }
    s.to_string()
}

fn looks_like_source_location(tail: &str) -> bool {
    // e.g. "/home/user/.cargo/registry/.../scanner.rs:422:33"
    tail.contains(".rs:")
        && tail.rsplit(':').take(2).filter(|s| !s.is_empty()).count() == 2
        && tail
            .rsplit(':')
            .take(2)
            .all(|seg| !seg.is_empty() && seg.chars().all(|c| c.is_ascii_digit()))
}

/// True when `entry` is a nested path (contains `.`) that does not name an
/// exact top-level column — i.e. it must be walked into a struct.
fn is_nested_path(schema: &ArrowSchema, entry: &str) -> bool {
    entry.contains('.') && schema.field_with_name(entry).is_err()
}

/// Rebuild `batch` (a pruned-struct projection as returned by Lance's `take`)
/// into the flat, dotted-name shape produced by the scan path, so `take` output
/// matches `head`/`cat` and the header from `project_arrow_schema`.
fn flatten_nested_projection(
    batch: &RecordBatch,
    schema: &ArrowSchema,
    projection: &[String],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(projection.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for entry in projection {
        fields.push(crate::projection::projected_field(schema, entry));
        columns.push(extract_column(batch, entry)?);
    }
    let out_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(out_schema, columns)?)
}

/// Reassemble a `take` result that includes system pseudo-columns into the
/// canonical output order: the projected columns (flattened to dotted leaves,
/// exactly like the scan path and `project_arrow_schema`), then the requested
/// `_rowid` / `_rowaddr` columns. `base_cols` is the user projection (or all
/// top-level names when unprojected); `system` names the pseudo-columns in
/// output order. Both kinds of column are pulled by name from `batch`.
fn assemble_take_output(
    batch: &RecordBatch,
    schema: &ArrowSchema,
    base_cols: &[String],
    system: &[&str],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(base_cols.len() + system.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(base_cols.len() + system.len());
    for entry in base_cols {
        fields.push(crate::projection::projected_field(schema, entry));
        columns.push(extract_column(batch, entry)?);
    }
    for name in system {
        let col = batch.column_by_name(name).ok_or_else(|| {
            Error::Lance(format!("take did not return system column '{name}'").into())
        })?;
        fields.push(Field::new(*name, DataType::UInt64, true));
        columns.push(col.clone());
    }
    let out_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(out_schema, columns)?)
}

/// Pull one projected column out of the pruned-struct `batch`. A top-level name
/// (even a literal dotted one present in the batch) is taken as-is; otherwise
/// the entry is a dotted path walked into the struct columns, with ancestor
/// null masks propagated onto the leaf (a null parent yields a null leaf, as the
/// scan path does).
fn extract_column(batch: &RecordBatch, entry: &str) -> Result<ArrayRef> {
    if let Some(col) = batch.column_by_name(entry) {
        return Ok(col.clone());
    }
    let mut segments = entry.split('.');
    let head = segments.next().expect("non-empty path");
    let mut current = batch
        .column_by_name(head)
        .expect("projection validated against schema")
        .clone();
    let mut nulls: Option<NullBuffer> = current.nulls().cloned();
    for seg in segments {
        let st = current
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("intermediate path segment is a struct");
        let child = st
            .column_by_name(seg)
            .expect("projection validated against schema")
            .clone();
        nulls = NullBuffer::union(nulls.as_ref(), child.nulls());
        current = child;
    }
    // Re-attach the accumulated ancestor validity onto the leaf.
    let data = current.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}

async fn apply_checkout(mut ds: InnerLance, version: Option<&VersionArgs>) -> Result<InnerLance> {
    let Some(args) = version else { return Ok(ds) };

    if let Some(tag) = &args.tag {
        // If the user also supplied --branch, verify the tag actually lives on
        // that branch rather than silently letting the tag's branch win.
        if let Some(requested) = &args.branch {
            let content = ds
                .tags()
                .get(tag)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?;
            let tag_branch = content.branch.as_deref().unwrap_or(MAIN_BRANCH);
            if tag_branch != requested.as_str() {
                return Err(Error::TagBranchMismatch {
                    tag: tag.clone(),
                    tag_branch: tag_branch.to_string(),
                    requested_branch: requested.clone(),
                });
            }
        }
        // `Ref::Tag` resolves both branch and version itself.
        ds = ds
            .checkout_version(tag.as_str())
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        return Ok(ds);
    }

    if let Some(branch) = &args.branch {
        ds = ds
            .checkout_branch(branch)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
    }
    if let Some(version) = args.version {
        ds = ds
            .checkout_version(version)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
    }
    // `--as-of` conflicts with `--version`/`--tag` at the clap layer, so it is
    // only ever reached after (an optional) branch checkout.
    if let Some(as_of) = &args.as_of {
        let target = parse_as_of(as_of)?;
        ds = checkout_as_of(ds, target, as_of).await?;
    }
    Ok(ds)
}

/// Format an instant at full precision (sub-second digits when present) with a
/// `Z` suffix. Commit timestamps carry nanoseconds, so seconds-truncation would
/// make the echoed/advised value round *down* to a different (earlier) version;
/// full precision keeps "paste this back" literally reproducible.
fn format_instant(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// Resolve `--as-of` against the (already branch-scoped) dataset: pick the
/// latest version whose commit timestamp is `<= target`, echo it on stderr for
/// reproducibility, and check that version out. Versions are returned in
/// ascending order, so a reverse scan finds the newest match first. `raw` is
/// the user's original input string, echoed verbatim in the out-of-range error
/// so re-passing it is never a self-truncating suggestion.
async fn checkout_as_of(ds: InnerLance, target: DateTime<Utc>, raw: &str) -> Result<InnerLance> {
    let versions = ds.versions().await.map_err(|e| Error::Lance(Box::new(e)))?;
    match versions.iter().rev().find(|v| v.timestamp <= target) {
        Some(chosen) => {
            eprintln!(
                "resolved --as-of to version {} ({})",
                chosen.version,
                format_instant(chosen.timestamp)
            );
            ds.checkout_version(chosen.version)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))
        }
        None => {
            // Every version is newer than `target`: the instant predates the
            // branch's history. Surface the earliest timestamp at full precision
            // so a user who passes it verbatim lands on (not before) v1.
            let earliest = versions
                .first()
                .map(|v| format_instant(v.timestamp))
                .unwrap_or_else(|| "<none>".to_string());
            Err(Error::AsOfBeforeFirstVersion {
                requested: raw.to_string(),
                earliest,
            })
        }
    }
}

/// Parse an `--as-of` value into a UTC instant.
///
/// Three formats are accepted, tried in order:
/// 1. RFC 3339 with an explicit offset (`2026-07-01T12:00:00Z`,
///    `2026-07-01T14:00:00+02:00`) — the offset is honoured and normalised to
///    UTC.
/// 2. A naive datetime with no offset (`2026-07-01T12:00:00`,
///    `2026-07-01T12:00`, or space-separated) — **interpreted as UTC**.
/// 3. A date with no time (`2026-07-01`) — interpreted as **midnight UTC**.
///
/// The naive-timezone rule (UTC, never local) keeps results reproducible
/// regardless of the machine running the CLI.
fn parse_as_of(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();

    // 1. RFC 3339 with an explicit offset.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // 2. Naive datetime (no offset), a few common spellings; interpreted as UTC.
    const NAIVE_DATETIME_FORMATS: &[&str] = &[
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ];
    for fmt in NAIVE_DATETIME_FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Utc.from_utc_datetime(&naive));
        }
    }

    // 3. Date only → midnight UTC.
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        // `and_hms_opt(0, 0, 0)` on a valid date is always in range.
        let naive = date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid time for any date");
        return Ok(Utc.from_utc_datetime(&naive));
    }

    Err(Error::InvalidAsOf(s.to_string()))
}

/// Streaming reader over a single blob cell's payload, backed by a Lance
/// [`BlobFile`]. `BlobFile` keeps its own cursor (behind interior mutability),
/// so [`Self::read_chunk`] advances it and reads bounded chunks from the
/// backing object store on demand — no whole-payload buffering, so extracting a
/// multi-GB payload stays within a fixed memory budget.
pub struct BlobReader(BlobFile);

impl BlobReader {
    /// Read up to `max` bytes from the current cursor, advancing it. Returns an
    /// empty buffer once the payload is exhausted.
    pub async fn read_chunk(&mut self, max: usize) -> Result<Vec<u8>> {
        let bytes = self
            .0
            .read_up_to(max)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        Ok(bytes.to_vec())
    }
}

/// Resolve `column` to the width of its `FixedSizeList`-of-float type, erroring
/// precisely when the column is missing or is not a float vector column.
fn vector_column_dim(schema: &ArrowSchema, column: &str) -> Result<usize> {
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
    match field.data_type() {
        DataType::FixedSizeList(inner, size)
            if matches!(
                inner.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) =>
        {
            Ok(*size as usize)
        }
        other => Err(Error::NotVectorColumn {
            column: column.to_string(),
            data_type: other.to_string(),
        }),
    }
}

/// Extract the `index_type` field from a Lance statistics JSON value, falling
/// back to `UNKNOWN` when Lance omits it (e.g. an unrecognised system index).
fn index_type_of(stats: &serde_json::Value) -> String {
    stats
        .get("index_type")
        .and_then(|v| v.as_str())
        .unwrap_or("UNKNOWN")
        .to_string()
}

fn unix_seconds_to_utc(seconds: u64) -> Option<DateTime<Utc>> {
    let secs = i64::try_from(seconds).ok()?;
    Utc.timestamp_opt(secs, 0).single()
}

/// Write a `RecordBatchReader` into a new Lance dataset at `path`.
///
/// Exposed for tests and external fixture builders; not used by the CLI itself.
pub async fn write_dataset<R>(path: &Path, reader: R) -> Result<()>
where
    R: RecordBatchReader + Send + 'static,
{
    let uri = path.to_string_lossy().into_owned();
    InnerLance::write(reader, uri.as_str(), None)
        .await
        .map_err(|e| Error::Lance(Box::new(e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::cast::AsArray as _;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Fields};

    use super::*;

    /// Shorthand: build the expected UTC instant from components.
    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    #[test]
    fn parses_rfc3339_zulu() {
        assert_eq!(
            parse_as_of("2026-07-01T12:00:00Z").unwrap(),
            utc(2026, 7, 1, 12, 0, 0)
        );
    }

    #[test]
    fn parses_rfc3339_offset_normalises_to_utc() {
        // 14:00 at +02:00 is 12:00 UTC.
        assert_eq!(
            parse_as_of("2026-07-01T14:00:00+02:00").unwrap(),
            utc(2026, 7, 1, 12, 0, 0)
        );
    }

    #[test]
    fn parses_rfc3339_with_fractional_seconds() {
        assert_eq!(
            parse_as_of("2026-07-01T12:00:00.500Z").unwrap(),
            utc(2026, 7, 1, 12, 0, 0) + chrono::Duration::milliseconds(500)
        );
    }

    #[test]
    fn parses_naive_datetime_as_utc() {
        assert_eq!(
            parse_as_of("2026-07-01T12:00:00").unwrap(),
            utc(2026, 7, 1, 12, 0, 0)
        );
    }

    #[test]
    fn parses_naive_datetime_without_seconds() {
        assert_eq!(
            parse_as_of("2026-06-15T09:30").unwrap(),
            utc(2026, 6, 15, 9, 30, 0)
        );
    }

    #[test]
    fn parses_space_separated_naive_datetime() {
        assert_eq!(
            parse_as_of("2026-07-01 12:00:00").unwrap(),
            utc(2026, 7, 1, 12, 0, 0)
        );
    }

    #[test]
    fn parses_date_only_as_midnight_utc() {
        assert_eq!(parse_as_of("2026-07-01").unwrap(), utc(2026, 7, 1, 0, 0, 0));
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(
            parse_as_of("  2026-07-01T12:00:00Z  ").unwrap(),
            utc(2026, 7, 1, 12, 0, 0)
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(
            parse_as_of("not-a-date"),
            Err(Error::InvalidAsOf(_))
        ));
    }

    #[test]
    fn rejects_impossible_date() {
        assert!(matches!(
            parse_as_of("2026-13-40"),
            Err(Error::InvalidAsOf(_))
        ));
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(parse_as_of(""), Err(Error::InvalidAsOf(_))));
    }

    /// `extract_column` must propagate an ancestor struct's null mask onto the
    /// extracted leaf (a null parent yields a null leaf). This path is not
    /// reachable through Lance — which drops struct-level validity at write time
    /// — so it is exercised directly on an in-memory batch.
    #[test]
    fn extract_column_propagates_parent_nulls() {
        let user_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        // Leaf arrays have NO nulls of their own.
        let user = StructArray::new(
            user_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
            // Parent `user` struct is null in the middle row.
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "user",
            DataType::Struct(user_fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(user)]).unwrap();

        let leaf = extract_column(&batch, "user.id").unwrap();
        let ids = leaf.as_primitive::<Int64Type>();
        assert_eq!(ids.len(), 3);
        assert!(!ids.is_null(0));
        assert_eq!(ids.value(0), 10);
        // Null parent row -> null leaf, even though the child array had a value.
        assert!(ids.is_null(1));
        assert!(!ids.is_null(2));
        assert_eq!(ids.value(2), 30);
    }
}
