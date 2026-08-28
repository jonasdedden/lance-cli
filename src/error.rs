use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("no inputs provided")]
    EmptyInputs,

    #[error("unknown column '{name}' (available: {available})")]
    UnknownColumn { name: String, available: String },

    #[error("column '{0}' appears more than once in --columns/--exclude-columns")]
    DuplicateColumn(String),

    #[error("pattern '{pattern}' matches no columns (available: {available})")]
    NoGlobMatch { pattern: String, available: String },

    #[error("input pattern '{pattern}' matched no files or datasets")]
    InputGlobNoMatch { pattern: String },

    #[error("invalid input glob pattern '{pattern}': {message}")]
    InvalidGlobPattern { pattern: String, message: String },

    #[error("unknown field '{field}' in '{parent}' for path '{path}' (available: {available})")]
    UnknownNestedField {
        path: String,
        parent: String,
        field: String,
        available: String,
    },

    #[error(
        "cannot descend into '{segment}' in path '{path}': '{segment}' is not a struct (found {data_type})"
    )]
    NonStructField {
        path: String,
        segment: String,
        data_type: String,
    },

    #[error(
        "input schemas differ: '{left}' and '{right}' do not match on field '{field}'; cat requires strictly matching schemas"
    )]
    SchemaMismatch {
        left: String,
        right: String,
        field: String,
    },

    #[error(
        "arrow type {data_type} in column '{column}' cannot be represented in CSV; use --format jsonl"
    )]
    UnsupportedCsvType { column: String, data_type: String },

    #[error(
        "column '{column}' has type {data_type}, which freq cannot count; supported columns are scalar primitives (strings, numbers, booleans, dates, times, timestamps, decimals)"
    )]
    UnsupportedFreqType { column: String, data_type: String },

    #[error(
        "column '{column}' has more than {limit} distinct values; freq refuses to accumulate this many (is this a high-cardinality column such as an id or UUID?). Narrow the input with --where or choose another column"
    )]
    CardinalityExceeded { column: String, limit: usize },

    #[error("--indices parse error: {0}")]
    IndexParse(String),

    #[error("index {index} is out of range for dataset with {rowcount} rows")]
    IndexOutOfRange { index: i64, rowcount: u64 },

    #[error("range {start}:{end} is empty (start > end)")]
    EmptyRange { start: i64, end: i64 },

    #[error("sample size {requested} is larger than the available row count {rowcount}")]
    SampleTooLarge { requested: u64, rowcount: u64 },

    #[error("invalid --where predicate: {0}")]
    InvalidPredicate(String),

    #[error(
        "--where cannot be combined with 'take --indices'; indices are positional. Filter rows with another command (e.g. 'head --where ...') instead"
    )]
    TakeWhereConflict,

    #[error(
        "column '{column}' is not a vector column (found {data_type}, expected a fixed-size list of f16/f32/f64)"
    )]
    NotVectorColumn { column: String, data_type: String },

    #[error("query has {query} dims, column {column} has {column_dims}")]
    VectorDimMismatch {
        query: usize,
        column: String,
        column_dims: usize,
    },

    #[error("could not parse query vector as a JSON array of numbers: {0}")]
    VectorParse(String),

    #[error("failed to read query vector file {path}")]
    VectorFileRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to open lance dataset at {path}")]
    LanceOpen {
        path: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("lance operation failed")]
    Lance(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("not a Lance dataset: {path} (expected a directory containing `_versions/`)")]
    NotLanceDataset { path: String },

    #[error(
        "cannot exclude the system column '{column}' while {flag} is set; drop {flag} instead (the pseudo-column is only ever emitted because the flag requests it)"
    )]
    RowIdExcluded { column: String, flag: &'static str },

    #[error(
        "tag '{tag}' is on branch '{tag_branch}', not '{requested_branch}'; remove --branch or pass --branch {tag_branch}"
    )]
    TagBranchMismatch {
        tag: String,
        tag_branch: String,
        requested_branch: String,
    },

    #[error(
        "could not parse --as-of value '{0}': expected RFC 3339 with offset (e.g. 2026-07-01T12:00:00Z), a naive datetime interpreted as UTC (e.g. 2026-07-01T12:00:00), or a date interpreted as midnight UTC (e.g. 2026-07-01)"
    )]
    InvalidAsOf(String),

    #[error(
        "--as-of {requested} predates the earliest version on this branch (earliest: {earliest}); pass a timestamp at or after it"
    )]
    AsOfBeforeFirstVersion { requested: String, earliest: String },

    #[error("--format is not applicable to '{command}' (it does not emit row-shaped output)")]
    FormatNotApplicable { command: &'static str },

    #[error(
        "--format ipc is only supported on the row-producing commands (cat/head/tail/take/sample), not '{command}'"
    )]
    IpcNotApplicable { command: &'static str },

    #[error(
        "{flag} is a value-rendering flag and does not apply to --format ipc, which is lossless; drop {flag} or choose a text format"
    )]
    IpcRenderingFlag { flag: &'static str },

    #[error(
        "refusing to write binary Arrow IPC to a terminal; redirect it to a file (lance-cli ... --format ipc > out.arrows) or pipe it to another program"
    )]
    IpcToTerminal,

    #[error(
        "diff compares two versions of one branch, but the endpoints resolved to different branches ('{from_branch}' and '{to_branch}'); scope both with --branch or use tags on the same branch"
    )]
    DiffCrossBranch {
        from_branch: String,
        to_branch: String,
    },

    #[error(
        "diff supports only the default human-readable summary or --format jsonl (got {format})"
    )]
    DiffFormatUnsupported { format: &'static str },

    #[error(
        "diff cannot combine a second dataset with Lance version selectors (--from/--to/--from-tag/--to-tag/--branch): those select two versions of ONE dataset. Pass either two datasets, or one dataset with version selectors"
    )]
    DiffSelectorsInTwoDatasetMode,

    #[error(
        "diff needs either a second dataset (to compare two datasets) or --from/--from-tag (to compare two versions of one Lance dataset)"
    )]
    DiffMissingFromRef,

    // Not a `#[source]`: the wrapped error is rendered inline so the single-line
    // message names which of the two diff inputs rejected the projection (the
    // field is deliberately named `error`, not `source`, to keep it out of the
    // error chain and avoid a duplicate `caused by:` line).
    #[error("in {path}: {error}")]
    DiffColumn { path: String, error: Box<Error> },

    #[error(
        "column '{column}' has type {data_type}, which blob cannot extract; supported columns are Binary/LargeBinary/FixedSizeBinary/BinaryView or a Lance blob-encoded column"
    )]
    NotBinaryColumn { column: String, data_type: String },

    #[error(
        "cell at index {index} in column '{column}' is null (or empty — blob encoding cannot distinguish them); nothing to extract"
    )]
    NullBlobCell { column: String, index: u64 },

    #[error(
        "refusing to write raw binary to a terminal; pass -o <file> or redirect stdout (e.g. `> out.bin`)"
    )]
    BlobToTty,

    #[error("failed to write blob output to {path}")]
    BlobOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("arrow error")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("csv writer error")]
    Csv(#[from] csv::Error),

    #[error("json serialization error")]
    Json(#[from] serde_json::Error),
}
