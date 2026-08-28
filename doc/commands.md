# Command reference

This page covers the options and output specific to each command. Flags that
apply everywhere (`--format`, `--columns`, `--where`, …) are described in
[columns-and-filters.md](columns-and-filters.md) and [output.md](output.md).

- [Row output: `cat`, `head`, `tail`, `take`, `sample`](#row-output)
- [`rowcount`](#rowcount)
- [`schema`](#schema)
- [`stats`](#stats)
- [`freq`](#freq)
- [`stat`](#stat)
- [`fragments`](#fragments)
- [`indices` and `index-stats`](#indices-and-index-stats)
- [`search`](#search)
- [`blob`](#blob)
- [`diff`](diff.md)

## Row output

`cat`, `head`, `tail`, `take`, and `sample` all print rows and share the
projection, filter, row-identity, and format flags.

```sh
lance-cli cat part_a.lance part_b.lance      # concatenate; inputs must share a schema
lance-cli cat 'data/part_*.lance'            # glob, expanded by lance-cli in lexicographic order
lance-cli head -n 5 dataset.lance
lance-cli tail -n 3 dataset.lance
lance-cli sample -n 100 --seed 42 dataset.lance
lance-cli take --indices '-1,0,2:4' dataset.lance
```

### `--indices` grammar

`take --indices` takes a comma-separated list of expressions. Order is
preserved and duplicates are printed as often as they appear.

| Expression | Meaning |
|---|---|
| `N` | Single row; negatives count from the end. |
| `A:B` | Inclusive range. |
| `:B` | Rows 0 through B. |
| `A:` | Row A through the last row. |
| `-5:-1` | Last 5 rows. |
| `3,1,1,0:2` | `[3, 1, 1, 0, 1, 2]` |

`take` addresses rows positionally, so it rejects `--where`. Filter with
`head` or `cat` instead.

## `rowcount`

Prints the number of rows. With `--where`, it counts matching rows and pushes
the predicate into Lance scalar indices when they are available, which avoids a
full scan.

## `schema`

```sh
lance-cli schema dataset.lance                  # arrow (logical) schema
lance-cli schema --type physical dataset.lance  # Lance-native: field ids, encodings
```

The Arrow schema reflects any projection: `--columns meta.user.id` lists a
single flat `meta.user.id` field. The physical schema shows the Lance-native
pruned field tree instead.

## `stats`

Per-column summary statistics, one row per column. `stats` streams the dataset
once and uses memory independent of the row count.

| Field | Meaning |
|---|---|
| `column` | Column name. |
| `type` | Arrow data type. |
| `count` | Number of non-null values. |
| `nulls` | Number of null values; `count + nulls` is the number of rows considered. |
| `min` / `max` | Numeric, temporal, string, and boolean columns. Strings compare lexicographically. |
| `mean` / `stddev` | Numeric columns only. `stddev` is the sample standard deviation (ddof = 1). |
| `distinct` | Distinct values, exact up to 10 000, then reported as `>10000`. |

Nested, binary, decimal, and dictionary columns report `count` and `nulls`
only; they never fail. A column holding any `NaN` reports `NaN` for `mean` and
`stddev`, while `min` and `max` skip `NaN` and give the real numeric range.

```sh
lance-cli stats dataset.lance
lance-cli stats --columns score,label dataset.lance
lance-cli stats --where "split = 'test'" dataset.lance
lance-cli stats --format jsonl dataset.lance
```

## `freq`

Value counts for one column: one row per distinct value with its `count` and
`percent` of all scanned rows.

```
$ lance-cli freq --column label dataset.lance
+-------+-------+---------+
| value | count | percent |
+=========================+
| ham   | 90    | 45.0%   |
|-------+-------+---------|
| spam  | 90    | 45.0%   |
|-------+-------+---------|
| NULL  | 20    | 10.0%   |
+-------+-------+---------+
```

| Flag | Meaning |
|---|---|
| `--column <name>` | Column to count (required). |
| `-n`, `--limit <N>` | Keep the top `N` rows; the rest is folded into an `<other>` row so the percentages still add up. `N` ≥ 1. |
| `--sort <count\|value>` | `count` (default) orders by frequency and breaks ties by value; `value` orders by value. |

Ordering follows the value's real type: numbers sort numerically, temporals
chronologically, with `NULL` and `NaN` last. Values that compare equal, such as
`-0.0` and `0.0`, fall back to a string tie-break so output stays
deterministic.

Only primitive columns are supported (strings, numbers, booleans, dates,
timestamps, decimals). Nested and binary columns are rejected.

Values are keyed by their CSV rendering, the same text `cat --format csv`
prints. Two consequences follow:

- `-0.0` and `0.0` render as `-0` and `0` and are two rows, while every `NaN`
  bit pattern renders as `NaN` and collapses into one.
- A literal string `"NULL"` renders like the real null row, and `"<other>"`
  like the truncation remainder. The counts stay separate and correct, but the
  printed labels collide in every format. Filter the literal out with `--where`
  if you need to tell them apart.

Memory use is proportional to the column's cardinality. Past roughly 1M
distinct values, `freq` stops with an error instead of consuming more memory.

## `stat`

A dataset health summary read from Lance manifest metadata. It never scans data
files beyond summing their sizes, so it stays fast on large datasets, and it
runs the fragment, version, branch, tag, and index lookups concurrently.

```
$ lance-cli stat sample.lance
+--------------+---------------------------------------+
| metric       | value                                 |
+======================================================+
| path         | sample.lance                          |
| format       | lance (manifest version 4)            |
| rows         | 9                                     |
| deleted rows | 0  (0.0%)                             |
| columns      | 7                                     |
| fragments    | 3  (min 3 rows, max 3 rows, median 3) |
| data size    | 6.2 KiB                               |
| versions     | 4  (latest 2026-07-21T22:53:12Z)      |
| branches     | 2                                     |
| tags         | 2                                     |
| indices      | 1  (idx_id BTree)                     |
+--------------+---------------------------------------+
```

`stat` (singular, after `stat(1)`) summarizes the dataset; `stats` (plural)
summarizes each column.

In table mode a note is appended when the fragment spread or the deleted-row
ratio suggests it, for example `note: many small fragments; compaction would
likely help`. The thresholds are a median fragment below 100k rows with 100 or
more fragments, or a deleted-row ratio of 10% or more.

`--no-size` skips the size lookups, which is useful on remote datasets.
`--branch`, `--version`, and `--tag` apply: row, fragment, size, and index
figures follow the selected version, and the version count is scoped to the
selected branch.

`--format jsonl` emits one object with raw numbers and a stable field set:

| Field | Type | Meaning |
|---|---|---|
| `path` | string | Path or URI the dataset was opened from. |
| `format` | string | Always `"lance"`. |
| `manifest_version` | number | Checked-out manifest version. |
| `rows` | number | Live rows (`physical_rows` − `deleted_rows`). |
| `physical_rows` | number | Rows stored, ignoring deletions. |
| `deleted_rows` | number | Tombstoned rows. |
| `deleted_ratio` | number | `deleted_rows / physical_rows`, `0` when empty. |
| `columns` | number | Column count of the Arrow schema. |
| `fragments` | number | Fragment count. |
| `fragment_min_rows` | number \| null | Smallest fragment's physical rows. |
| `fragment_max_rows` | number \| null | Largest fragment's physical rows. |
| `fragment_median_rows` | number \| null | Median physical rows per fragment, rounded up on ties. |
| `data_size_bytes` | number \| null | Summed data-file bytes; `null` under `--no-size`. |
| `versions` | number | Version count on the selected branch. |
| `latest_version_timestamp` | string \| null | RFC 3339 UTC timestamp of the latest version. |
| `branches` | number | Branch count, dataset-wide. |
| `tags` | number | Tag count, dataset-wide. |
| `num_indices` | number | Index count on the checked-out version. |
| `indices` | array | `{ "name", "type" }` objects. |
| `compaction_hint` | string \| null | The advisory note, or `null`. |

`--format csv` mirrors the human-readable `metric,value` pairs. Use `jsonl` for
raw numbers.

## `fragments`

Fragments are the physical unit of a Lance dataset. `fragments` prints one row
per fragment with physical rows, tombstoned rows, data file count, and on-disk
size, plus a totals line in table mode. Row, deletion, and file information
comes from the manifest. Sizes come from the manifest where cached and from
concurrent object-store lookups otherwise.

```sh
lance-cli fragments dataset.lance
lance-cli fragments --version 3 dataset.lance
lance-cli fragments --verbose dataset.lance     # include data file paths
lance-cli fragments --no-size dataset.lance     # skip size lookups
lance-cli fragments --format jsonl dataset.lance
```

## `indices` and `index-stats`

`indices` lists every index on the active version with its type:

```
$ lance-cli indices dataset.lance
| name    | type   | uuid | columns   | dataset_version | created_at |
| idx_id  | BTree  | …    | ["id"]    | 4               | …          |
```

Lance indices go stale as rows are appended after the index was built.
`index-stats` reports how many rows are covered:

```
$ lance-cli index-stats dataset.lance
| name    | type   | indexed_rows | unindexed_rows | coverage |
| idx_id  | BTree  | 980000       | 20000          | 98.0%    |
```

`coverage` is `indexed_rows / (indexed_rows + unindexed_rows)`, or `n/a` for an
empty index. Under `--format jsonl` an extra `detail` column carries Lance's
raw statistics JSON verbatim, so type-specific internals such as IVF partition
counts or PQ sub-vectors pass through unchanged:

```
$ lance-cli --format jsonl index-stats dataset.lance
{"name":"idx_id","type":"BTree","indexed_rows":980000,"unindexed_rows":20000,"coverage":"98.0%","detail":"{\"index_type\":\"BTree\",…}"}
```

## `search`

Nearest-neighbor search against a vector column, an Arrow `FixedSizeList` of
`f16`, `f32`, or `f64`. It returns the `k` closest rows ordered by distance and
appends a `_distance` column, so every output format works unchanged. An ANN
index on the column is used automatically; without one, Lance falls back to
brute-force KNN and prints a note on stderr.

The query vector is a JSON array of numbers. Its length must match the column
width, and it is cast to the column's element type.

| Flag | Meaning |
|---|---|
| `--column <name>` | Vector column to search (required). |
| `--vector '[...]'` | Inline JSON array. |
| `--vector-file <path>` | Read the JSON array from a file, or `-` for stdin. |
| `-k <N>` | Number of neighbors (default 10). |
| `--nprobes <N>` | IVF partitions to probe; no effect without an index. |
| `--refine-factor <N>` | Re-rank the top `k * N` candidates for better recall. |

Exactly one of `--vector` and `--vector-file` is required.

```sh
lance-cli search --column embedding --vector '[0.1, 0.2, 0.3]' -k 10 ds.lance
lance-cli search --column embedding --vector-file query.json -k 10 ds.lance
cat query.json | lance-cli search --column embedding --vector-file - -k 10 ds.lance
lance-cli search --column embedding --vector-file q.json -k 10 --nprobes 32 --refine-factor 5 ds.lance
lance-cli search --column embedding --vector-file q.json -k 10 --columns id,title ds.lance
```

Full-text search against inverted indices is planned as a follow-up.

## `blob`

Writes one cell's raw bytes to a file or to stdout, which is the counterpart to
the binary rendering options that keep terminal output readable.

```sh
lance-cli blob --column image --index 42 -o out.png dataset.lance
lance-cli blob --column audio --index 7 dataset.lance > clip.wav
lance-cli blob --column image --index -1 -o last.png dataset.lance
```

`--index` takes one value and uses the same negative-index semantics as `take`.
Extraction works on `Binary`, `LargeBinary`, `FixedSizeBinary`, and
`BinaryView` columns, and on Lance blob-encoded columns
(`lance-encoding:blob`). Blob-encoded columns are streamed through Lance's blob
API in bounded chunks, so multi-GB payloads are never held in memory. lance-cli
picks the path from the column's field metadata.

- Writing raw bytes to an interactive terminal is refused. Pass `-o <file>` or
  redirect stdout.
- A null cell, an out-of-range index, or a non-binary column is a hard error.
  With `-o`, no partial or empty file is left behind: the payload goes to a
  temp file and is renamed on success. A successful `-o` overwrites an existing
  file.
- Output is raw bytes rather than rows, so `--format` is rejected.
  `--columns`, `--exclude-columns`, and `--binary-format` do not apply and are
  ignored. The Lance version selectors work as usual.

Lance's blob descriptors do not distinguish a null cell from a zero-length
payload, since a null is encoded as `size == 0`. Both are reported as a null
cell and nothing is extracted.
