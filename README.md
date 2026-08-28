# lance-cli

A command-line tool for inspecting [Lance](https://lance.org/) datasets: rows,
schema, statistics, versions, indices, fragments, and vector search, from one
binary with no Python runtime.

```sh
lance-cli rowcount dataset.lance
lance-cli head -n 5 dataset.lance
lance-cli stats --where "split = 'test'" dataset.lance
lance-cli search --column embedding --vector-file q.json -k 10 dataset.lance
```

## Install

> **Not released yet.** Neither package below is published — the commands are
> what installation will look like. The project is intended for donation to the
> [Lance](https://github.com/lancedb) namespace, and the first release will be
> cut from there. For now, build from a clone.

```sh
uv tool install lance-cli   # prebuilt binary, from PyPI
cargo install lance-cli     # from crates.io
```

Building from a clone and installing shell completions are covered in
[doc/install.md](https://github.com/jonasdedden/lance-cli/blob/main/doc/install.md).

## Commands

Every command takes a local path or an object-store URI (`s3://`, `gs://`,
`az://`, `file://`).

| Command | Description |
|---|---|
| `cat` | Print every row of one or more datasets. |
| `head` / `tail` | First / last `N` rows (default 10). |
| `take` | Rows at given indices, e.g. `--indices '-1,0,2:4'`. |
| `sample` | `N` random rows without replacement; `--seed` for reproducibility. |
| `rowcount` | Number of rows. |
| `schema` | Logical (Arrow) or physical (Lance-native) schema. |
| `stats` | Per-column summary statistics, the `df.describe()` of a dataset. |
| `freq` | Value counts for one column: each distinct value, its count and percent. |
| `diff` | Compare two datasets, or two versions of one dataset. |
| `blob` | Write one cell's binary payload to a file or stdout. |
| `search` | Nearest-neighbor vector search; appends a `_distance` column. |
| `completions` | Print a shell completion script. |

Metadata commands, answered from the manifest without reading row data:

| Command | Description |
|---|---|
| `stat` | One-screen dataset health summary. |
| `versions` / `branches` / `tags` | List versions, branches, and tags. |
| `indices` | Indices on the dataset: name, type, columns, version. |
| `index-stats` | Indexed vs unindexed row counts per index. |
| `fragments` | Fragments with row, deletion, file, and size information. |

Per-command options and output are documented in
[doc/commands.md](https://github.com/jonasdedden/lance-cli/blob/main/doc/commands.md).

## Global flags

| Flag | Purpose |
|---|---|
| `--format <csv\|jsonl\|table\|ipc>` | Output format. Defaults to `table` for the summary and metadata commands, `jsonl` elsewhere. `ipc` is a lossless Arrow stream, valid on `cat`/`head`/`tail`/`take`/`sample`. |
| `--columns` / `--exclude-columns` | Comma-separated include/exclude lists. Accept globs and nested paths. |
| `--where <predicate>` | SQL-style row filter, applied before row selection. |
| `--binary-format <none\|hex\|base64>` | How binary values are rendered. Default `none` prints a `BINARY_DATA` placeholder. |
| `--max-list-items <N>` | Truncate lists to the first `N` elements. |
| `--max-cell-width <N>` | Table only: cut each data cell to `N` characters. |
| `--float-precision <N>` | Render floats with exactly `N` fractional digits. |
| `--with-row-id` / `--with-row-addr` | Append the `_rowid` / `_rowaddr` pseudo-columns. |
| `--branch` / `--version` / `--tag` / `--as-of` | Which version of the dataset to read. |
| `--no-progress` | Turn off the scan progress indicator on stderr. |

`--max-list-items`, `--max-cell-width`, and `--float-precision` are lossy and
affect display only. Combining them with `--format ipc` is an error rather than
a silent no-op.

## Examples

The dataset below has 200 rows and five columns, one of them a 1536-dimensional
embedding.

```sh
$ lance-cli rowcount dataset.lance
200

$ lance-cli head -n 3 --columns id,label,score dataset.lance
{"id":1,"label":"ham","score":0.568}
{"id":2,"label":"spam","score":0.225}
{"id":3,"label":"ham","score":0.413}

$ lance-cli tail -n 3 --format csv --exclude-columns emb dataset.lance
id,label,score,split
198,spam,0.93,test
199,ham,0.009,test
200,,0.532,test

$ lance-cli head -n 2 --where "split = 'test' AND score > 0.9" --columns id,score dataset.lance
{"id":165,"score":0.943}
{"id":176,"score":0.983}
```

Trim a wide embedding column instead of flooding the terminal:

```sh
$ lance-cli head -n 1 --columns id,emb --max-list-items 4 --float-precision 2 dataset.lance
{"id":1,"emb":[-0.78,-0.76,0.17,0.74,"… (1532 more)"]}
```

Check the class balance of a label:

```sh
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

Summarize a column:

```sh
$ lance-cli stats --columns score --float-precision 3 dataset.lance
+--------+---------+-------+-------+-------+-------+-------+--------+----------+
| column | type    | count | nulls | min   | max   | mean  | stddev | distinct |
+==============================================================================+
| score  | Float64 | 200   | 0     | 0.002 | 0.987 | 0.502 | 0.288  | 180      |
+--------+---------+-------+-------+-------+-------+-------+--------+----------+
```

Read an earlier version of the same dataset, here before the test split was
appended:

```sh
$ lance-cli rowcount --version 1 dataset.lance
160
```

Pipe a lossless Arrow stream into another tool:

```sh
$ lance-cli cat --where "score > 0.9" dataset.lance --format ipc \
    | duckdb -c "SELECT count(*) FROM read_arrow('/dev/stdin')"
```

## Documentation

- [Install and shell completions](https://github.com/jonasdedden/lance-cli/blob/main/doc/install.md)
- [Command reference](https://github.com/jonasdedden/lance-cli/blob/main/doc/commands.md)
- [Columns and filters](https://github.com/jonasdedden/lance-cli/blob/main/doc/columns-and-filters.md)
- [Output formats](https://github.com/jonasdedden/lance-cli/blob/main/doc/output.md)
- [Versions, branches, tags, and row identity](https://github.com/jonasdedden/lance-cli/blob/main/doc/versions.md)
- [Diffing datasets and versions](https://github.com/jonasdedden/lance-cli/blob/main/doc/diff.md)
- [Remote object stores](https://github.com/jonasdedden/lance-cli/blob/main/doc/remote-storage.md)
- [Changelog](https://github.com/jonasdedden/lance-cli/blob/main/CHANGELOG.md)

## License

MIT.
