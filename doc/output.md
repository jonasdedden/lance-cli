# Output formats

`--format` selects the output format. Metadata and summary commands
(`versions`, `branches`, `tags`, `indices`, `index-stats`, `fragments`,
`stats`, `freq`, `stat`) default to `table`; everything else defaults to
`jsonl`.

## JSONL

- One JSON object per line, keys in projected column order.
- `NaN` and `±Infinity` are emitted as the strings `"NaN"`, `"Infinity"`, and
  `"-Infinity"`.
- Timestamps are ISO-8601 (`2024-01-01T00:00:00.000000`), with an offset when
  the Arrow type carries a timezone.
- Lists become JSON arrays, structs become objects, maps become objects with
  stringified keys.

## CSV

- The first line is a header row. Column names containing `,`, newlines, or
  quotes are quoted per RFC 4180.
- Nulls are empty cells. Floats use `NaN`, `inf`, `-inf`.
- Nested types (list, struct, map, duration, interval) are rejected; use JSONL
  for those. `--max-list-items` does not change this.

## Table

- Unicode borders on a TTY, an ASCII grid when piped, so `lance-cli … | grep` stays
  usable.
- Primitives render as in CSV.
- Nested cells are JSON-encoded inside the cell, for example `["id"]` for a
  single-element list, which is more permissive than CSV.
- All rows are buffered before printing, since column widths need the full
  table. Prefer `jsonl` or `csv` for large scans.

## Arrow IPC (`--format ipc`)

`ipc` writes the [Arrow IPC streaming
format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
to stdout: the schema, then each `RecordBatch` from the scan, then the
end-of-stream marker. It bypasses value rendering, so it is lossless for types,
nulls, and nested, binary, and timestamp values, and it streams at constant
memory. Pipe it into DuckDB (`read_arrow`), pyarrow
(`pyarrow.ipc.open_stream`), Polars, or an ADBC tool.

```sh
lance-cli cat --where "score > 0.9" ds.lance --format ipc \
  | duckdb -c "SELECT * FROM read_arrow('/dev/stdin')"
lance-cli sample -n 1000 ds.lance --format ipc > sample.arrows
lance-cli head -n 100 ds.lance --format ipc \
  | python -c "import pyarrow.ipc, sys; print(pyarrow.ipc.open_stream(sys.stdin.buffer).read_all())"
```

- Valid only on `cat`, `head`, `tail`, `take`, and `sample`. The metadata and
  summary commands materialize their own computed shapes and reject it; they
  may gain IPC later.
- Writing the stream to a terminal is refused. Redirect or pipe it.
- The value-rendering flags (`--binary-format`, `--max-list-items`,
  `--max-cell-width`, `--float-precision`) have nothing to act on in a lossless
  stream, so combining them with `ipc` is an error rather than a silent no-op.
- Projection and `--where` apply as usual. For `cat` with multiple inputs the
  schema comes from the first dataset; all inputs must already share a schema.
- An empty result still produces a valid stream: schema plus end-of-stream,
  zero batches.

## Binary values

Binary payloads are collapsed to a placeholder by default:

```
$ lance-cli head -n 1 dataset.lance
{"id":1,"data":"BINARY_DATA",…}

$ lance-cli head -n 1 --binary-format hex dataset.lance
{"id":1,"data":"\\x48\\x65\\x6c\\x6c\\x6f",…}

$ lance-cli head -n 1 --binary-format base64 dataset.lance
{"id":1,"data":"SGVsbG8=",…}
```

The placeholder applies recursively: binary nested inside a struct or list also
renders as `BINARY_DATA`. To get the actual bytes out, use
[`blob`](commands.md#blob).

## Rendering flags

`--max-list-items`, `--max-cell-width`, and `--float-precision` change how
values are printed, never the data. Their output is lossy and not meant for
round-tripping; in particular it is not a guarantee that the result parses as
CSV, and CSV still rejects nested columns.

- `--max-list-items N`: after `N` elements a list gets a trailing string
  element `… (K more)`, where `K` is the number dropped. It applies at every
  nesting level and to `FixedSizeList` embedding columns. The marker is a JSON
  string, so JSONL arrays stay valid JSON.
- `--max-cell-width N` (table only): each data cell is cut to at most `N`
  characters, ending in `…`. The `…` counts toward `N`, so `N = 0` collapses a
  non-empty cell to a bare `…`. Header cells are never truncated. Counting is
  character-based and never splits a multi-byte codepoint; CJK and full-width
  characters count as one each.
- `--float-precision N`: `f16`, `f32`, and `f64` render with exactly `N`
  fractional digits, using `format!`-style round-half-to-even. `NaN` and
  `Infinity` are untouched. In `stats` this applies to `mean` and `stddev`;
  `min` and `max` are pre-rendered strings and unaffected.

## Progress indicator

Long scans (`cat`, `stats`, `freq`, and filtered `head`/`tail`/`sample`) draw a
progress indicator on stderr when stderr is a TTY, so stdout and piped output
are unaffected. `--no-progress` turns it off unconditionally.

## Exit codes

`0` is success, and `2` is any command error. `diff` uses `1` for "the two
sides differ", so error exits use `2` everywhere to keep `1` unambiguous. See
[diff.md](diff.md).
