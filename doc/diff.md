# Diffing datasets and versions

`lance-cli diff` has two modes, chosen by how many datasets you name:

| Invocation | Mode | Compares |
|---|---|---|
| `lance-cli diff A B` | dataset-vs-dataset | Two different datasets. |
| `lance-cli diff DS --from <ref> …` | version | Two versions of one dataset. |

The rules keep the modes apart:

- A second dataset selects dataset-vs-dataset mode. The version selectors
  (`--from`, `--to`, `--from-tag`, `--to-tag`, `--branch`) are ambiguous across
  two datasets, so combining any of them with a second dataset is an error.
- A single dataset with at least one of `--from` or `--from-tag` selects
  version mode. A single dataset with no version selector is an error.

`diff` prints its own report shape, either a human summary or one JSON record
under `--format jsonl`. The rendering flags (`--max-list-items`,
`--max-cell-width`, `--float-precision`) and the row formats (`csv`, `table`,
`ipc`) do not apply and are rejected.

## Dataset vs dataset

Compares schema, schema metadata, and row count, which answers whether
`part_b` is schema-compatible with `part_a` or whether an export lost rows.
Neither input takes version selectors.

```sh
lance-cli diff part_a.lance part_b.lance
lance-cli diff part_a.lance part_b.lance --columns id,score
lance-cli diff part_a.lance part_b.lance --format jsonl
```

It reports:

- Row count of each dataset and the net delta.
- Schema changes: columns only in `A` (removed), only in `B` (added), and
  columns in both whose type or nullability changed (retyped). Nested types are
  compared structurally, and a nullability change shows as `Int32` → `Int32?`.
- Metadata changes: Arrow schema-level metadata keys added, removed, or
  changed.

`--columns` and `--exclude-columns` scope the comparison to the projected
columns. Row-level content is never compared; that is an explicit non-goal. The
projection is resolved against each dataset's own schema, so a scoped column
must exist on both sides. Metadata is dataset-level and always compared in
full.

The `--format jsonl` record:

```jsonc
{
  "left": "part_a.lance", "right": "part_b.lance",
  "identical": false,
  "rows":     { "left": 100, "right": 98, "net": -2 },
  "schema":   { "added":   [{ "name": "flag", "type": "Boolean?" }],
                "removed": [],
                "retyped": [{ "name": "score", "from": "Int32", "to": "Int64?" }] },
  "metadata": { "added":   [{ "key": "owner", "value": "team-b" }],
                "removed": [],
                "changed": [{ "key": "version", "from": "1", "to": "2" }] }
}
```

## Version vs version

Compares two versions of the same Lance dataset. Lance manifests carry
fragment, schema, and index metadata, so almost all of the report is derived
without scanning data.

```sh
lance-cli diff dataset.lance --from 3 --to 7
lance-cli diff dataset.lance --from-tag release-1 --to-tag release-2
lance-cli diff dataset.lance --from 3                  # --to defaults to branch latest
lance-cli diff dataset.lance --branch dev --from 2 --to 5
```

| Flag | Meaning |
|---|---|
| `--from <N>` | Left-hand version number. Required unless `--from-tag`. |
| `--from-tag <name>` | Left-hand endpoint by tag; resolves to its `(branch, version)`. |
| `--to <N>` | Right-hand version number. Defaults to the latest version of the same branch as `--from`. |
| `--to-tag <name>` | Right-hand endpoint by tag. |
| `--branch <name>` | Scope both endpoints to this branch. Default `main`. |

```
$ lance-cli diff dataset.lance --from 1
diff dataset.lance
  from  main v1
  to    main v2

Rows: 160 -> 200  (net +40; +40 added, -0 deleted)

Schema changes: none

Fragments: +1 added, -0 removed, 0 rewritten
  added:     [1]

Index changes: none

Versions in range (v1, v2]:
  v2  2026-08-27T15:55:37Z
```

It reports:

- Row delta: live row count at each version and the net change, split into rows
  added and rows deleted from fragment metadata (appended fragments and
  un-tombstoned rows against removed fragments and new tombstones). The split
  is metadata-truthful rather than a logical diff: because Lance rewrites whole
  fragments, a compaction that rewrites `N` unchanged rows counts them as `+N`
  added and `−N` deleted, and a version `restore` shows the rows it brings back
  as added.
- Schema changes: columns added, removed, or retyped. A nullability change
  counts as a retype, shown as `Int32` → `Int32?`.
- Fragment changes: fragments added, removed, or rewritten. Lance never reuses
  fragment ids, so compaction appears as removed plus added. The rewritten
  bucket is for a surviving fragment whose set of data files changed, such as a
  column added by schema evolution. Tombstone-only changes show up in the row
  delta instead.
- Index changes: indices created or dropped between the two versions.
- Version log: the versions in the `(from, to]` range with timestamps and
  commit messages.

## Exit codes

Both modes follow `diff(1)`:

| Code | Meaning |
|---|---|
| `0` | The two sides are identical. |
| `1` | The two sides differ. |
| `2` | Error: bad usage, missing dataset, cross-branch comparison, a second dataset mixed with version selectors. |

Code `2` is used for every command error across `lance-cli`, so `1` unambiguously
means "the two sides differ" and is never confused with a failure.

```sh
# Machine-readable version diff for a CI gate.
lance-cli diff dataset.lance --from 3 --to 7 --format jsonl

# Compare two tagged releases and act on the exit code.
if lance-cli diff dataset.lance --from-tag v1 --to-tag v2; then
  echo "no changes"
fi

# Contract check: does a new export match the reference schema and row count?
if ! lance-cli diff reference.lance export.lance; then
  echo "export drifted from the reference" >&2
fi
```
