# Columns and filters

## `--columns` / `--exclude-columns`

Both flags take a comma-separated list of tokens, resolved against the dataset
schema. `--exclude-columns` takes precedence over `--columns`.

```sh
# Exact top-level names; your order is preserved.
lance-cli cat --columns id,score dataset.lance

# Glob patterns.
lance-cli head --columns 'emb_*' dataset.lance
lance-cli cat  --exclude-columns 'raw_*,debug_*' dataset.lance

# Nested struct field paths.
lance-cli head --columns meta.user.id,id dataset.lance
```

- Globs use `*` (any run of characters) and `?` (exactly one) and match
  top-level names only. Matches expand in schema order at the position the
  pattern occupies. A pattern that matches nothing is an error, as an unknown
  exact name is.
- Nested paths are validated against the Arrow schema, so `meta.nope` and
  `score.x` (traversal into a non-struct) fail with a message rather than a
  panic deep inside Lance.
- A token that exactly matches a top-level column always selects that literal
  column, which is how you pick a column named `a*b` or `meta.user`. This wins
  over glob and path interpretation.
- A column matched by both a glob and an explicit name appears once, at its
  first position.

### Nested output shape

A projected nested leaf becomes one flat column named by its full dotted path,
which is what Lance's scanner returns. `--columns meta.user.id,id` yields:

```json
{"meta.user.id": 10, "id": 1}
```

`head`, `cat`, `take`, `sample`, `tail`, and `schema --type arrow` all agree on
this shape. Projecting a whole struct keeps it nested:

```json
{"meta": {"user": {"id": 10, "name": "alice"}, "source": "web"}}
```

Naming both a struct and a leaf inside it (`--columns meta,meta.user.id`) emits
the whole struct and a duplicate flat `meta.user.id` column.
`schema --type physical` shows the Lance-native pruned field tree rather than
the flat view.

Excluding a nested path prunes that leaf or subtree and emits the struct's
surviving leaves as flat dotted columns: `--exclude-columns meta.user.id`
yields `meta.user.name` and `meta.source`. Untouched struct columns stay whole.

## `--where`

`--where` takes a SQL-style predicate, parsed by Lance (DataFusion SQL
for Lance), and keeps the rows that match. It is available on `cat`, `head`,
`tail`, `sample`, `rowcount`, `stats`, and `freq`.

The filter runs before row selection, so `head`, `tail`, and `sample` operate
on the matching rows rather than filtering a positional slice. It also runs
before projection, so you can filter on a column you project away:
`--columns id --where 'score > 1.5'`.

```sh
lance-cli head -n 20 --where "score > 0.5 AND name LIKE 'a%'" dataset.lance
lance-cli rowcount --where "label = 'spam'" dataset.lance
lance-cli cat --where "created_at >= TIMESTAMP '2026-01-01'" --columns id,score dataset.lance
lance-cli sample -n 100 --where "split = 'test'" dataset.lance
```

`take --indices` addresses rows positionally, so combining it with `--where` is
rejected. Invalid predicates surface Lance's parse error as
`invalid --where predicate: …`.
