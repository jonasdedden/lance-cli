//! Resolve `--columns` / `--exclude-columns` against an arrow schema.
//!
//! Beyond plain top-level column names, both flags accept:
//!
//! * **Glob patterns** (`*`, `?`) matched against top-level column names. A
//!   pattern matching nothing is an error, mirroring how an unknown exact name
//!   behaves. Matched columns are emitted in schema order at the position the
//!   pattern occupies in the user's list.
//! * **Nested field paths** (`meta.user.id`) that walk into struct columns.
//!   Paths are validated against the Arrow schema so a bad path yields a clear
//!   error instead of a panic deep inside Lance.
//!
//! **Ambiguity / escaping rule:** a token that *exactly* matches a real
//! top-level column name is always treated as that literal column — this is how
//! a column literally named `a*b` or `meta.user` is selected, and it wins over
//! both glob and path interpretation. Only when there is no exact top-level
//! match is a token containing `*`/`?` treated as a glob, or a token containing
//! `.` treated as a nested path.
//!
//! Nested projection is surfaced as **flat, dotted-named columns**: projecting
//! `meta.user.id` yields a single leaf column named `meta.user.id`. This
//! matches what Lance's scanner returns natively and is documented in the
//! README.

use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::Result;
use crate::error::Error;

/// Resolve the requested projection against `schema`. Returns `None` when neither
/// flag was provided (callers interpret as "all columns, no filtering").
///
/// `exclude` takes precedence over `include`: if both are set, the result is
/// `<all columns> \ exclude` (the include list is ignored, matching the spec).
///
/// The returned entries are either top-level column names or dotted nested
/// paths; both are understood directly by Lance's scanner/projection and by
/// [`crate::commands::common::project_arrow_schema`].
pub fn resolve(
    schema: &SchemaRef,
    include: Option<&[String]>,
    exclude: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    if include.is_none() && exclude.is_none() {
        return Ok(None);
    }

    if let Some(excl) = exclude {
        return Ok(Some(resolve_exclude(schema, excl)?));
    }

    let incl = include.expect("checked above");
    Ok(Some(resolve_include(schema, incl)?))
}

/// How a single user token is interpreted, after applying the exact-match-first
/// rule.
enum Token<'a> {
    /// Exact top-level column name (possibly containing `.` or `*`).
    TopLevel(&'a str),
    /// Glob pattern to expand against top-level names.
    Glob(&'a str),
    /// Dotted nested path (`meta.user.id`).
    Path(&'a str),
    /// A plain name that matches nothing — reported as an unknown column.
    Unknown(&'a str),
}

fn classify<'a>(token: &'a str, top_level: &HashSet<&str>) -> Token<'a> {
    if top_level.contains(token) {
        Token::TopLevel(token)
    } else if token.contains('*') || token.contains('?') {
        Token::Glob(token)
    } else if token.contains('.') {
        Token::Path(token)
    } else {
        Token::Unknown(token)
    }
}

/// Where an already-seen entry first came from. Only a repeated *explicit*
/// entry is a `DuplicateColumn` error; overlaps involving a glob-introduced
/// entry dedupe silently, so `emb_1,emb_*` and `emb_*,emb_1` behave identically
/// ("appears once at its first position").
#[derive(Clone, Copy, PartialEq)]
enum Origin {
    Explicit,
    Glob,
}

fn resolve_include(schema: &SchemaRef, incl: &[String]) -> Result<Vec<String>> {
    let all: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let top_level: HashSet<&str> = all.iter().copied().collect();

    let mut out: Vec<String> = Vec::new();
    let mut seen: HashMap<String, Origin> = HashMap::new();

    for token in incl {
        match classify(token, &top_level) {
            Token::TopLevel(name) => push_explicit(name, &mut out, &mut seen)?,
            Token::Path(path) => {
                validate_path(schema, path)?;
                push_explicit(path, &mut out, &mut seen)?;
            }
            Token::Glob(pattern) => {
                let mut matched = false;
                for col in &all {
                    if wildcard_match(pattern, col) {
                        matched = true;
                        // Glob overlaps (with an earlier glob or explicit name)
                        // dedupe silently, keeping the first position.
                        if !seen.contains_key(*col) {
                            seen.insert((*col).to_string(), Origin::Glob);
                            out.push((*col).to_string());
                        }
                    }
                }
                if !matched {
                    return Err(Error::NoGlobMatch {
                        pattern: pattern.to_string(),
                        available: all.join(", "),
                    });
                }
            }
            Token::Unknown(name) => {
                return Err(Error::UnknownColumn {
                    name: name.to_string(),
                    available: all.join(", "),
                });
            }
        }
    }

    Ok(out)
}

/// Add an explicit (exact-name or path) entry. Naming the same entry explicitly
/// twice is a `DuplicateColumn` error; but if it was already introduced by a
/// glob it is silently skipped (it is already present at the glob's position),
/// so overlap is order-insensitive.
fn push_explicit(
    entry: &str,
    out: &mut Vec<String>,
    seen: &mut HashMap<String, Origin>,
) -> Result<()> {
    match seen.get(entry) {
        Some(Origin::Explicit) => Err(Error::DuplicateColumn(entry.to_string())),
        Some(Origin::Glob) => Ok(()),
        None => {
            seen.insert(entry.to_string(), Origin::Explicit);
            out.push(entry.to_string());
            Ok(())
        }
    }
}

fn resolve_exclude(schema: &SchemaRef, excl: &[String]) -> Result<Vec<String>> {
    let all: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let top_level: HashSet<&str> = all.iter().copied().collect();

    // Whole top-level columns to drop (by name or glob), and nested subtrees to
    // prune (dotted paths). Validate everything up front.
    let mut drop_columns: HashSet<String> = HashSet::new();
    let mut prune_paths: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for token in excl {
        if !seen.insert(token.clone()) {
            return Err(Error::DuplicateColumn(token.clone()));
        }
        match classify(token, &top_level) {
            Token::TopLevel(name) => {
                drop_columns.insert(name.to_string());
            }
            Token::Path(path) => {
                validate_path(schema, path)?;
                prune_paths.push(path.to_string());
            }
            Token::Glob(pattern) => {
                let mut matched = false;
                for col in &all {
                    if wildcard_match(pattern, col) {
                        matched = true;
                        drop_columns.insert((*col).to_string());
                    }
                }
                if !matched {
                    return Err(Error::NoGlobMatch {
                        pattern: pattern.to_string(),
                        available: all.join(", "),
                    });
                }
            }
            Token::Unknown(name) => {
                return Err(Error::UnknownColumn {
                    name: name.to_string(),
                    available: all.join(", "),
                });
            }
        }
    }

    // Build the surviving projection in schema order.
    let mut out: Vec<String> = Vec::new();
    for field in schema.fields() {
        let name = field.name();
        if drop_columns.contains(name) {
            continue;
        }
        // Paths pruning inside this column, relative to it.
        let touching: Vec<&str> = prune_paths
            .iter()
            .filter(|p| {
                p.split_once('.')
                    .map(|(head, _)| head == name.as_str())
                    .unwrap_or(false)
            })
            .map(String::as_str)
            .collect();
        if touching.is_empty() {
            // Untouched column: keep whole.
            out.push(name.clone());
            continue;
        }
        // Struct column with some leaves pruned: emit the surviving leaves as
        // flat dotted paths (schema order). Validation already guaranteed the
        // paths descend into this struct, so it must be a struct.
        let leaves = leaf_paths(name, field);
        for leaf in leaves {
            let excluded = touching
                .iter()
                .any(|p| leaf == *p || is_prefix_path(p, &leaf));
            if !excluded {
                out.push(leaf);
            }
        }
    }

    Ok(out)
}

/// True when `prefix` is a strict path-prefix of `path` (i.e. `path` is
/// `prefix` followed by `.` and more segments). Used so excluding `meta.user`
/// removes `meta.user.id` and `meta.user.name`.
fn is_prefix_path(prefix: &str, path: &str) -> bool {
    path.len() > prefix.len() && path.as_bytes()[prefix.len()] == b'.' && path.starts_with(prefix)
}

/// Enumerate the dotted leaf paths of `field` under `prefix`. A non-struct
/// field is its own single leaf; a struct expands into its children.
fn leaf_paths(prefix: &str, field: &Field) -> Vec<String> {
    match field.data_type() {
        DataType::Struct(children) => {
            let mut out = Vec::new();
            for child in children {
                let child_prefix = format!("{prefix}.{}", child.name());
                out.extend(leaf_paths(&child_prefix, child));
            }
            out
        }
        _ => vec![prefix.to_string()],
    }
}

/// Validate a dotted `path` against `schema`, walking `Struct` fields.
///
/// Errors:
/// * [`Error::UnknownNestedField`] — a segment names no field in its parent.
/// * [`Error::NonStructField`] — an intermediate segment is not a struct.
pub fn validate_path(schema: &Schema, path: &str) -> Result<()> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut fields: &Fields = schema.fields();
    let mut parent = String::new();

    for (i, seg) in segments.iter().enumerate() {
        let Some(field) = fields.iter().find(|f| f.name() == seg) else {
            let available = fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let parent = if parent.is_empty() {
                "<schema>".to_string()
            } else {
                parent
            };
            return Err(Error::UnknownNestedField {
                path: path.to_string(),
                parent,
                field: (*seg).to_string(),
                available,
            });
        };
        let is_last = i == segments.len() - 1;
        if is_last {
            return Ok(());
        }
        match field.data_type() {
            DataType::Struct(children) => {
                fields = children;
                parent = if parent.is_empty() {
                    (*seg).to_string()
                } else {
                    format!("{parent}.{seg}")
                };
            }
            other => {
                return Err(Error::NonStructField {
                    path: path.to_string(),
                    segment: (*seg).to_string(),
                    data_type: format!("{other:?}"),
                });
            }
        }
    }
    // Unreachable: a non-empty split always returns from the last iteration.
    Ok(())
}

/// Build the flat output [`Field`] for a single resolved projection entry.
///
/// `entry` is either an exact top-level column name (returned as-is, even if it
/// contains `.`) or a validated dotted path, in which case the leaf field's
/// type is used with the full dotted name. A nested leaf is nullable when the
/// leaf or any ancestor struct is nullable (a null parent yields a null leaf).
///
/// Panics if `entry` was not validated against `schema` by [`resolve`].
pub fn projected_field(schema: &Schema, entry: &str) -> Field {
    if let Ok(field) = schema.field_with_name(entry) {
        return field.clone();
    }
    let mut fields: &Fields = schema.fields();
    let segments: Vec<&str> = entry.split('.').collect();
    let mut nullable = false;
    for (i, seg) in segments.iter().enumerate() {
        let field = fields
            .iter()
            .find(|f| f.name() == seg)
            .expect("projection validated against schema");
        nullable |= field.is_nullable();
        if i == segments.len() - 1 {
            return Field::new(entry, field.data_type().clone(), nullable);
        }
        match field.data_type() {
            DataType::Struct(children) => fields = children,
            _ => unreachable!("projection validated against schema"),
        }
    }
    unreachable!("non-empty path always returns at the leaf")
}

/// Match `text` against a shell-style glob `pattern` supporting `*` (any run of
/// characters, including none) and `?` (exactly one character). Hand-rolled to
/// avoid a dependency: patterns are only ever matched against flat column-name
/// strings, so globset's richer path semantics buy nothing here.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    // Classic two-pointer glob matcher with backtracking on the last `*`.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]))
    }

    fn nested_schema() -> SchemaRef {
        let user = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let meta = Fields::from(vec![
            Field::new("user", DataType::Struct(user), true),
            Field::new("source", DataType::Utf8, true),
        ]);
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Float64, true),
            Field::new("emb_0", DataType::Float64, true),
            Field::new("emb_1", DataType::Float64, true),
            Field::new("emb_2", DataType::Float64, true),
            Field::new("meta", DataType::Struct(meta), true),
        ]))
    }

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn none_means_all() {
        let s = schema();
        assert!(resolve(&s, None, None).unwrap().is_none());
    }

    #[test]
    fn include_preserves_user_order() {
        let s = schema();
        let got = resolve(&s, Some(&v(&["c", "a"])), None).unwrap().unwrap();
        assert_eq!(got, v(&["c", "a"]));
    }

    #[test]
    fn exclude_keeps_schema_order() {
        let s = schema();
        let got = resolve(&s, None, Some(&v(&["b"]))).unwrap().unwrap();
        assert_eq!(got, v(&["a", "c"]));
    }

    #[test]
    fn exclude_takes_precedence() {
        let s = schema();
        let got = resolve(&s, Some(&v(&["a", "b"])), Some(&v(&["b"])))
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["a", "c"]));
    }

    #[test]
    fn unknown_column_errors() {
        let s = schema();
        assert!(matches!(
            resolve(&s, Some(&v(&["zzz"])), None),
            Err(Error::UnknownColumn { .. })
        ));
    }

    #[test]
    fn duplicate_errors() {
        let s = schema();
        assert!(matches!(
            resolve(&s, Some(&v(&["a", "a"])), None),
            Err(Error::DuplicateColumn(_))
        ));
    }

    // ---------------- globs ----------------

    #[test]
    fn glob_include_expands_in_schema_order() {
        let s = nested_schema();
        let got = resolve(&s, Some(&v(&["emb_*"])), None).unwrap().unwrap();
        assert_eq!(got, v(&["emb_0", "emb_1", "emb_2"]));
    }

    #[test]
    fn glob_and_explicit_keep_relative_positions() {
        let s = nested_schema();
        // Glob expands in place; explicit `id` keeps its user position.
        let got = resolve(&s, Some(&v(&["id", "emb_*"])), None)
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["id", "emb_0", "emb_1", "emb_2"]));
    }

    #[test]
    fn glob_question_matches_single_char() {
        let s = nested_schema();
        let got = resolve(&s, Some(&v(&["emb_?"])), None).unwrap().unwrap();
        assert_eq!(got, v(&["emb_0", "emb_1", "emb_2"]));
    }

    #[test]
    fn glob_no_match_errors() {
        let s = nested_schema();
        assert!(matches!(
            resolve(&s, Some(&v(&["nope_*"])), None),
            Err(Error::NoGlobMatch { .. })
        ));
    }

    #[test]
    fn glob_exclude_removes_matches() {
        let s = nested_schema();
        let got = resolve(&s, None, Some(&v(&["emb_*"]))).unwrap().unwrap();
        // `meta` is untouched by the glob, so it stays a whole struct column.
        assert_eq!(got, v(&["id", "score", "meta"]));
    }

    #[test]
    fn glob_overlap_with_explicit_dedupes_explicit_first() {
        let s = nested_schema();
        // `emb_1` matched by both the explicit name and the glob: appears once,
        // at its first position (the explicit one, index 0).
        let got = resolve(&s, Some(&v(&["emb_1", "emb_*"])), None)
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["emb_1", "emb_0", "emb_2"]));
    }

    #[test]
    fn glob_overlap_with_explicit_dedupes_glob_first() {
        let s = nested_schema();
        // Reverse order must not error: the glob introduces `emb_1`, and the
        // later explicit `emb_1` dedupes silently at its glob position.
        let got = resolve(&s, Some(&v(&["emb_*", "emb_1"])), None)
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["emb_0", "emb_1", "emb_2"]));
    }

    #[test]
    fn explicit_named_twice_still_errors() {
        let s = nested_schema();
        // Two *explicit* mentions remain a duplicate error.
        assert!(matches!(
            resolve(&s, Some(&v(&["emb_1", "emb_1"])), None),
            Err(Error::DuplicateColumn(_))
        ));
    }

    #[test]
    fn literal_star_name_wins_over_glob() {
        let s = Arc::new(Schema::new(vec![
            Field::new("a*b", DataType::Int32, true),
            Field::new("axb", DataType::Int32, true),
        ]));
        // Exact match on the literal `a*b` column wins; `axb` is not pulled in.
        let got = resolve(&s, Some(&v(&["a*b"])), None).unwrap().unwrap();
        assert_eq!(got, v(&["a*b"]));
    }

    // ---------------- nested paths ----------------

    #[test]
    fn nested_path_include() {
        let s = nested_schema();
        let got = resolve(&s, Some(&v(&["meta.user.id", "id"])), None)
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["meta.user.id", "id"]));
    }

    #[test]
    fn nested_unknown_field_errors() {
        let s = nested_schema();
        assert!(matches!(
            resolve(&s, Some(&v(&["meta.nope"])), None),
            Err(Error::UnknownNestedField { .. })
        ));
    }

    #[test]
    fn nested_non_struct_traversal_errors() {
        let s = nested_schema();
        assert!(matches!(
            resolve(&s, Some(&v(&["score.x"])), None),
            Err(Error::NonStructField { .. })
        ));
    }

    #[test]
    fn literal_dotted_name_wins_over_path() {
        let s = Arc::new(Schema::new(vec![
            Field::new("meta.user", DataType::Int32, true),
            Field::new("other", DataType::Int32, true),
        ]));
        // Exact top-level match on `meta.user` wins over path interpretation.
        let got = resolve(&s, Some(&v(&["meta.user"])), None)
            .unwrap()
            .unwrap();
        assert_eq!(got, v(&["meta.user"]));
    }

    #[test]
    fn exclude_nested_leaf_flattens_siblings() {
        let s = nested_schema();
        let got = resolve(&s, None, Some(&v(&["meta.user.id"])))
            .unwrap()
            .unwrap();
        // `meta` becomes its surviving leaves as flat dotted columns.
        assert_eq!(
            got,
            v(&[
                "id",
                "score",
                "emb_0",
                "emb_1",
                "emb_2",
                "meta.user.name",
                "meta.source"
            ])
        );
    }

    #[test]
    fn exclude_nested_subtree_prunes_all_children() {
        let s = nested_schema();
        let got = resolve(&s, None, Some(&v(&["meta.user"])))
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            v(&["id", "score", "emb_0", "emb_1", "emb_2", "meta.source"])
        );
    }

    // ---------------- projected_field ----------------

    #[test]
    fn projected_field_top_level() {
        let s = nested_schema();
        let f = projected_field(&s, "id");
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &DataType::Int32);
        assert!(!f.is_nullable());
    }

    #[test]
    fn projected_field_nested_leaf_uses_dotted_name() {
        let s = nested_schema();
        let f = projected_field(&s, "meta.user.id");
        assert_eq!(f.name(), "meta.user.id");
        assert_eq!(f.data_type(), &DataType::Int64);
        // nullable because ancestor structs are nullable.
        assert!(f.is_nullable());
    }

    // ---------------- wildcard matcher ----------------

    #[test]
    fn wildcard_semantics() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("emb_*", "emb_12"));
        assert!(wildcard_match("emb_?", "emb_3"));
        assert!(!wildcard_match("emb_?", "emb_33"));
        assert!(wildcard_match("a*b*c", "aXXbYYc"));
        assert!(!wildcard_match("a*b*c", "aXXb"));
        assert!(wildcard_match("abc", "abc"));
        assert!(!wildcard_match("abc", "abcd"));
    }
}
