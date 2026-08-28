//! Schema-delta machinery shared by the two `diff` commands.
//!
//! Both the Lance version diff (`commands::lance::diff`, comparing two versions
//! of one dataset) and the generic dataset-vs-dataset diff (`commands::diff`,
//! comparing two different datasets) compute the same
//! field-by-field schema delta and render it the same way. That logic lives
//! here so the two commands stay byte-for-byte consistent; each command layers
//! its own row/fragment/metadata deltas and endpoint labelling on top.

use std::collections::BTreeMap;
use std::io::Write;

use arrow_schema::{Field, SchemaRef};
use serde_json::{Value, json};

use crate::Result;

/// Field-by-field difference between two Arrow schemas.
///
/// Columns are matched by name; nested types are compared structurally via
/// their `DataType`, and nullability is folded into the type label so a pure
/// nullability change registers as a retype.
pub(crate) struct SchemaDelta {
    /// `(name, type_label)` for columns present only in the right/"to" schema.
    pub(crate) added: Vec<(String, String)>,
    /// `(name, type_label)` for columns present only in the left/"from" schema.
    pub(crate) removed: Vec<(String, String)>,
    /// `(name, from_label, to_label)` for columns whose type/nullability changed.
    pub(crate) retyped: Vec<(String, String, String)>,
}

/// A compact type label that also encodes nullability (`Int32` vs `Int32?`), so
/// a pure nullability change registers as a retype. Nested types render through
/// `DataType`'s `Display`, which recurses structurally.
fn type_label(field: &Field) -> String {
    format!(
        "{}{}",
        field.data_type(),
        if field.is_nullable() { "?" } else { "" }
    )
}

/// Compute the schema delta between a "from" (left) and "to" (right) schema.
pub(crate) fn build_schema_delta(from: &SchemaRef, to: &SchemaRef) -> SchemaDelta {
    let from_fields: BTreeMap<&str, &Field> = from
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let to_fields: BTreeMap<&str, &Field> = to
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let mut added = Vec::new();
    let mut retyped = Vec::new();
    for (name, tf) in &to_fields {
        match from_fields.get(name) {
            None => added.push((name.to_string(), type_label(tf))),
            Some(ff) => {
                let (fl, tl) = (type_label(ff), type_label(tf));
                if fl != tl {
                    retyped.push((name.to_string(), fl, tl));
                }
            }
        }
    }
    let removed: Vec<(String, String)> = from_fields
        .iter()
        .filter(|(name, _)| !to_fields.contains_key(*name))
        .map(|(name, ff)| (name.to_string(), type_label(ff)))
        .collect();

    SchemaDelta {
        added,
        removed,
        retyped,
    }
}

impl SchemaDelta {
    /// True when the two schemas are field-for-field identical.
    pub(crate) fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.retyped.is_empty()
    }

    /// Write the `+`/`-`/`~` change lines (no header). Both diff commands print
    /// their own header before calling this, so the body stays identical.
    pub(crate) fn write_rows<W: Write>(&self, out: &mut W) -> Result<()> {
        for (name, ty) in &self.added {
            writeln!(out, "  + {name}: {ty}")?;
        }
        for (name, ty) in &self.removed {
            writeln!(out, "  - {name}: {ty}")?;
        }
        for (name, from_ty, to_ty) in &self.retyped {
            writeln!(out, "  ~ {name}: {from_ty} -> {to_ty}")?;
        }
        Ok(())
    }

    /// The `{added, removed, retyped}` object embedded in each command's JSON
    /// record. Field names are the documented, stable schema-diff contract.
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "added": self
                .added
                .iter()
                .map(|(name, ty)| json!({ "name": name, "type": ty }))
                .collect::<Vec<_>>(),
            "removed": self
                .removed
                .iter()
                .map(|(name, ty)| json!({ "name": name, "type": ty }))
                .collect::<Vec<_>>(),
            "retyped": self
                .retyped
                .iter()
                .map(|(name, f, t)| json!({ "name": name, "from": f, "to": t }))
                .collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn schema_delta_detects_add_remove_retype() {
        let from = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("gone", DataType::Utf8, true),
            Field::new("score", DataType::Int32, true),
        ]));
        let to = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Int64, true), // retyped
            Field::new("added", DataType::Float64, true), // added
        ]));
        let d = build_schema_delta(&from, &to);
        assert_eq!(d.added, vec![("added".to_string(), "Float64?".to_string())]);
        assert_eq!(d.removed, vec![("gone".to_string(), "Utf8?".to_string())]);
        assert_eq!(
            d.retyped,
            vec![(
                "score".to_string(),
                "Int32?".to_string(),
                "Int64?".to_string()
            )]
        );
    }

    #[test]
    fn schema_delta_flags_nullability_change() {
        let from = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let to = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let d = build_schema_delta(&from, &to);
        assert_eq!(
            d.retyped,
            vec![("id".to_string(), "Int32".to_string(), "Int32?".to_string())]
        );
    }

    #[test]
    fn schema_delta_compares_nested_structs_structurally() {
        // Two structs differing in an inner field's type register as a retype;
        // the label recurses through the nested `DataType`.
        let from = Arc::new(Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(
                vec![Field::new("id", DataType::Int32, true)]
                    .into_iter()
                    .collect(),
            ),
            true,
        )]));
        let to = Arc::new(Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(
                vec![Field::new("id", DataType::Int64, true)]
                    .into_iter()
                    .collect(),
            ),
            true,
        )]));
        let d = build_schema_delta(&from, &to);
        assert_eq!(d.retyped.len(), 1);
        let (name, from_ty, to_ty) = &d.retyped[0];
        assert_eq!(name, "meta");
        assert!(from_ty.contains("Int32"), "got {from_ty}");
        assert!(to_ty.contains("Int64"), "got {to_ty}");
    }

    #[test]
    fn identical_schemas_are_empty() {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert!(build_schema_delta(&s, &s).is_empty());
    }
}
