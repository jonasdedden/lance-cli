//! Shared handling of the `--with-row-id` / `--with-row-addr` pseudo-columns.
//!
//! Lance tracks a stable per-row identity (`_rowid`) and a physical address
//! (`_rowaddr`). Neither is a schema column: they are *system* columns computed
//! at read time. The row-producing commands (`cat`/`head`/`tail`/`take`/
//! `sample`) can surface them via `--with-row-id` / `--with-row-addr`.
//!
//! Both identifiers are `UInt64` and Lance emits them **appended** to the
//! output — after the projected columns, `_rowid` first, then `_rowaddr`. This
//! module owns that convention so every command (streaming scan and positional
//! `take` alike) builds an identical output shape, and the writer header built
//! by [`extend_schema`] matches the batches produced by the scan.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::Result;
use crate::error::Error;

/// Column name for the stable Lance row id (`lance_core::ROW_ID`).
pub const ROW_ID: &str = "_rowid";
/// Column name for the physical Lance row address (`lance_core::ROW_ADDR`).
pub const ROW_ADDR: &str = "_rowaddr";

/// Which Lance system pseudo-columns to emit, from `--with-row-id` /
/// `--with-row-addr`. Copyable so it threads cheaply through [`ScanOptions`] and
/// `Dataset::take` alongside the projection.
///
/// [`ScanOptions`]: crate::dataset::ScanOptions
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RowIds {
    pub with_row_id: bool,
    pub with_row_addr: bool,
}

impl RowIds {
    /// True when either pseudo-column was requested.
    pub fn any(&self) -> bool {
        self.with_row_id || self.with_row_addr
    }

    /// The requested system column names in Lance's output order (`_rowid`
    /// before `_rowaddr`). Empty when neither flag is set.
    pub fn columns(&self) -> Vec<&'static str> {
        let mut out = Vec::with_capacity(2);
        if self.with_row_id {
            out.push(ROW_ID);
        }
        if self.with_row_addr {
            out.push(ROW_ADDR);
        }
        out
    }

    /// True when `name` is one of the *requested* system columns.
    fn requests(&self, name: &str) -> bool {
        (self.with_row_id && name == ROW_ID) || (self.with_row_addr && name == ROW_ADDR)
    }
}

/// Drop any requested system column named in `--columns`. The flags — not the
/// projection — govern whether the pseudo-columns are emitted, so naming e.g.
/// `_rowid` in `--columns` alongside `--with-row-id` is redundant rather than an
/// error: strip it here so it isn't handed to the projection resolver (which
/// would reject an unknown column) and let the flag append it in canonical
/// position. A system column named without its flag is left in place, so the
/// resolver reports it as an unknown column.
pub fn strip_columns(columns: Option<&[String]>, flags: RowIds) -> Option<Vec<String>> {
    columns.map(|cols| {
        cols.iter()
            .filter(|c| !flags.requests(c))
            .cloned()
            .collect()
    })
}

/// Reject explicitly excluding a requested pseudo-column. They are always
/// emitted when the flag is set, so `--exclude-columns _rowid --with-row-id` is
/// contradictory; error with a hint to drop the flag instead. Excluding a
/// system column *without* its flag falls through to the normal resolver, which
/// reports it as an unknown column.
pub fn validate_exclude(exclude: Option<&[String]>, flags: RowIds) -> Result<()> {
    let Some(excl) = exclude else { return Ok(()) };
    for name in excl {
        if flags.requests(name) {
            let flag = if name == ROW_ID {
                "--with-row-id"
            } else {
                "--with-row-addr"
            };
            return Err(Error::RowIdExcluded {
                column: name.clone(),
                flag,
            });
        }
    }
    Ok(())
}

/// Append the requested system fields to `schema`, producing the writer header
/// that matches the batches the scan emits: projected columns first, then
/// `_rowid`, then `_rowaddr`. Both are `UInt64` and nullable, mirroring Lance's
/// `ROW_ID_FIELD` / `ROW_ADDR_FIELD`.
pub fn extend_schema(schema: &SchemaRef, flags: RowIds) -> SchemaRef {
    if !flags.any() {
        return schema.clone();
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    for name in flags.columns() {
        fields.push(Field::new(name, DataType::UInt64, true));
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn columns_are_ordered_rowid_then_rowaddr() {
        let both = RowIds {
            with_row_id: true,
            with_row_addr: true,
        };
        assert_eq!(both.columns(), vec![ROW_ID, ROW_ADDR]);
        assert!(both.any());

        let none = RowIds::default();
        assert!(none.columns().is_empty());
        assert!(!none.any());
    }

    #[test]
    fn strip_removes_only_flagged_system_columns() {
        let flags = RowIds {
            with_row_id: true,
            with_row_addr: false,
        };
        // `_rowid` is stripped (flag set); `_rowaddr` is kept (flag unset).
        let cols = v(&["id", "_rowid", "_rowaddr"]);
        assert_eq!(
            strip_columns(Some(&cols), flags),
            Some(v(&["id", "_rowaddr"]))
        );
        // No projection stays "all columns".
        assert_eq!(strip_columns(None, flags), None);
    }

    #[test]
    fn exclude_of_flagged_system_column_errors() {
        let flags = RowIds {
            with_row_id: true,
            with_row_addr: false,
        };
        let excl = v(&["_rowid"]);
        assert!(matches!(
            validate_exclude(Some(&excl), flags),
            Err(Error::RowIdExcluded { .. })
        ));
        // Excluding `_rowaddr` while only `--with-row-id` is set is not our
        // concern: it falls through to the normal resolver.
        let excl = v(&["_rowaddr"]);
        assert!(validate_exclude(Some(&excl), flags).is_ok());
    }

    #[test]
    fn extend_schema_appends_uint64_fields() {
        let base = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let flags = RowIds {
            with_row_id: true,
            with_row_addr: true,
        };
        let out = extend_schema(&base, flags);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", ROW_ID, ROW_ADDR]);
        assert_eq!(out.field(1).data_type(), &DataType::UInt64);
        assert!(out.field(1).is_nullable());
    }
}
