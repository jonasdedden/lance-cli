use std::io::{BufWriter, IsTerminal, stdout};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};

use crate::Result;
use crate::cli::Format;
use crate::output::table::TableStyle;
use crate::output::{RenderOptions, RowWriter, make_writer};
use crate::row_id::{self, RowIds};

/// Reconcile the `--with-row-id` / `--with-row-addr` flags with the projection,
/// returning the `--columns` list with any redundant system-column names
/// stripped (ready for [`crate::projection::resolve`]).
///
/// Errors with [`Error::RowIdExcluded`] when a requested pseudo-column is
/// explicitly named in `--exclude-columns`.
pub fn prepare_row_id_columns(
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    row_ids: RowIds,
) -> Result<Option<Vec<String>>> {
    row_id::validate_exclude(exclude, row_ids)?;
    Ok(row_id::strip_columns(columns, row_ids))
}

/// Build the Arrow schema of the projected output.
///
/// Each entry is either a top-level column name or a validated nested path
/// (`meta.user.id`); a nested path becomes a single flat leaf field whose name
/// is the full dotted path, matching the shape Lance's scanner returns. See
/// [`crate::projection::projected_field`].
pub fn project_arrow_schema(schema: &Schema, projection: Option<&[String]>) -> SchemaRef {
    match projection {
        None => Arc::new(schema.clone()),
        Some(cols) => {
            let fields: Vec<_> = cols
                .iter()
                .map(|n| crate::projection::projected_field(schema, n))
                .collect();
            Arc::new(Schema::new(fields))
        }
    }
}

/// Returns `Ok(())` when two schemas have the same fields (name, type, nullability) in order.
/// Otherwise returns the name of the first mismatched field (or a structural description).
pub fn schemas_match(a: &SchemaRef, b: &SchemaRef) -> std::result::Result<(), String> {
    if a.fields().len() != b.fields().len() {
        return Err(format!(
            "column count differs ({} vs {})",
            a.fields().len(),
            b.fields().len()
        ));
    }
    for (fa, fb) in a.fields().iter().zip(b.fields().iter()) {
        if fa.name() != fb.name()
            || fa.data_type() != fb.data_type()
            || fa.is_nullable() != fb.is_nullable()
        {
            return Err(fa.name().clone());
        }
    }
    Ok(())
}

/// Build a buffered writer over the locked stdout in the format requested by
/// the user. The `TableStyle` is chosen here — and only here — based on
/// whether stdout is actually a terminal, because this is the one call site
/// where we know the destination really is stdout. Other callers (notably
/// tests writing into a `Cursor`) use `make_writer` directly and pin the
/// style explicitly so their output is deterministic.
pub fn make_stdout_writer(format: Format, render: RenderOptions) -> Box<dyn RowWriter> {
    let table_style = if stdout().is_terminal() {
        TableStyle::Pretty
    } else {
        TableStyle::Plain
    };
    let out = BufWriter::new(stdout().lock());
    make_writer(format, render, table_style, out)
}

/// Format a byte count with binary (KiB/MiB/…) units for human consumption.
/// Shared by the `fragments` and `stat` commands.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    // The `>= 1023.95` threshold (rather than `>= 1024.0`) bumps to the next
    // unit when the value would round up to `1024.0` at one decimal place —
    // e.g. 1,048,575 bytes shows as `1.0 MiB`, not `1024.0 KiB`.
    while value >= 1023.95 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_boundaries() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        // Just under 1 MiB must not render as `1024.0 KiB`.
        assert_eq!(human_bytes(1_048_575), "1.0 MiB");
        assert_eq!(human_bytes(1_048_576), "1.0 MiB");
    }
}
