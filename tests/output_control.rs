//! Tests for the #11 output-control flags: `--max-list-items`,
//! `--max-cell-width`, and `--float-precision`.
//!
//! These drive the writers directly through `make_writer` + `RenderOptions`
//! over hand-built `RecordBatch`es, which is exactly the layer the flags touch.
//! Defaults are asserted byte-identical for a truncatable case.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    FixedSizeListArray, Float32Array, Float64Array, Int32Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance_cli::cli::Format;
use lance_cli::output::table::TableStyle;
use lance_cli::output::{RenderOptions, make_writer};
use serde_json::Value;

// ---------- fixtures ----------

fn emb_child_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float32, false))
}

/// Schema: id (Int32), emb (FixedSizeList<Float32; 6>), score (Float64),
/// text (Utf8). Exercises the embedding truncation headline plus float
/// precision and cell-width truncation.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new_fixed_size_list("emb", emb_child_field(), 6, false),
        Field::new("score", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
    ]))
}

fn batch(n: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..n as i32).collect();
    let mut vals: Vec<f32> = Vec::with_capacity(n * 6);
    for r in 0..n {
        for c in 0..6 {
            vals.push((r * 6 + c) as f32 / 100.0);
        }
    }
    let emb = FixedSizeListArray::new(
        emb_child_field(),
        6,
        Arc::new(Float32Array::from(vals)),
        None,
    );
    let scores = Float64Array::from(
        (0..n)
            .map(|r| Some(0.123456 + r as f64))
            .collect::<Vec<_>>(),
    );
    let text = StringArray::from(
        (0..n)
            .map(|r| {
                Some(format!(
                    "row-{r}-with-a-fairly-long-value-that-should-truncate"
                ))
            })
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(emb),
            Arc::new(scores),
            Arc::new(text),
        ],
    )
    .unwrap()
}

fn render(format: Format, opts: RenderOptions, batch: &RecordBatch) -> String {
    let schema = batch.schema();
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(format, opts, TableStyle::Plain, Cursor::new(&mut out));
        w.start(&schema).unwrap();
        w.write_batch(batch).unwrap();
        w.finish().unwrap();
    }
    String::from_utf8(out).unwrap()
}

fn opts() -> RenderOptions {
    RenderOptions::default()
}

// ---------- defaults are byte-identical ----------

#[test]
fn defaults_render_full_list_without_marker() {
    // Regression guard: with no options set, the (truncatable) embedding column
    // renders every element and carries no marker.
    let b = batch(1);
    let jsonl = render(Format::Jsonl, opts(), &b);
    let v: Value = serde_json::from_str(jsonl.trim()).unwrap();
    let emb = v["emb"].as_array().unwrap();
    assert_eq!(emb.len(), 6, "default must render all 6 elements");
    assert!(
        emb.iter().all(|e| e.is_number()),
        "no truncation marker under defaults"
    );
}

#[test]
fn default_float_matches_full_precision_string() {
    // 0.123456 must keep its shortest round-trip form under defaults.
    let b = batch(1);
    let jsonl = render(Format::Jsonl, opts(), &b);
    let v: Value = serde_json::from_str(jsonl.trim()).unwrap();
    assert_eq!(v["score"].as_f64().unwrap(), 0.123456);
}

/// Golden JSONL bytes under default options. Locks the exact serialization,
/// including that an `f32` is widened to `f64` before rendering (so `0.1_f32`
/// prints as its long widened form `0.10000000149011612`), an `f64` keeps its
/// own shortest form (`0.2`), and a list renders in full with no marker.
#[test]
fn default_jsonl_line_is_byte_identical_golden() {
    let s = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
        Field::new_list(
            "tags",
            Arc::new(Field::new("item", DataType::Utf8, true)),
            false,
        ),
    ]));
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.values().append_value("x");
    tags.values().append_value("y");
    tags.append(true);
    let b = RecordBatch::try_new(
        s,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float32Array::from(vec![0.1_f32])),
            Arc::new(Float64Array::from(vec![0.2_f64])),
            Arc::new(tags.finish()),
        ],
    )
    .unwrap();
    let jsonl = render(Format::Jsonl, opts(), &b);
    assert_eq!(
        jsonl,
        "{\"id\":1,\"a\":0.10000000149011612,\"b\":0.2,\"tags\":[\"x\",\"y\"]}\n"
    );
}

/// Golden CSV bytes under default options. The same `f32`/`f64` values render
/// with their *format-native* shortest forms here (`0.1` for the f32, unlike
/// JSON's widened form), which is exactly the behavior the byte-identical
/// default guarantee must pin.
#[test]
fn default_csv_lines_are_byte_identical_golden() {
    let s = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let b = RecordBatch::try_new(
        s,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float32Array::from(vec![0.1_f32])),
            Arc::new(Float64Array::from(vec![0.2_f64])),
        ],
    )
    .unwrap();
    let csv = render(Format::Csv, opts(), &b);
    assert_eq!(csv, "id,a,b\n1,0.1,0.2\n");
}

// ---------- --max-list-items ----------

#[test]
fn max_list_items_truncates_fixed_size_list_in_jsonl() {
    let b = batch(1);
    let o = RenderOptions {
        max_list_items: Some(4),
        ..opts()
    };
    let jsonl = render(Format::Jsonl, o, &b);
    let v: Value = serde_json::from_str(jsonl.trim()).unwrap();
    let emb = v["emb"].as_array().unwrap();
    assert_eq!(emb.len(), 5, "4 elements + 1 marker");
    assert_eq!(emb[4], Value::String("… (2 more)".to_string()));
    assert!(emb[..4].iter().all(|e| e.is_number()));
}

#[test]
fn max_list_items_marker_keeps_valid_json() {
    let b = batch(3);
    let o = RenderOptions {
        max_list_items: Some(1),
        ..opts()
    };
    let jsonl = render(Format::Jsonl, o, &b);
    for line in jsonl.lines() {
        // Every truncated line is still parseable JSON.
        let v: Value = serde_json::from_str(line).unwrap();
        let emb = v["emb"].as_array().unwrap();
        assert_eq!(emb.len(), 2);
        assert_eq!(emb[1], Value::String("… (5 more)".to_string()));
    }
}

#[test]
fn max_list_items_appears_in_table_nested_cell() {
    let b = batch(1);
    let o = RenderOptions {
        max_list_items: Some(2),
        ..opts()
    };
    let table = render(Format::Table, o, &b);
    assert!(
        table.contains("… (4 more)"),
        "table nested cell should carry the marker: {table}"
    );
}

// ---------- --float-precision ----------

#[test]
fn float_precision_rounds_in_jsonl_and_stays_a_number() {
    let b = batch(1);
    let o = RenderOptions {
        float_precision: Some(3),
        ..opts()
    };
    let jsonl = render(Format::Jsonl, o, &b);
    let v: Value = serde_json::from_str(jsonl.trim()).unwrap();
    assert!(v["score"].is_number());
    // arbitrary_precision keeps the exact "0.123" digits.
    assert_eq!(v["score"].to_string(), "0.123");
}

#[test]
fn float_precision_rounds_in_csv() {
    // CSV rejects nested columns, so use a flat score-only batch.
    let s = Arc::new(Schema::new(vec![Field::new(
        "score",
        DataType::Float64,
        false,
    )]));
    let b = RecordBatch::try_new(s, vec![Arc::new(Float64Array::from(vec![0.123456]))]).unwrap();
    let o = RenderOptions {
        float_precision: Some(2),
        ..opts()
    };
    let csv = render(Format::Csv, o, &b);
    // header line + one data line.
    let data = csv.lines().nth(1).unwrap();
    assert_eq!(data, "0.12");
}

// ---------- --max-cell-width (table only) ----------

#[test]
fn max_cell_width_truncates_with_ellipsis() {
    let b = batch(1);
    let o = RenderOptions {
        max_cell_width: Some(10),
        ..opts()
    };
    let table = render(Format::Table, o, &b);
    // The long text cell must be shortened and end with the ellipsis.
    assert!(table.contains("row-0-wit…"), "got: {table}");
    // And the full untruncated value must be gone.
    assert!(!table.contains("should-truncate"));
}

#[test]
fn max_cell_width_never_splits_a_codepoint() {
    // A cell of multi-byte characters truncated mid-string must stay valid
    // UTF-8 and count characters, not bytes.
    let s = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
    let b = RecordBatch::try_new(
        s.clone(),
        vec![Arc::new(StringArray::from(vec!["日本語のテキストです"]))],
    )
    .unwrap();
    let o = RenderOptions {
        max_cell_width: Some(5),
        ..opts()
    };
    let table = render(Format::Table, o, &b);
    // 4 kept chars + ellipsis; must be present verbatim and valid UTF-8.
    assert!(table.contains("日本語の…"), "got: {table}");
}

// ---------- combined flags ----------

#[test]
fn combined_jsonl_truncation_and_precision() {
    let b = batch(2);
    let o = RenderOptions {
        max_list_items: Some(3),
        float_precision: Some(2),
        ..opts()
    };
    let jsonl = render(Format::Jsonl, o, &b);
    let lines: Vec<&str> = jsonl.lines().collect();
    assert_eq!(lines.len(), 2);
    for line in lines {
        let obj: Value = serde_json::from_str(line).unwrap();
        let emb = obj["emb"].as_array().unwrap();
        assert_eq!(emb.len(), 4); // 3 + marker
        assert_eq!(emb[3], Value::String("… (3 more)".to_string()));
        assert!(obj["score"].is_number());
    }
}

// ---------- CSV nested rejection unaffected ----------

#[test]
fn csv_still_rejects_nested_even_with_max_list_items() {
    let b = batch(1);
    let o = RenderOptions {
        max_list_items: Some(2),
        ..opts()
    };
    let schema = b.schema();
    let mut out: Vec<u8> = Vec::new();
    let mut w = make_writer(Format::Csv, o, TableStyle::Plain, Cursor::new(&mut out));
    // The fixed-size-list column is not CSV-representable; start() must reject it.
    let err = w.start(&schema);
    assert!(err.is_err(), "CSV must reject the nested embedding column");
}
