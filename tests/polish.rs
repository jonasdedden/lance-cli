//! End-to-end tests for the CLI-polish features (issue #14): shell completions,
//! the scan progress indicator's stdout hygiene, and `cat` glob expansion.
//!
//! These drive the real compiled binary (`CARGO_BIN_EXE_lance-cli`) so that the
//! progress indicator's TTY gating is exercised for real: the test captures
//! stdout and stderr through pipes, so stderr is *not* a TTY and the indicator
//! must therefore be absent entirely — which is exactly what we assert.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use tokio::runtime::Runtime;

use common::{tempdir, write_simple};

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Run the `lance-cli` binary with `args` and capture its output (stdout + stderr are
/// both pipes, so stderr is never a TTY).
fn run(args: &[&str]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lance-cli"))
        .args(args)
        .output()
        .expect("spawn lance-cli binary")
}

fn stdout_of(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).expect("stdout is UTF-8")
}

/// Single-column `id: Int32` schema for order-sensitive glob fixtures.
fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

/// Write an `id: Int32` dataset with the given ids under `dir/name`.
async fn write_ids(dir: &Path, name: &str, ids: &[i32]) -> PathBuf {
    let path = dir.join(name);
    let batch =
        RecordBatch::try_new(id_schema(), vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap();
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), id_schema());
    lance_cli::dataset::write_dataset(&path, iter)
        .await
        .unwrap();
    path
}

// -------------------- shell completions --------------------

#[test]
fn completions_generate_for_every_shell() {
    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        let out = run(&["completions", shell]);
        assert!(out.status.success(), "completions {shell} exited non-zero");
        let script = stdout_of(&out);
        assert!(!script.is_empty(), "completions {shell} produced no output");
        assert!(
            script.contains("lance-cli"),
            "completions {shell} script does not mention the program name:\n{script}"
        );
        // A dataset input must not be required, and no scan machinery runs, so
        // nothing should reach stderr.
        assert!(
            out.stderr.is_empty(),
            "completions {shell} wrote to stderr: {:?}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn completions_include_every_command_including_metadata_ones() {
    // Grouping the top-level `--help` hides subcommands from the *help command
    // list* only (issue #50); completions are generated from a separate,
    // un-hidden `Cli::command()`, so every command — data, metadata, and Setup —
    // must still be completable. `fragments` (metadata) and `blob`/`diff` (data)
    // are the canaries.
    for shell in ["bash", "zsh", "fish"] {
        let script = stdout_of(&run(&["completions", shell]));
        for cmd in ["fragments", "search", "blob", "diff", "completions"] {
            assert!(
                script.contains(cmd),
                "{shell} completions missing command `{cmd}`:\n{script}"
            );
        }
    }
}

#[test]
fn completions_rejects_unknown_shell() {
    let out = run(&["completions", "notashell"]);
    assert!(!out.status.success(), "unknown shell should be rejected");
}

#[test]
fn completions_rejects_format_flag() {
    // `--format` does not apply to `completions` (it prints a shell script, not
    // row-shaped output), so it must hard-error per the repo convention rather
    // than being silently ignored.
    let out = run(&["completions", "bash", "--format", "csv"]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "--format on completions should exit 2"
    );
    assert!(
        out.stdout.is_empty(),
        "no completion script should be printed"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not applicable to 'completions'"),
        "stderr missing FormatNotApplicable message, got: {stderr}"
    );
}

// -------------------- progress / stdout hygiene --------------------

#[test]
fn cat_stdout_is_clean_and_stderr_empty_when_piped() {
    let tmp = tempdir();
    let path = runtime().block_on(async { write_simple(&tmp, "s").await });
    let p = path.to_str().unwrap();

    let out = run(&["cat", "--format", "csv", p]);
    assert!(out.status.success(), "cat failed: {out:?}");
    // Progress draws only to stderr and only on a TTY; here stderr is a pipe, so
    // the indicator must be absent entirely.
    assert!(
        out.stderr.is_empty(),
        "stderr should be empty when piped, got: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = stdout_of(&out);
    assert!(
        stdout.starts_with("id,name,score"),
        "unexpected stdout:\n{stdout}"
    );
}

#[test]
fn no_progress_produces_byte_identical_stdout() {
    let tmp = tempdir();
    let path = runtime().block_on(async { write_simple(&tmp, "s").await });
    let p = path.to_str().unwrap();

    let with_default = run(&["cat", "--format", "csv", p]);
    let with_flag = run(&["cat", "--format", "csv", "--no-progress", p]);
    assert!(with_default.status.success() && with_flag.status.success());
    assert_eq!(
        with_default.stdout, with_flag.stdout,
        "--no-progress must not change stdout"
    );
}

#[test]
fn no_progress_is_accepted_everywhere() {
    let tmp = tempdir();
    let path = runtime().block_on(async { write_simple(&tmp, "s").await });
    let p = path.to_str().unwrap();

    // A representative spread of scanning and non-scanning commands.
    let cases: &[&[&str]] = &[
        &["cat", "--no-progress"],
        &["head", "--no-progress", "-n", "2"],
        &["tail", "--no-progress", "-n", "2"],
        &["rowcount", "--no-progress"],
        &["sample", "--no-progress", "-n", "2", "--seed", "1"],
        &["stats", "--no-progress"],
        &["freq", "--no-progress", "--column", "id"],
        &["schema", "--no-progress"],
    ];
    for case in cases {
        let mut args = case.to_vec();
        args.push(p);
        let out = run(&args);
        assert!(
            out.status.success(),
            "`lance-cli {}` with --no-progress failed: {}",
            case.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

// -------------------- cat glob expansion --------------------

#[test]
fn cat_glob_expands_and_concatenates_in_lexicographic_order() {
    let tmp = tempdir();
    let dir = tmp.path();
    runtime().block_on(async {
        // Created out of order; the a-before-b output proves the sort.
        write_ids(dir, "part_b.lance", &[3, 4]).await;
        write_ids(dir, "part_a.lance", &[1, 2]).await;
    });
    let pattern = dir.join("part_*.lance").to_string_lossy().into_owned();

    let out = run(&["cat", "--format", "csv", &pattern]);
    assert!(out.status.success(), "glob cat failed: {out:?}");
    let stdout = stdout_of(&out);
    // Header once, then part_a's ids (1,2) before part_b's (3,4).
    assert_eq!(
        stdout, "id\n1\n2\n3\n4\n",
        "unexpected concatenation:\n{stdout}"
    );
}

#[test]
fn cat_glob_no_match_is_an_error() {
    let tmp = tempdir();
    let pattern = tmp
        .path()
        .join("nomatch_*.lance")
        .to_string_lossy()
        .into_owned();
    let out = run(&["cat", &pattern]);
    // Errors exit 2 (the repo's convention; 1 is reserved for `diff` deltas).
    assert_eq!(out.status.code(), Some(2), "no-match glob should exit 2");
    assert!(out.stdout.is_empty(), "no-match glob must not write stdout");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("matched no files"),
        "stderr missing no-match message, got: {stderr}"
    );
}
