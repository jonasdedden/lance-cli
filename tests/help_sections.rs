//! Snapshot-style tests for the grouped `--help` output (issue #47).
//!
//! Rather than pin the entire (long, wrap-sensitive) help text, these assert the
//! structural contract that matters: the section headings exist, and a
//! representative flag lands under the *correct* heading. That catches both
//! regressions (a flag drifting out of its group) and clap's classic pitfalls —
//! heading leakage (a flag inheriting the previous group's heading) and a
//! command-specific flag being swept into a named section.

use std::process::Command;

use clap::CommandFactory;
use lance_cli::cli::{COMMAND_SECTIONS, Cli};

/// Run `lance-cli <args...> --help` and return stdout.
fn help(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_lance-cli"))
        .args(args)
        .arg("--help")
        .output()
        .expect("spawn lance-cli binary");
    assert!(
        out.status.success(),
        "`lance-cli {} --help` exited non-zero",
        args.join(" ")
    );
    String::from_utf8(out.stdout).expect("help output is UTF-8")
}

/// Parse `--help` stdout into `(heading, flags-under-it)` pairs, in render order.
///
/// A "heading" is any non-indented line ending in `:` (e.g. `Options:`,
/// `Lance options:`); a "flag" is an indented line whose first token starts with
/// `-`. Long value descriptions (indented prose) are ignored because they never
/// start with `-`.
fn sections(help_text: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for line in help_text.lines() {
        if !line.starts_with(char::is_whitespace) && line.ends_with(':') && !line.is_empty() {
            out.push((line.trim_end_matches(':').to_string(), Vec::new()));
        } else if let Some(current) = out.last_mut() {
            let trimmed = line.trim_start();
            if let Some(first) = trimmed.split_whitespace().next()
                && first.starts_with('-')
            {
                // Normalize `-n,` / `--limit` etc. to the leading token.
                current.1.push(first.trim_end_matches(',').to_string());
            }
        }
    }
    out
}

/// The set of flags rendered under `heading`, or `None` if the heading is absent.
fn flags_under<'a>(secs: &'a [(String, Vec<String>)], heading: &str) -> Option<&'a [String]> {
    secs.iter()
        .find(|(h, _)| h == heading)
        .map(|(_, f)| f.as_slice())
}

fn assert_grouped(cmd: &str, secs: &[(String, Vec<String>)]) {
    // Each named section exists and contains its representative flag.
    let selection = flags_under(secs, "Selection options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Selection options` heading"));
    assert!(
        selection.iter().any(|f| f == "--columns"),
        "`{cmd}`: --columns not under `Selection options` (got {selection:?})"
    );

    let output = flags_under(secs, "Output options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Output options` heading"));
    assert!(
        output.iter().any(|f| f == "--format"),
        "`{cmd}`: --format not under `Output options` (got {output:?})"
    );
    assert!(
        output.iter().any(|f| f == "--no-progress"),
        "`{cmd}`: --no-progress not under `Output options` (got {output:?})"
    );

    let version = flags_under(secs, "Version options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Version options` heading"));
    assert!(
        version.iter().any(|f| f == "--branch"),
        "`{cmd}`: --branch not under `Version options` (got {version:?})"
    );

    // No leakage: the format/selection flags must NOT also appear in the default
    // `Options` block (which should only hold command-specific flags + --help).
    if let Some(default) = flags_under(secs, "Options") {
        for leaked in ["--format", "--columns", "--branch", "--no-progress"] {
            assert!(
                !default.iter().any(|f| f == leaked),
                "`{cmd}`: {leaked} leaked into the ungrouped `Options` block"
            );
        }
    }

    // Section ordering: Selection is rendered before Output for every command.
    let pos = |name: &str| secs.iter().position(|(h, _)| h == name);
    assert!(
        pos("Selection options") < pos("Output options"),
        "`{cmd}`: expected `Selection options` before `Output options`"
    );
}

#[test]
fn cat_help_groups_options_into_sections() {
    let secs = sections(&help(&["cat"]));
    assert_grouped("cat", &secs);

    // cat flattens RowIdArgs and FilterArg; both add Selection flags.
    let selection = flags_under(&secs, "Selection options").unwrap();
    for flag in ["--with-row-id", "--where"] {
        assert!(
            selection.iter().any(|f| f == flag),
            "cat: {flag} should be under `Selection options` (got {selection:?})"
        );
    }
}

#[test]
fn search_help_groups_options_into_sections() {
    let secs = sections(&help(&["search"]));
    assert_grouped("search", &secs);

    // search's own tuning flags stay ungrouped in the default `Options` block.
    let default = flags_under(&secs, "Options").expect("search has an `Options` block");
    for own in ["--column", "--vector", "--nprobes"] {
        assert!(
            default.iter().any(|f| f == own),
            "search: {own} should stay ungrouped in `Options` (got {default:?})"
        );
    }
}

// ── Grouped subcommand sections in the top-level `--help` (issue #50) ─────────

/// The command names listed under a top-level `--help` heading, in render order.
///
/// Command rows are `  name   one-liner`, so — unlike option rows — their first
/// token does not start with `-`; that is how they are told apart from the flag
/// rows the option sections contain.
fn commands_under(help_text: &str, heading: &str) -> Vec<String> {
    let mut in_section = false;
    let mut names = Vec::new();
    for line in help_text.lines() {
        let is_heading =
            !line.starts_with(char::is_whitespace) && line.ends_with(':') && !line.is_empty();
        if is_heading {
            in_section = line.trim_end_matches(':') == heading;
            continue;
        }
        if in_section
            && let Some(first) = line.split_whitespace().next()
            && !first.starts_with('-')
        {
            names.push(first.to_string());
        }
    }
    names
}

#[test]
fn top_level_help_groups_subcommands_into_sections() {
    let text = help(&[]); // `lance-cli --help`
    let data = commands_under(&text, "Commands");
    let metadata = commands_under(&text, "Metadata commands");
    let setup = commands_under(&text, "Setup");

    // Representative membership. The split is "reads rows" vs "reads the
    // manifest only", so `search` sits with the data commands even though it is
    // index-backed, and `stat` sits with the metadata ones.
    for c in ["cat", "diff", "blob", "schema", "search"] {
        assert!(
            data.contains(&c.to_string()),
            "`{c}` should be a data command (got {data:?})"
        );
    }
    for c in ["fragments", "versions", "stat"] {
        assert!(
            metadata.contains(&c.to_string()),
            "`{c}` should be a metadata command (got {metadata:?})"
        );
    }
    assert!(
        setup.contains(&"completions".to_string()),
        "`completions` should be under `Setup` (got {setup:?})"
    );

    // No metadata command leaks into the data section.
    for c in &metadata {
        assert!(
            !data.contains(c),
            "metadata command `{c}` leaked into the `Commands` section"
        );
    }

    // Section order: data first, metadata second, Setup last.
    let at = |h: &str| text.find(&format!("{h}:")).unwrap_or(usize::MAX);
    assert!(
        at("Commands") < at("Metadata commands"),
        "`Commands` must render before `Metadata commands`"
    );
    assert!(
        at("Metadata commands") < at("Setup"),
        "`Metadata commands` must render before `Setup`"
    );
}

/// The maintainability guard: every subcommand — including clap's auto-generated
/// `help` — must be assigned to exactly one section, so a newly added command
/// fails this test until it is placed. Also rejects stale entries that name a
/// command which no longer exists.
#[test]
fn every_subcommand_is_assigned_to_exactly_one_section() {
    let mut cmd = Cli::command();
    cmd.build(); // materializes the auto `help` subcommand, so it is covered too.

    for sub in cmd.get_subcommands() {
        let name = sub.get_name();
        let count = COMMAND_SECTIONS
            .iter()
            .filter(|(_, names)| names.contains(&name))
            .count();
        assert_eq!(
            count, 1,
            "subcommand `{name}` must be in exactly one --help section, found in {count}"
        );
    }

    let real: Vec<&str> = cmd.get_subcommands().map(|s| s.get_name()).collect();
    for (heading, names) in COMMAND_SECTIONS {
        for name in *names {
            assert!(
                real.contains(name),
                "section `{heading}` lists `{name}`, which is not a real subcommand"
            );
        }
    }
}

#[test]
fn help_subcommand_for_a_command_is_unaffected() {
    // `lance-cli help cat` and `lance-cli cat --help` must still resolve to cat's own help,
    // unchanged by the top-level grouping (subcommands are only hidden from the
    // top-level command list, not from parsing).
    for args in [["help", "cat"], ["cat", "--help"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_lance-cli"))
            .args(args)
            .output()
            .expect("spawn lance-cli binary");
        assert!(
            out.status.success(),
            "`lance-cli {}` failed",
            args.join(" ")
        );
        let text = String::from_utf8(out.stdout).expect("help output is UTF-8");
        assert!(
            text.contains("Concatenate one or more datasets"),
            "`lance-cli {}` missing cat's about line:\n{text}",
            args.join(" ")
        );
        assert!(
            text.contains("Usage: lance-cli cat"),
            "`lance-cli {}` missing cat usage:\n{text}",
            args.join(" ")
        );
    }
}
