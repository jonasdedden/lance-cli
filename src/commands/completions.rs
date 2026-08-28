//! `completions <shell>` — print a shell completion script to stdout.
//!
//! Generated straight from the existing [`Cli`] derive via `clap_complete`, so
//! the completions never drift from the real flags and subcommands. The command
//! takes no dataset input and bypasses the format/output machinery entirely; it
//! writes the script to stdout and exits 0. See the README install section for
//! per-shell installation instructions.

use std::io::Write;

use clap::CommandFactory;
use clap_complete::{Shell, generate};

use crate::cli::Cli;

/// The program name completions are generated for. Matches `Cli`'s `command`
/// name and the installed binary, so the generated script targets `lance-cli`.
const BIN_NAME: &str = "lance-cli";

/// Write the completion script for `shell` to `out`.
///
/// Split from stdout so tests can capture the script into a buffer and assert it
/// is non-empty and mentions the program name.
pub fn write(shell: Shell, out: &mut impl Write) {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, BIN_NAME, out);
}

/// Print the completion script for `shell` to stdout.
pub fn run(shell: Shell) {
    write(shell, &mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every supported shell must generate a non-empty script that mentions the
    /// program name, and generation must not panic.
    #[test]
    fn generates_non_empty_scripts_for_every_shell() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::PowerShell,
            Shell::Elvish,
        ] {
            let mut buf: Vec<u8> = Vec::new();
            write(shell, &mut buf);
            let script = String::from_utf8(buf).expect("completion script is valid UTF-8");
            assert!(!script.is_empty(), "{shell} script was empty");
            assert!(
                script.contains(BIN_NAME),
                "{shell} script does not mention {BIN_NAME:?}:\n{script}"
            );
        }
    }
}
