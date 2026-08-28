use std::process::ExitCode;

use lance_cli::cli::Cli;
use lance_cli::commands::Outcome;

#[tokio::main]
async fn main() -> ExitCode {
    // `parse_grouped` is `Cli::parse` with the top-level `--help` subcommands
    // rendered in grouped sections (issue #50); see `cli::command_grouped`.
    let cli = Cli::parse_grouped();
    // Exit codes follow `diff(1)`: 0 = success/identical, 1 = `diff` found
    // differences, 2 = error. Note this makes the error code 2 for *every*
    // command (it was 1 before `diff` landed), so 1 unambiguously means
    // "diff: the two versions differ" and never gets conflated with a failure.
    match lance_cli::commands::dispatch(cli).await {
        Ok(Outcome::Success) => ExitCode::SUCCESS,
        Ok(Outcome::Different) => ExitCode::from(1),
        Err(err) => {
            eprintln!("lance-cli: {err}");
            let mut source = std::error::Error::source(&err);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::from(2)
        }
    }
}
