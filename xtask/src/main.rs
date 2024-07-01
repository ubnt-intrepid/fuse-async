mod coverage;
mod doc;
mod env;
mod error;
mod hook;
mod lint;
mod process;

use pico_args::Arguments;

use crate::doc::DocBuilder;
use crate::env::Env;
pub(crate) use crate::error::TaskResult;
use crate::lint::Linter;

fn show_help() {
    eprintln!(
        "\
cargo-xtask
Free-form automation tool

Usage:
    cargo xtask <SUBCOMMAND>

Subcommands:
    lint            Run lints
    doc             Build API docs
    coverage        Run coverage test
    install-hooks   Install Git hooks
    pre-commit      Run pre-commit hook

Flags:
    -h, --help  Show this message
"
    );
}

fn main() -> TaskResult<()> {
    let mut args = Arguments::from_env();

    if args.contains(["-h", "--help"]) {
        show_help();
        return Ok(());
    }

    let env = Env::init()?;
    let subcommand = args.subcommand()?;

    match subcommand.as_deref() {
        Some("lint") => {
            args.finish();

            let linter = Linter { env: &env };
            linter.run_rustfmt()?;
            linter.run_clippy()?;
        }

        Some("doc") => {
            args.finish();

            let doc_builder = DocBuilder { env: &env };
            doc_builder.build_docs()?;
        }

        Some("coverage") => {
            args.finish();
            coverage::do_coverage(&env)?;
        }

        Some("install-hooks") => {
            let force = args.contains(["-f", "--force"]);
            args.finish();
            hook::install(&env, force)?;
        }

        Some("pre-commit") => {
            args.finish();
            hook::pre_commit(&env)?;
        }

        Some(subcommand) => {
            show_help();
            return Err(format!("unknown subcommand: {}", subcommand).into());
        }
        None => {
            show_help();
            return Err("missing subcommand".into());
        }
    }

    Ok(())
}
