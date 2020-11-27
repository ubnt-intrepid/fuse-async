mod hook;

use anyhow::Context as _;
use pico_args::Arguments;
use std::{
    env, fs, io,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};
use wait_timeout::ChildExt as _;

fn main() -> anyhow::Result<()> {
    let show_help = || {
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
    };

    let mut args = Arguments::from_env();

    if args.contains(["-h", "--help"]) {
        show_help();
        return Ok(());
    }

    let subcommand = args.subcommand()?;
    match subcommand.as_deref() {
        Some("lint") => {
            args.finish()?;
            do_lint()
        }
        Some("doc") => {
            args.finish()?;
            do_doc()
        }
        Some("coverage") => {
            args.finish()?;
            do_coverage()
        }
        Some("install-hooks") => {
            let force = args.contains(["-f", "--force"]);
            args.finish()?;
            hook::install(force)
        }
        Some("pre-commit") => {
            args.finish()?;
            hook::pre_commit()
        }
        Some(subcommand) => {
            show_help();
            anyhow::bail!("unknown subcommand: {}", subcommand);
        }
        None => {
            show_help();
            anyhow::bail!("missing subcommand");
        }
    }
}

fn do_lint() -> anyhow::Result<()> {
    let has_rustfmt = cargo().args(&["fmt", "--version"]).run_silent().is_ok();
    let has_clippy = cargo().args(&["clippy", "--version"]).run_silent().is_ok();

    if has_rustfmt {
        cargo().args(&["fmt", "--", "--check"]).run()?;
    }

    if has_clippy {
        cargo().args(&["clippy", "--all-targets"]).run()?;
    }

    Ok(())
}

fn do_doc() -> anyhow::Result<()> {
    // TODOs:
    // * restrict network access during building docs.
    // * restrict all write access expect target/

    // ref: https://blog.rust-lang.org/2019/09/18/upcoming-docsrs-changes.html#what-will-change
    const CARGO_DOC_TIMEOUT: Duration = Duration::from_secs(60 * 15);

    let doc_dir = target_dir().join("doc");
    if doc_dir.exists() {
        fs::remove_dir_all(&doc_dir)?;
    }

    for package in &[
        "polyfuse",
        "polyfuse-kernel",
        "polyfuse-mount",
        "polyfuse-async-std",
    ] {
        cargo()
            .arg("doc")
            .arg("--no-deps")
            .arg(format!("--package={}", package))
            .run_timeout(CARGO_DOC_TIMEOUT)?;
    }

    let lockfile = doc_dir.join(".lock");
    if lockfile.exists() {
        fs::remove_file(lockfile)?;
    }

    let indexfile = doc_dir.join("index.html");
    fs::write(
        indexfile,
        "<meta http-equiv=\"refresh\" content=\"0;url=polyfuse\">\n",
    )?;

    Ok(())
}

fn do_coverage() -> anyhow::Result<()> {
    // Ref: https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/source-based-code-coverage.html

    const BLACKLIST: &[&str] = &[
        "polyfuse_example_basic", //
        "polyfuse_example_hello",
    ];

    println!("[cargo-xtask] Build instrumented tests...");
    let tests = build_instrumented_tests()?;

    let cov_dir = target_dir().join("cov");

    for test in &tests {
        let test_name = test
            .file_stem()
            .context("no file name")?
            .to_str()
            .context("invalid file stem")?;

        if BLACKLIST.iter().any(|prefix| test_name.starts_with(prefix)) {
            println!("[cargo-xtask] Skip coverage test of {}", test_name);
            continue;
        }

        let profraw_file = cov_dir.join(format!("{}.profraw", test_name));
        let profdata_file = cov_dir.join(format!("{}.profdata", test_name));
        let report_dir = cov_dir.join("report").join(test_name);

        // Run instrumented test
        Command::new(&test)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("LLVM_PROFILE_FILE", &profraw_file)
            .run()?;

        println!("[cargo-xtask] Collect coverage profile for {}", test_name);
        Command::new("llvm-profdata")
            .arg("merge")
            .arg("-sparse")
            .arg(&profraw_file)
            .arg("-o")
            .arg(&profdata_file)
            .run()?;

        println!("[cargo-xtask] Create coverage report");
        Command::new("llvm-cov")
            .arg("show")
            .arg("-Xdemangler=rustfilt")
            .arg("-show-line-counts-or-regions")
            .arg("-show-instantiations")
            .arg(format!("-instr-profile={}", profdata_file.display()))
            .arg(format!("-output-dir={}", report_dir.display()))
            .arg("-format=html")
            .arg(test)
            .run()?;
    }

    Ok(())
}

fn build_instrumented_tests() -> anyhow::Result<Vec<PathBuf>> {
    use miniserde::json::{self, Value};
    use std::io::{BufRead as _, BufReader};

    let mut child = cargo()
        .arg("test")
        .arg("--no-run")
        .arg("--workspace")
        .arg("--message-format=json")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .env("CARGO_BUILD_RUSTFLAGS", "-Zinstrument-coverage")
        .env("CARGO_TERM_PROGRESS_WHEN", "never")
        .spawn()
        .context("Failed to spawn cargo command")?;

    let stdout = child.stdout.take().context("stdout is not set")?;
    let stdout = BufReader::new(stdout);

    let mut executables = vec![];
    for line in stdout.lines() {
        if let Ok(line) = line {
            if let Ok(line) = json::from_str(line.trim_end()) {
                if let Value::Object(obj) = line {
                    if let Some(Value::String(ref executable)) = obj.get("executable") {
                        executables.push(PathBuf::from(executable));
                    }
                }
            }
        }
    }

    child.wait_timeout(Duration::from_millis(10))?;

    Ok(executables)
}

fn cargo() -> Command {
    let cargo = env::var_os("CARGO")
        .or_else(|| option_env!("CARGO").map(Into::into))
        .unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.current_dir(project_root());
    command.stdin(Stdio::null());
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());
    command.env("CARGO_INCREMENTAL", "0");
    command.env("CARGO_NET_OFFLINE", "true");
    command.env("RUST_BACKTRACE", "full");
    command
}

trait CommandExt {
    fn run(&mut self) -> anyhow::Result<()>;
    fn run_timeout(&mut self, timeout: Duration) -> anyhow::Result<()>;
    fn run_silent(&mut self) -> anyhow::Result<()>;
}

impl CommandExt for Command {
    fn run(&mut self) -> anyhow::Result<()> {
        run_impl(self, None)
    }

    fn run_timeout(&mut self, timeout: Duration) -> anyhow::Result<()> {
        run_impl(self, Some(timeout))
    }

    fn run_silent(&mut self) -> anyhow::Result<()> {
        self.stdout(Stdio::null());
        self.stderr(Stdio::null());
        self.run()
    }
}

fn run_impl(cmd: &mut Command, timeout: Option<Duration>) -> anyhow::Result<()> {
    eprintln!("[cargo-xtask] run {:?}", cmd);

    let mut child = cmd.spawn().context("failed to spawn the subprocess")?;

    let st = match timeout {
        Some(timeout) => match child.wait_timeout(timeout)? {
            Some(st) => st,
            None => {
                if let Err(err) = child.kill() {
                    match err.kind() {
                        io::ErrorKind::InvalidInput => (),
                        _ => anyhow::bail!(err),
                    }
                }
                child.wait()?
            }
        },
        None => child.wait()?,
    };

    anyhow::ensure!(st.success(), "Subprocess failed: {}", st);

    Ok(())
}

fn project_root() -> PathBuf {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_MANIFEST_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("./xtask"));
    manifest_dir.parent().unwrap().to_owned()
}

fn target_dir() -> PathBuf {
    env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| project_root().join("target"))
}
