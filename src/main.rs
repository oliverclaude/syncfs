use std::ffi::OsString;
use std::process::ExitCode;

use clap::Parser;
use syncfs::cli::Cli;
use syncfs::config::{self, Environment, Loaded, Mode};
use syncfs::{helper, logging, mount};

fn main() -> ExitCode {
    let argv: Vec<OsString> = std::env::args_os().collect();
    let result = if argv.first().is_some_and(|a| helper::invoked_as_helper(a)) {
        run_helper(argv)
    } else {
        run(&Cli::parse(), false)
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("syncfs: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run_helper(argv: Vec<OsString>) -> anyhow::Result<ExitCode> {
    let h = helper::parse(argv.into_iter().skip(1))?;
    let cli = Cli::try_parse_from(std::iter::once(OsString::from("syncfs")).chain(h.args))?;
    if h.verbose {
        eprintln!("mount.syncfs: {cli:?}");
    }
    if h.fake {
        load(&cli, h.sloppy)?;
        return Ok(ExitCode::SUCCESS);
    }
    run(&cli, h.sloppy)
}

fn load(cli: &Cli, sloppy: bool) -> anyhow::Result<Loaded> {
    let env = Environment::current()?;
    let mut sources = cli.sources(&env.cwd);
    sources.sloppy = sloppy;
    let loaded = config::load(&sources, &env, |p| std::fs::read_to_string(p))?;
    for w in &loaded.resolved.warnings {
        eprintln!("syncfs: warning: {w}");
    }
    Ok(loaded)
}

fn run(cli: &Cli, sloppy: bool) -> anyhow::Result<ExitCode> {
    let loaded = load(cli, sloppy)?;
    if cli.print_config {
        print!("{}", render(&loaded)?);
        return Ok(ExitCode::SUCCESS);
    }
    let cfg = &loaded.resolved.config;
    logging::init(cfg.log_level);
    match cfg.mode {
        Mode::Mount => {
            mount::run(cfg, &loaded.kernel_flags)?;
            Ok(ExitCode::SUCCESS)
        }
        Mode::Daemon => {
            eprintln!("syncfs: daemon mode is not implemented yet (step 5)");
            Ok(ExitCode::from(2))
        }
    }
}

fn render(loaded: &Loaded) -> anyhow::Result<String> {
    let c = &loaded.resolved.config;
    let source = loaded.config_file.as_ref().map_or_else(
        || "none (built-in defaults)".to_string(),
        |p| p.display().to_string(),
    );
    Ok(format!(
        "# effective syncfs configuration\n\
         # config file: {source}\n\
         # cluster size: {}, quorum: {}\n{}",
        c.cluster_size(),
        c.quorum(),
        serde_yaml_ng::to_string(c)?
    ))
}
