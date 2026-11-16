use std::process::ExitCode;

use clap::Parser;
use syncfs::cli::Cli;
use syncfs::config::{self, Environment, Loaded};

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("syncfs: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> anyhow::Result<ExitCode> {
    let env = Environment::current()?;
    let loaded = config::load(&cli.sources(&env.cwd), &env, |p| std::fs::read_to_string(p))?;
    for w in &loaded.resolved.warnings {
        eprintln!("syncfs: warning: {w}");
    }
    if cli.print_config {
        print!("{}", render(&loaded)?);
        return Ok(ExitCode::SUCCESS);
    }
    eprintln!("syncfs: the filesystem is not implemented yet; use --print-config");
    Ok(ExitCode::from(2))
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
