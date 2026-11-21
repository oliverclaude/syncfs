use std::ffi::OsString;
use std::process::ExitCode;

use clap::Parser;
use syncfs::cli::Cli;
use syncfs::config::{self, Environment, Loaded, Mode};
use syncfs::{helper, logging, mount, reload, voter};

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
    if let Some(target) = h.remount {
        let options = helper::remount_options(&h.raw_options, &helper::kernel_options_of(&target));
        if h.verbose {
            eprintln!(
                "mount.syncfs: remount {}: -o {:?} asks for {:?}",
                target.display(),
                h.raw_options,
                options
            );
        }
        return remount(&target, options, h.sloppy, h.fake);
    }
    if h.fake {
        load(&cli, h.sloppy)?;
        return Ok(ExitCode::SUCCESS);
    }
    run(&cli, h.sloppy)
}

/// `mount -o remount`: checks the new options, then hands them to the
/// daemon serving `target` and signals it to reload. The outcome is only in
/// the daemon's log; this fails only when the request cannot be made.
fn remount(
    target: &std::path::Path,
    options: Vec<String>,
    sloppy: bool,
    fake: bool,
) -> anyhow::Result<ExitCode> {
    let env = Environment::current()?;
    let target = config::absolutize(target, &env.cwd);
    config::parse_mount_options(options.iter().map(String::as_str), &env.cwd, sloppy)?;
    if fake {
        return Ok(ExitCode::SUCCESS);
    }
    let req = reload::Request { options, sloppy };
    let pid = reload::request(&target, &req)?;
    eprintln!(
        "syncfs: asked the daemon for {} (pid {pid}) to reload; the result is in its log",
        target.display()
    );
    Ok(ExitCode::SUCCESS)
}

fn load(cli: &Cli, sloppy: bool) -> anyhow::Result<(Loaded, reload::Base)> {
    let env = Environment::current()?;
    let mut sources = cli.sources(&env.cwd);
    sources.sloppy = sloppy;
    let loaded = config::load(&sources, &env, |p| std::fs::read_to_string(p))?;
    for w in &loaded.resolved.warnings {
        eprintln!("syncfs: warning: {w}");
    }
    let base = reload::Base {
        sources,
        env,
        kernel_flags: loaded.kernel_flags.clone(),
    };
    Ok((loaded, base))
}

fn run(cli: &Cli, sloppy: bool) -> anyhow::Result<ExitCode> {
    let (loaded, base) = load(cli, sloppy)?;
    if cli.print_config {
        print!("{}", render(&loaded)?);
        return Ok(ExitCode::SUCCESS);
    }
    let cfg = &loaded.resolved.config;
    logging::init(cfg.log_level);
    match cfg.mode {
        Mode::Mount => {
            mount::run(cfg, &loaded.kernel_flags, base)?;
            Ok(ExitCode::SUCCESS)
        }
        Mode::Daemon => {
            voter::run(cfg, base)?;
            Ok(ExitCode::SUCCESS)
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
