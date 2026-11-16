//! Runs the real `syncfs` binary.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn syncfs(args: &[&str]) -> Output {
    let home = std::env::temp_dir().join("syncfs-cli-test-home");
    Command::new(env!("CARGO_BIN_EXE_syncfs"))
        .args(args)
        .env("HOME", &home)
        .env_remove("XDG_STATE_HOME")
        .current_dir(std::env::temp_dir())
        .output()
        .expect("run syncfs")
}

fn example() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/syncfs.yaml")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn example_config_is_valid_and_complete() {
    let ex = example();
    let o = syncfs(&["--config", ex.to_str().unwrap(), "--print-config"]);
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    assert!(out.contains("# cluster size: 3, quorum: 2"), "{out}");
    assert!(out.contains("backing_dir: /srv/syncfs"), "{out}");

    // The example documents every key: its YAML keys equal the printed ones.
    let keys = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|l| !l.starts_with(['#', ' ', '-']) && l.contains(':'))
            .map(|l| l.split(':').next().unwrap().to_string())
            .collect()
    };
    assert_eq!(keys(&std::fs::read_to_string(&ex).unwrap()), keys(&out));
}

#[test]
fn print_config_round_trips_as_yaml() {
    let ex = example();
    let first = stdout(&syncfs(&[
        "--config",
        ex.to_str().unwrap(),
        "--print-config",
    ]));
    let dumped = std::env::temp_dir().join("syncfs-cli-test-roundtrip.yaml");
    std::fs::write(&dumped, &first).unwrap();
    let second = syncfs(&["--config", dumped.to_str().unwrap(), "--print-config"]);
    assert!(second.status.success(), "{}", stderr(&second));
    let body = |s: &str| {
        s.lines()
            .skip(1)
            .filter(|l| !l.starts_with("# config file"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(body(&first), body(&stdout(&second)));
}

#[test]
fn precedence_file_then_options_then_flags() {
    let ex = example();
    let o = syncfs(&[
        "--config",
        ex.to_str().unwrap(),
        "-o",
        "node_id=2,block_size=4K,log_level=warn",
        "--log-level",
        "trace",
        "--print-config",
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    assert!(out.contains("node_id: 2\n"), "{out}"); // -o over file
    assert!(out.contains("block_size: 4KiB\n"), "{out}"); // -o over file
    assert!(out.contains("log_level: trace\n"), "{out}"); // flag over -o
    assert!(out.contains("mountpoint: /mnt/syncfs\n"), "{out}"); // file over default
    assert!(out.contains("heartbeat_interval: 1s\n"), "{out}");
}

#[test]
fn invalid_config_fails_with_every_reason() {
    let o = syncfs(&["--print-config", "-o", "node_id=0,block_size=3"]);
    assert_eq!(o.status.code(), Some(1));
    let err = stderr(&o);
    for needle in [
        "node_id must be",
        "needs backing_dir",
        "needs mountpoint",
        "block_size 3",
    ] {
        assert!(err.contains(needle), "{needle}: {err}");
    }
}

#[test]
fn missing_requested_config_file_fails() {
    let o = syncfs(&["--config", "/nonexistent/syncfs.yaml", "--print-config"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("cannot read config file"));
}

#[test]
fn daemon_mode_needs_no_paths() {
    let o = syncfs(&["--mode", "daemon", "--print-config"]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(stdout(&o).contains("mode: daemon\n"));
}
