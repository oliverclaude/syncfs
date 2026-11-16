//! Reads every configuration source and stacks them:
//! built-in defaults < YAML file < `-o` options < CLI flags.

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::options::{parse_mount_options, OptionError};
use super::partial::PartialConfig;
use super::resolve::{resolve, Environment, Resolved, ValidationErrors, DEFAULT_CONFIG_PATH};

/// Everything the command line provides.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sources {
    /// Settings from explicit CLI flags and positional arguments.
    pub cli: PartialConfig,
    /// `--config PATH`.
    pub cli_config_file: Option<PathBuf>,
    /// Raw `-o` arguments, in order.
    pub mount_options: Vec<String>,
}

/// The effective configuration and where it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loaded {
    pub resolved: Resolved,
    /// The YAML file that was read, or `None` if the default file does not
    /// exist and none was requested.
    pub config_file: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum LoadError {
    #[error(transparent)]
    Options(#[from] OptionError),
    #[error("cannot read config file {path:?}: {source}")]
    Read { path: PathBuf, source: io::Error },
    #[error("invalid config file {path:?}: {source}")]
    Yaml {
        path: PathBuf,
        source: serde_yaml_ng::Error,
    },
    #[error("{0}")]
    Invalid(#[from] ValidationErrors),
}

/// Loads the configuration. The config file is `--config`, else
/// `-o config=`, else [`DEFAULT_CONFIG_PATH`]; a missing default file is
/// not an error, a missing requested file is. `read_file` is injected so
/// tests need no real files.
pub fn load(
    sources: &Sources,
    env: &Environment,
    read_file: impl Fn(&Path) -> io::Result<String>,
) -> Result<Loaded, LoadError> {
    let opts = parse_mount_options(sources.mount_options.iter().map(String::as_str), &env.cwd)?;

    let requested = sources.cli_config_file.clone().or(opts.config_file);
    let path = requested
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (file_layer, config_file) = match read_file(&path) {
        Ok(text) => {
            let layer = PartialConfig::from_yaml(&text).map_err(|source| LoadError::Yaml {
                path: path.clone(),
                source,
            })?;
            (layer, Some(path))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound && requested.is_none() => {
            (PartialConfig::default(), None)
        }
        Err(source) => return Err(LoadError::Read { path, source }),
    };

    let merged = file_layer.overlay(opts.layer).overlay(sources.cli.clone());
    Ok(Loaded {
        resolved: resolve(merged, env)?,
        config_file,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ByteSize, LogLevel, Mode};

    const YAML: &str = r#"
node_id: 2
listen: "0.0.0.0:7420"
peers:
  - { id: 1, address: "10.0.0.1:7420" }
  - { id: 2, address: "10.0.0.2:7420" }
  - { id: 3, address: "10.0.0.3:7420" }
backing_dir: /srv/yaml
mountpoint: /mnt/yaml
block_size: 128K
log_level: warn
debug: true
"#;

    fn env() -> Environment {
        Environment {
            is_root: true,
            home: None,
            xdg_state_home: None,
            cwd: "/cwd".into(),
        }
    }

    fn files(path: &Path) -> io::Result<String> {
        match path.to_str() {
            Some(DEFAULT_CONFIG_PATH) | Some("/etc/other.yaml") => Ok(YAML.into()),
            Some("/etc/broken.yaml") => Ok("node_id: [".into()),
            Some("/etc/denied.yaml") => Err(io::ErrorKind::PermissionDenied.into()),
            _ => Err(io::ErrorKind::NotFound.into()),
        }
    }

    fn no_files(_: &Path) -> io::Result<String> {
        Err(io::ErrorKind::NotFound.into())
    }

    fn opts(o: &[&str]) -> Sources {
        Sources {
            mount_options: o.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn yaml_overrides_defaults() {
        let l = load(&Sources::default(), &env(), files).unwrap();
        assert_eq!(l.config_file, Some(DEFAULT_CONFIG_PATH.into()));
        let c = l.resolved.config;
        assert_eq!(c.node_id, 2);
        assert_eq!(c.quorum(), 2);
        assert_eq!(c.block_size, ByteSize(128 * 1024));
        assert_eq!(c.log_level, LogLevel::Warn);
        // Unset in YAML: defaults.
        assert_eq!(c.state_dir, PathBuf::from("/var/lib/syncfs"));
        assert_eq!(c.mode, Mode::Mount);
    }

    #[test]
    fn options_override_yaml() {
        let l = load(
            &opts(&["node_id=3,block_size=4K", "nodebug"]),
            &env(),
            files,
        )
        .unwrap();
        let c = l.resolved.config;
        assert_eq!(c.node_id, 3);
        assert_eq!(c.block_size, ByteSize(4096));
        assert!(!c.debug);
        assert_eq!(c.backing_dir, Some("/srv/yaml".into()));
    }

    #[test]
    fn cli_overrides_options() {
        let mut s = opts(&["node_id=3,backing_dir=/srv/opt,peer=1@a:1,peer=3@c:1"]);
        s.cli.node_id = Some(1);
        s.cli.backing_dir = Some("/srv/cli".into());
        let c = load(&s, &env(), files).unwrap().resolved.config;
        assert_eq!(c.node_id, 1);
        assert_eq!(c.backing_dir, Some("/srv/cli".into()));
        // -o peers replaced the YAML list.
        assert_eq!(c.peers.len(), 2);
        assert_eq!(c.mountpoint, Some("/mnt/yaml".into()));
    }

    #[test]
    fn config_file_selection() {
        // -o config= is used when --config is absent.
        let l = load(&opts(&["config=/etc/other.yaml"]), &env(), files).unwrap();
        assert_eq!(l.config_file, Some("/etc/other.yaml".into()));
        // --config beats -o config=.
        let mut s = opts(&["config=/etc/broken.yaml"]);
        s.cli_config_file = Some("/etc/other.yaml".into());
        assert!(load(&s, &env(), files).is_ok());
        // Relative -o config= is resolved against cwd.
        let e = load(&opts(&["config=x.yaml"]), &env(), files).unwrap_err();
        assert!(matches!(e, LoadError::Read { ref path, .. } if path == Path::new("/cwd/x.yaml")));
    }

    #[test]
    fn missing_default_file_is_fine() {
        let mut s = Sources::default();
        s.cli.backing_dir = Some("/a".into());
        s.cli.mountpoint = Some("/b".into());
        let l = load(&s, &env(), no_files).unwrap();
        assert_eq!(l.config_file, None);
        assert_eq!(l.resolved.config.node_id, 1);
    }

    #[test]
    fn file_errors() {
        let mut s = Sources {
            cli_config_file: Some("/etc/missing.yaml".into()),
            ..Default::default()
        };
        assert!(matches!(
            load(&s, &env(), files),
            Err(LoadError::Read { .. })
        ));
        s.cli_config_file = Some("/etc/denied.yaml".into());
        assert!(matches!(
            load(&s, &env(), files),
            Err(LoadError::Read { .. })
        ));
        s.cli_config_file = Some("/etc/broken.yaml".into());
        assert!(matches!(
            load(&s, &env(), files),
            Err(LoadError::Yaml { .. })
        ));
        assert!(matches!(
            load(&opts(&["wat"]), &env(), files),
            Err(LoadError::Options(_))
        ));
        assert!(matches!(
            load(&opts(&["node_id=9"]), &env(), files),
            Err(LoadError::Invalid(_))
        ));
    }
}
