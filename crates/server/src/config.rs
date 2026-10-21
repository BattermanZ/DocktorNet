use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use url::Url;

/// The public Cloudflare v4 API, used unless `DOCKTORNET_CF_API_URL` overrides it.
pub const CLOUDFLARE_API_URL: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerConfig {
    pub listen: String,
    pub secret: String,
    pub db_path: PathBuf,
    /// Start even when the database sits on a container's own filesystem.
    pub allow_ephemeral_db: bool,
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
    pub full_resync_interval: Duration,
    pub npm: NpmConfig,
    pub cloudflare: CloudflareConfig,
    pub cloudflare_api_url: Url,
}

#[derive(Debug, Clone)]
pub struct NpmConfig {
    pub url: Url,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct CloudflareConfig {
    pub api_token: String,
    pub zone_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
    #[error("environment variable '{0}' must not be empty")]
    Empty(&'static str),
    #[error("invalid boolean value for '{name}': '{value}'")]
    InvalidBool { name: &'static str, value: String },
    #[error("invalid url for '{name}': '{value}'")]
    InvalidUrl { name: &'static str, value: String },
    #[error("invalid duration for '{name}': '{value}'")]
    InvalidDuration { name: &'static str, value: String },
    #[error("DOCKTORNET_DB_PATH must be absolute, e.g. /data/docktornet.db; got '{0}'")]
    RelativeDbPath(String),
    #[error("DOCKTORNET_DB_PATH must name a file, e.g. /data/docktornet.db; got '{0}'")]
    DbPathNotAFile(String),
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let required = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));
        let required_non_empty = |k: &'static str| {
            let value = required(k)?;
            if value.is_empty() {
                Err(ConfigError::Empty(k))
            } else {
                Ok(value)
            }
        };

        let listen = required("DOCKTORNET_LISTEN")?;
        let secret = required_non_empty("DOCKTORNET_SECRET")?;
        let dry_run = parse_bool_opt(env.get("DOCKTORNET_DRY_RUN"), "DOCKTORNET_DRY_RUN", true)?;
        let adopt_existing = parse_bool_opt(
            env.get("DOCKTORNET_ADOPT_EXISTING"),
            "DOCKTORNET_ADOPT_EXISTING",
            false,
        )?;
        let allow_ephemeral_db = parse_bool_opt(
            env.get("DOCKTORNET_ALLOW_EPHEMERAL_DB"),
            "DOCKTORNET_ALLOW_EPHEMERAL_DB",
            false,
        )?;
        // A relative path resolves against the image's working directory,
        // which is the container layer: every recreate would start empty.
        let db_path = PathBuf::from(required("DOCKTORNET_DB_PATH")?);
        if !db_path.is_absolute() {
            return Err(ConfigError::RelativeDbPath(
                db_path.to_string_lossy().into_owned(),
            ));
        }
        if db_path.as_os_str().as_encoded_bytes().ends_with(b"/") || db_path.file_name().is_none() {
            return Err(ConfigError::DbPathNotAFile(
                db_path.to_string_lossy().into_owned(),
            ));
        }
        let full_resync_interval = match env.get("DOCKTORNET_FULL_RESYNC_INTERVAL") {
            Some(value) => {
                humantime::parse_duration(value).map_err(|_| ConfigError::InvalidDuration {
                    name: "DOCKTORNET_FULL_RESYNC_INTERVAL",
                    value: value.clone(),
                })?
            }
            None => Duration::from_secs(3600),
        };

        Ok(Self {
            listen,
            secret,
            db_path,
            allow_ephemeral_db,
            dry_run,
            adopt_existing,
            gatus_config_dir: PathBuf::from(required("DOCKTORNET_GATUS_CONFIG_DIR")?),
            full_resync_interval,
            npm: NpmConfig {
                url: parse_url(required("DOCKTORNET_NPM_URL")?, "DOCKTORNET_NPM_URL")?,
                user: required("DOCKTORNET_NPM_USER")?,
                password: required("DOCKTORNET_NPM_PASSWORD")?,
            },
            cloudflare: CloudflareConfig {
                api_token: required("DOCKTORNET_CF_API_TOKEN")?,
                zone_id: required("DOCKTORNET_CF_ZONE_ID")?,
            },
            cloudflare_api_url: parse_cloudflare_api_url(
                env.get("DOCKTORNET_CF_API_URL")
                    .cloned()
                    .unwrap_or_else(|| CLOUDFLARE_API_URL.to_string()),
            )?,
        })
    }
}

fn parse_bool_opt(
    value: Option<&String>,
    name: &'static str,
    default: bool,
) -> Result<bool, ConfigError> {
    match value {
        None => Ok(default),
        Some(value) => parse_bool(value, name),
    }
}

fn parse_bool(value: &str, name: &'static str) -> Result<bool, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ConfigError::InvalidBool {
            name,
            value: value.to_string(),
        }),
    }
}

fn parse_url(value: String, name: &'static str) -> Result<Url, ConfigError> {
    let url = Url::parse(&value).map_err(|_| ConfigError::InvalidUrl {
        name,
        value: value.clone(),
    })?;

    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(ConfigError::InvalidUrl { name, value }),
    }
}

/// Like [`parse_url`], but the path must be the v4 API root.  Cloudflare
/// answers 404 off that path, and the client reads a 404 on delete as "already
/// gone", so a bare host would make every DNS deletion a silent no-op.
fn parse_cloudflare_api_url(value: String) -> Result<Url, ConfigError> {
    const NAME: &str = "DOCKTORNET_CF_API_URL";
    let url = parse_url(value.clone(), NAME)?;
    if url.path().trim_end_matches('/').ends_with("/client/v4") {
        Ok(url)
    } else {
        Err(ConfigError::InvalidUrl { name: NAME, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn happy_path() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap();
        assert_eq!(c.listen, "0.0.0.0:5363");
        assert_eq!(c.db_path, PathBuf::from("/data/x.db"));
        assert!(c.dry_run);
        assert!(!c.adopt_existing);
        assert_eq!(c.gatus_config_dir, PathBuf::from("/etc/gatus/config"));
        assert_eq!(c.npm.url.as_str(), "http://npm:81/");
        assert_eq!(c.cloudflare.zone_id, "zone");
    }

    #[test]
    fn missing_var() {
        let err = ServerConfig::from_map(env(&[])).unwrap_err();
        match err {
            ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_LISTEN"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parses_integration_config_with_safe_defaults() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap();

        assert!(c.dry_run);
        assert!(!c.adopt_existing);
        assert_eq!(c.gatus_config_dir, PathBuf::from("/etc/gatus/config"));
        assert_eq!(c.npm.url.as_str(), "http://npm:81/");
        assert_eq!(c.cloudflare.zone_id, "zone");
    }

    #[test]
    fn parses_explicit_write_flags() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_DRY_RUN", "false"),
            ("DOCKTORNET_ADOPT_EXISTING", "true"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap();

        assert!(!c.dry_run);
        assert!(c.adopt_existing);
    }

    #[test]
    fn rejects_invalid_bool() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_DRY_RUN", "maybe"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::InvalidBool { name, value } => {
                assert_eq!(name, "DOCKTORNET_DRY_RUN");
                assert_eq!(value, "maybe");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_url() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "not a url"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::InvalidUrl { name, value } => {
                assert_eq!(name, "DOCKTORNET_NPM_URL");
                assert_eq!(value, "not a url");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn full_resync_interval_defaults_to_one_hour() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap();
        assert_eq!(c.full_resync_interval, Duration::from_secs(3600));
    }

    #[test]
    fn full_resync_interval_parses_humantime() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
            ("DOCKTORNET_FULL_RESYNC_INTERVAL", "15m"),
        ]))
        .unwrap();
        assert_eq!(c.full_resync_interval, Duration::from_secs(900));
    }

    #[test]
    fn rejects_invalid_full_resync_interval() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
            ("DOCKTORNET_FULL_RESYNC_INTERVAL", "soonish"),
        ]))
        .unwrap_err();
        match err {
            ConfigError::InvalidDuration { name, value } => {
                assert_eq!(name, "DOCKTORNET_FULL_RESYNC_INTERVAL");
                assert_eq!(value, "soonish");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_secret() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", ""),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::Empty(name) => assert_eq!(name, "DOCKTORNET_SECRET"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn cloudflare_api_url_defaults_to_the_public_api() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap();
        assert_eq!(
            c.cloudflare_api_url.as_str(),
            "https://api.cloudflare.com/client/v4"
        );
    }

    #[test]
    fn cloudflare_api_url_can_point_at_a_stub() {
        let c = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
            ("DOCKTORNET_CF_API_URL", "http://cf-stub:8080/client/v4"),
        ]))
        .unwrap();
        assert_eq!(
            c.cloudflare_api_url.as_str(),
            "http://cf-stub:8080/client/v4"
        );
    }

    #[test]
    fn rejects_invalid_cloudflare_api_url() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
            ("DOCKTORNET_CF_API_URL", "cf-stub:8080"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::InvalidUrl { name, value } => {
                assert_eq!(name, "DOCKTORNET_CF_API_URL");
                assert_eq!(value, "cf-stub:8080");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_cloudflare_api_url_without_the_v4_path() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
            ("DOCKTORNET_CF_API_URL", "https://api.cloudflare.com"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::InvalidUrl { name, value } => {
                assert_eq!(name, "DOCKTORNET_CF_API_URL");
                assert_eq!(value, "https://api.cloudflare.com");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_relative_db_path() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "data/docktornet.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "DOCKTORNET_DB_PATH must be absolute, e.g. /data/docktornet.db; got 'data/docktornet.db'"
        );
    }

    #[test]
    fn rejects_db_path_naming_a_directory() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "http://npm:81"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "DOCKTORNET_DB_PATH must name a file, e.g. /data/docktornet.db; got '/data/'"
        );
    }

    #[test]
    fn rejects_non_http_npm_url() {
        let err = ServerConfig::from_map(env(&[
            ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
            ("DOCKTORNET_NPM_URL", "file:///tmp/npm"),
            ("DOCKTORNET_NPM_USER", "admin@example.com"),
            ("DOCKTORNET_NPM_PASSWORD", "pw"),
            ("DOCKTORNET_CF_API_TOKEN", "token"),
            ("DOCKTORNET_CF_ZONE_ID", "zone"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::InvalidUrl { name, value } => {
                assert_eq!(name, "DOCKTORNET_NPM_URL");
                assert_eq!(value, "file:///tmp/npm");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
