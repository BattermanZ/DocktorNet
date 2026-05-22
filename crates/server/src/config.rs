use std::collections::HashMap;
use std::path::PathBuf;

use url::Url;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerConfig {
    pub listen: String,
    pub secret: String,
    pub db_path: PathBuf,
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
    pub npm: NpmConfig,
    pub cloudflare: CloudflareConfig,
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
    #[error("invalid boolean value for '{name}': '{value}'")]
    InvalidBool { name: &'static str, value: String },
    #[error("invalid url for '{name}': '{value}'")]
    InvalidUrl { name: &'static str, value: String },
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let required = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));

        let dry_run = parse_bool_opt(env.get("DOCKTORNET_DRY_RUN"), "DOCKTORNET_DRY_RUN", true)?;
        let adopt_existing = parse_bool_opt(
            env.get("DOCKTORNET_ADOPT_EXISTING"),
            "DOCKTORNET_ADOPT_EXISTING",
            false,
        )?;

        Ok(Self {
            listen: required("DOCKTORNET_LISTEN")?,
            secret: required("DOCKTORNET_SECRET")?,
            db_path: PathBuf::from(required("DOCKTORNET_DB_PATH")?),
            dry_run,
            adopt_existing,
            gatus_config_dir: PathBuf::from(required("DOCKTORNET_GATUS_CONFIG_DIR")?),
            npm: NpmConfig {
                url: parse_url(required("DOCKTORNET_NPM_URL")?, "DOCKTORNET_NPM_URL")?,
                user: required("DOCKTORNET_NPM_USER")?,
                password: required("DOCKTORNET_NPM_PASSWORD")?,
            },
            cloudflare: CloudflareConfig {
                api_token: required("DOCKTORNET_CF_API_TOKEN")?,
                zone_id: required("DOCKTORNET_CF_ZONE_ID")?,
            },
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
    Url::parse(&value).map_err(|_| ConfigError::InvalidUrl { name, value })
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
}
