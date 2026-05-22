use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerConfig {
    pub listen: String, // e.g. "0.0.0.0:5363"
    pub secret: String,
    pub db_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));
        Ok(Self {
            listen: get("DOCKTORNET_LISTEN")?,
            secret: get("DOCKTORNET_SECRET")?,
            db_path: PathBuf::from(get("DOCKTORNET_DB_PATH")?),
        })
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
        ]))
        .unwrap();
        assert_eq!(c.listen, "0.0.0.0:5363");
        assert_eq!(c.db_path, PathBuf::from("/data/x.db"));
    }

    #[test]
    fn missing_var() {
        let err = ServerConfig::from_map(env(&[])).unwrap_err();
        match err {
            ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_LISTEN"),
        }
    }
}
