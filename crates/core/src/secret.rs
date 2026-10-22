use std::fmt;

/// The shortest `DOCKTORNET_SECRET` the server and agent accept.
pub const MIN_SHARED_SECRET_LEN: usize = 32;

/// A credential read from config. `Debug` prints a placeholder, so a config
/// struct that holds one can be logged; code that needs the value calls
/// [`Secret::expose`].
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

/// Why `DOCKTORNET_SECRET` was refused. The message never contains the value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WeakSecret {
    #[error(
        "DOCKTORNET_SECRET must be at least {MIN_SHARED_SECRET_LEN} characters, got {0}; generate one with `openssl rand -hex 32`"
    )]
    TooShort(usize),
    #[error(
        "DOCKTORNET_SECRET still holds the 'changeme' placeholder; generate one with `openssl rand -hex 32`"
    )]
    Placeholder,
}

/// Checks the bearer secret the server and its agents share. Both binaries
/// call it while loading config, so a weak secret stops startup.
pub fn validate_shared_secret(value: &str) -> Result<(), WeakSecret> {
    if value.to_lowercase().contains("changeme") {
        return Err(WeakSecret::Placeholder);
    }
    let len = value.chars().count();
    if len < MIN_SHARED_SECRET_LEN {
        return Err(WeakSecret::TooShort(len));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_64_character_hex_secret() {
        let secret = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        assert_eq!(validate_shared_secret(secret), Ok(()));
    }

    #[test]
    fn rejects_short_and_placeholder_secrets() {
        let changeme_40 = format!("{}CHANGEME{}", "a".repeat(16), "b".repeat(16));
        assert_eq!(validate_shared_secret(""), Err(WeakSecret::TooShort(0)));
        assert_eq!(validate_shared_secret("s"), Err(WeakSecret::TooShort(1)));
        assert_eq!(
            validate_shared_secret("changeme"),
            Err(WeakSecret::Placeholder)
        );
        assert_eq!(
            validate_shared_secret("changeme-please"),
            Err(WeakSecret::Placeholder)
        );
        assert_eq!(
            validate_shared_secret(&changeme_40),
            Err(WeakSecret::Placeholder)
        );
    }

    #[test]
    fn debug_hides_the_value() {
        let printed = format!("{:?}", Secret::from("hunter2-hunter2"));
        assert!(!printed.contains("hunter2"), "{printed}");
    }
}
