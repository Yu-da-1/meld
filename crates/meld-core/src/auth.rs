//! Bearer tokens that authenticate clients and nodes to the controller.

use std::{env, error::Error, fmt};

/// Environment variable holding the token `meld` presents to the controller.
pub const CLIENT_TOKEN_ENV: &str = "MELD_CLIENT_TOKEN";
/// Environment variable holding the token a node presents to the controller.
pub const NODE_TOKEN_ENV: &str = "MELD_NODE_TOKEN";

/// A shared secret. It is never shown by `Debug`, and cannot be compared
/// with `==`, so a careless comparison cannot leak it through timing.
#[derive(Clone)]
pub struct SecretToken(String);

impl SecretToken {
    /// Short tokens are guessable, so they are refused.
    pub const MIN_LEN: usize = 16;

    pub fn new(value: impl Into<String>) -> Result<Self, InvalidToken> {
        let value = value.into();
        if value.len() < Self::MIN_LEN {
            return Err(InvalidToken::TooShort);
        }
        // Visible ASCII only, so the token is always a valid header value.
        if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
            return Err(InvalidToken::BadCharacter);
        }
        Ok(Self(value))
    }

    /// Reads a token from the environment. An unset or empty variable is
    /// `None`; a set but unusable one is an error rather than silently
    /// turning authentication off.
    pub fn from_env(name: &'static str) -> Result<Option<Self>, InvalidToken> {
        match env::var(name) {
            Ok(value) if value.is_empty() => Ok(None),
            Ok(value) => Self::new(value).map(Some),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => Err(InvalidToken::BadCharacter),
        }
        .map_err(|error| error.in_variable(name))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidToken {
    TooShort,
    BadCharacter,
    /// The same, reported with the variable it came from.
    InVariable {
        name: &'static str,
        reason: Box<InvalidToken>,
    },
}

impl InvalidToken {
    fn in_variable(self, name: &'static str) -> Self {
        Self::InVariable {
            name,
            reason: Box::new(self),
        }
    }
}

impl fmt::Display for InvalidToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => write!(
                formatter,
                "a token must be at least {} characters",
                SecretToken::MIN_LEN
            ),
            Self::BadCharacter => {
                formatter.write_str("a token may only contain visible ASCII characters")
            }
            Self::InVariable { name, reason } => write!(formatter, "{name}: {reason}"),
        }
    }
}

impl Error for InvalidToken {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_non_ascii_tokens_are_refused() {
        assert_eq!(
            SecretToken::new("short").unwrap_err(),
            InvalidToken::TooShort
        );
        assert_eq!(
            SecretToken::new("has a space in it!!").unwrap_err(),
            InvalidToken::BadCharacter
        );
        assert_eq!(
            SecretToken::new("ひみつのとーくんひみつのとーくん").unwrap_err(),
            InvalidToken::BadCharacter
        );
        assert!(SecretToken::new("0123456789abcdef").is_ok());
    }

    #[test]
    fn debug_output_does_not_contain_the_secret() {
        let token = SecretToken::new("0123456789abcdef").expect("valid token");
        assert!(!format!("{token:?}").contains("0123456789abcdef"));
    }
}
