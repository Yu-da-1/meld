//! Bearer tokens that authenticate clients and nodes to the controller.

use std::{env, error::Error, fmt};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::Sha256Digest;

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

/// A secret a node makes for itself and keeps. Only its digest is ever
/// given to the controller, which binds it to the node's identity: from then
/// on, only the holder of the secret can speak as that node.
#[derive(Clone)]
pub struct NodeSecret(String);

impl NodeSecret {
    const HEX_LEN: usize = 64;

    /// 244 random bits.
    pub fn generate() -> Self {
        Self(format!(
            "{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    }

    pub fn parse(value: &str) -> Result<Self, InvalidToken> {
        let valid = value.len() == Self::HEX_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            Err(InvalidToken::BadCharacter)
        }
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn digest(&self) -> Sha256Digest {
        credential_digest(&self.0)
    }
}

impl fmt::Debug for NodeSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NodeSecret(<redacted>)")
    }
}

/// The digest the controller keeps of whatever a caller presents as a secret.
pub fn credential_digest(presented: &str) -> Sha256Digest {
    Sha256Digest::from_bytes(Sha256::digest(presented.as_bytes()).into())
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
    fn a_node_secret_is_random_parses_back_and_is_never_shown() {
        let secret = NodeSecret::generate();
        assert_ne!(secret.expose(), NodeSecret::generate().expose());
        assert_eq!(
            NodeSecret::parse(secret.expose())
                .expect("generated secret parses")
                .digest(),
            secret.digest()
        );
        assert!(NodeSecret::parse("short").is_err());
        assert!(NodeSecret::parse(&"G".repeat(64)).is_err());
        assert!(!format!("{secret:?}").contains(secret.expose()));
    }

    #[test]
    fn debug_output_does_not_contain_the_secret() {
        let token = SecretToken::new("0123456789abcdef").expect("valid token");
        assert!(!format!("{token:?}").contains("0123456789abcdef"));
    }
}
