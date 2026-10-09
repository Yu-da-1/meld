//! Authentication of the callers of the controller API.
//!
//! Every route accepts one or both of two roles. A `Client` is a person using
//! `meld`; a `Node` is a `meld-node` process. Each role has its own token, so
//! a node cannot submit jobs and a client cannot pose as a node.

use std::{error::Error, fmt};

use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use meld_core::{ApiErrorResponse, SecretToken};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Node,
}

/// The roles a route lets in.
#[derive(Debug, Clone, Copy)]
pub struct Allowed {
    client: bool,
    node: bool,
}

impl Allowed {
    pub const CLIENT: Self = Self {
        client: true,
        node: false,
    };
    pub const NODE: Self = Self {
        client: false,
        node: true,
    };
    pub const EITHER: Self = Self {
        client: true,
        node: true,
    };

    fn permits(self, role: Role) -> bool {
        match role {
            Role::Client => self.client,
            Role::Node => self.node,
        }
    }
}

type Digest32 = [u8; 32];

/// The configured tokens, kept only as hashes. Hashing first makes the
/// comparison independent of where the tokens differ and of their length.
#[derive(Debug, Clone)]
pub struct Authenticator {
    tokens: Option<Tokens>,
}

#[derive(Debug, Clone)]
struct Tokens {
    client: Digest32,
    node: Digest32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AuthConfigError {
    /// Authentication needs both tokens; one alone would leave a role open.
    MissingToken(&'static str),
    /// A shared token would make the two roles indistinguishable.
    IdenticalTokens,
}

impl fmt::Display for AuthConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingToken(name) => write!(
                formatter,
                "{name} is not set; authentication needs both a client and a node token"
            ),
            Self::IdenticalTokens => {
                formatter.write_str("the client token and the node token must differ")
            }
        }
    }
}

impl Error for AuthConfigError {}

impl Authenticator {
    /// Lets every request through. Only for a controller reachable from this
    /// machine alone, and for tests.
    pub fn disabled() -> Self {
        Self { tokens: None }
    }

    /// Authentication is on when either token is given, and then needs both.
    pub fn new(
        client: Option<SecretToken>,
        node: Option<SecretToken>,
        client_name: &'static str,
        node_name: &'static str,
    ) -> Result<Self, AuthConfigError> {
        match (client, node) {
            (None, None) => Ok(Self::disabled()),
            (Some(_), None) => Err(AuthConfigError::MissingToken(node_name)),
            (None, Some(_)) => Err(AuthConfigError::MissingToken(client_name)),
            (Some(client), Some(node)) => Self::enabled(&client, &node),
        }
    }

    pub fn enabled(client: &SecretToken, node: &SecretToken) -> Result<Self, AuthConfigError> {
        let client = hash(client.expose());
        let node = hash(node.expose());
        if equal(&client, &node) {
            return Err(AuthConfigError::IdenticalTokens);
        }
        Ok(Self {
            tokens: Some(Tokens { client, node }),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.tokens.is_some()
    }

    /// The role a presented token belongs to, if any.
    fn role_of(&self, presented: &str) -> Option<Role> {
        let tokens = self.tokens.as_ref()?;
        let presented = hash(presented);
        // Both comparisons always run.
        let is_client = equal(&presented, &tokens.client);
        let is_node = equal(&presented, &tokens.node);
        match (is_client, is_node) {
            (true, _) => Some(Role::Client),
            (_, true) => Some(Role::Node),
            _ => None,
        }
    }
}

fn hash(value: &str) -> Digest32 {
    Sha256::digest(value.as_bytes()).into()
}

fn equal(left: &Digest32, right: &Digest32) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

/// Extracts the token of an `Authorization: Bearer <token>` header.
fn bearer_token(request: &Request) -> Option<&str> {
    let value = request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

/// Middleware that admits a request only if its token belongs to an allowed
/// role. The reason is deliberately the same for a missing, wrong or
/// malformed token.
pub async fn authenticate(
    State((authenticator, allowed)): State<(Authenticator, Allowed)>,
    request: Request,
    next: Next,
) -> Response {
    if !authenticator.is_enabled() {
        return next.run(request).await;
    }
    match bearer_token(&request).and_then(|token| authenticator.role_of(token)) {
        Some(role) if allowed.permits(role) => next.run(request).await,
        Some(_) => rejection(
            StatusCode::FORBIDDEN,
            "this token is not allowed to use this endpoint",
        ),
        None => {
            let mut response = rejection(
                StatusCode::UNAUTHORIZED,
                "missing or invalid authentication token",
            );
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            response
        }
    }
}

fn rejection(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ApiErrorResponse {
            error: message.to_owned(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(value: &str) -> SecretToken {
        SecretToken::new(value).expect("valid token")
    }

    fn authenticator() -> Authenticator {
        Authenticator::enabled(&token("client-token-0000"), &token("node-token-000000"))
            .expect("distinct tokens")
    }

    #[test]
    fn each_token_identifies_its_own_role() {
        let authenticator = authenticator();
        assert_eq!(
            authenticator.role_of("client-token-0000"),
            Some(Role::Client)
        );
        assert_eq!(authenticator.role_of("node-token-000000"), Some(Role::Node));
        assert_eq!(authenticator.role_of("client-token-0001"), None);
        assert_eq!(authenticator.role_of(""), None);
    }

    #[test]
    fn authentication_needs_both_tokens_and_they_must_differ() {
        assert!(
            !Authenticator::new(None, None, "C", "N")
                .unwrap()
                .is_enabled()
        );
        assert_eq!(
            Authenticator::new(Some(token("client-token-0000")), None, "C", "N").unwrap_err(),
            AuthConfigError::MissingToken("N")
        );
        assert_eq!(
            Authenticator::new(None, Some(token("node-token-000000")), "C", "N").unwrap_err(),
            AuthConfigError::MissingToken("C")
        );
        assert_eq!(
            Authenticator::enabled(&token("same-token-000000"), &token("same-token-000000"))
                .unwrap_err(),
            AuthConfigError::IdenticalTokens
        );
    }

    #[test]
    fn routes_admit_only_the_roles_they_allow() {
        assert!(Allowed::CLIENT.permits(Role::Client));
        assert!(!Allowed::CLIENT.permits(Role::Node));
        assert!(Allowed::NODE.permits(Role::Node));
        assert!(!Allowed::NODE.permits(Role::Client));
        assert!(Allowed::EITHER.permits(Role::Client) && Allowed::EITHER.permits(Role::Node));
    }
}
