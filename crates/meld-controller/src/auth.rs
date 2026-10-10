//! Authentication of the callers of the controller API.
//!
//! There are three kinds of caller, each with its own secret, so that none can
//! do what another may:
//!
//! - a client, a person using `meld`, holds the client token;
//! - a node that wants to join holds the enrollment token, shared by the
//!   nodes of the cluster, which only lets it register;
//! - a node that has registered speaks with a secret of its own, which the
//!   controller bound to its identity, and which lets it act as that node
//!   and no other.

use std::{error::Error, fmt};

use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use meld_core::{Admission, ApiErrorResponse, NodeId, SecretToken, credential_digest};
use sha2::{Digest, Sha256};

use crate::api::ControllerState;

/// A caller of the token kind, which the controller knows without looking at
/// its nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    /// A node that holds the enrollment token.
    Enrollment,
}

/// Who an authenticated request came from. Handlers read it to make sure a
/// node acts only for itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    Client,
    Enrollment,
    /// A node, proven by the secret bound to its identity.
    Node {
        id: NodeId,
        admission: Admission,
    },
}

/// How much of a node a route trusts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeAccess {
    No,
    /// Any node that holds a bound secret. The route itself answers a node
    /// that is waiting or revoked, so it can say why.
    Registered,
    Approved,
}

/// The callers a route lets in.
#[derive(Debug, Clone, Copy)]
pub struct Allowed {
    client: bool,
    enrollment: bool,
    node: NodeAccess,
}

impl Allowed {
    /// Operator actions.
    pub const CLIENT: Self = Self {
        client: true,
        enrollment: false,
        node: NodeAccess::No,
    };
    /// Joining the cluster: with the enrollment token, or as a node again.
    pub const JOIN: Self = Self {
        client: false,
        enrollment: true,
        node: NodeAccess::Registered,
    };
    /// What a node does once it has joined.
    pub const NODE: Self = Self {
        client: false,
        enrollment: false,
        node: NodeAccess::Registered,
    };
    /// Files, which clients upload and fetch and approved nodes do the reverse.
    pub const DATA: Self = Self {
        client: true,
        enrollment: false,
        node: NodeAccess::Approved,
    };

    fn permits(self, principal: Principal) -> bool {
        match principal {
            Principal::Client => self.client,
            Principal::Enrollment => self.enrollment,
            Principal::Node { admission, .. } => match self.node {
                NodeAccess::No => false,
                NodeAccess::Registered => true,
                NodeAccess::Approved => admission == Admission::Approved,
            },
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
            (_, true) => Some(Role::Enrollment),
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

/// Middleware that admits a request only if its caller is allowed on the
/// route, and tells the handler who it is. The reason is deliberately the
/// same for a missing, wrong or malformed secret.
pub async fn authenticate(
    State((authenticator, allowed, state)): State<(Authenticator, Allowed, ControllerState)>,
    mut request: Request,
    next: Next,
) -> Response {
    if !authenticator.is_enabled() {
        return next.run(request).await;
    }
    let principal =
        bearer_token(&request).and_then(|presented| match authenticator.role_of(presented) {
            Some(Role::Client) => Some(Principal::Client),
            Some(Role::Enrollment) => Some(Principal::Enrollment),
            None => state.node_for_secret(&credential_digest(presented)),
        });
    match principal {
        Some(principal) if allowed.permits(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
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
        assert_eq!(
            authenticator.role_of("node-token-000000"),
            Some(Role::Enrollment)
        );
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
    fn routes_admit_only_the_callers_they_allow() {
        let id = NodeId::generate();
        let node = |admission| Principal::Node { id, admission };

        assert!(Allowed::CLIENT.permits(Principal::Client));
        assert!(!Allowed::CLIENT.permits(Principal::Enrollment));
        assert!(!Allowed::CLIENT.permits(node(Admission::Approved)));

        // Joining: the enrollment token, or a node that holds its own secret.
        assert!(Allowed::JOIN.permits(Principal::Enrollment));
        assert!(Allowed::JOIN.permits(node(Admission::Pending)));
        assert!(!Allowed::JOIN.permits(Principal::Client));

        // After joining, the enrollment token is no longer enough.
        assert!(!Allowed::NODE.permits(Principal::Enrollment));
        assert!(!Allowed::NODE.permits(Principal::Client));
        // A waiting or revoked node gets in, to be told why it is refused.
        assert!(Allowed::NODE.permits(node(Admission::Pending)));
        assert!(Allowed::NODE.permits(node(Admission::Revoked)));

        // Data goes to approved nodes only.
        assert!(Allowed::DATA.permits(Principal::Client));
        assert!(Allowed::DATA.permits(node(Admission::Approved)));
        assert!(!Allowed::DATA.permits(node(Admission::Pending)));
        assert!(!Allowed::DATA.permits(node(Admission::Revoked)));
        assert!(!Allowed::DATA.permits(Principal::Enrollment));
    }
}
