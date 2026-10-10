//! Encrypted transport for a Meld cluster on a trusted LAN.
//!
//! The controller makes itself a self-signed certificate the first time it
//! starts, and prints its fingerprint. Clients and nodes are given that
//! fingerprint and accept the controller only if its certificate matches. No
//! certificate authority is involved; the fingerprint is the trust, as with
//! a known SSH host key.

use std::{
    env, fmt, fs,
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use meld_core::Sha256Digest;
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as TlsError, ServerConfig, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, ring},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
};
use sha2::{Digest, Sha256};
pub use tokio_rustls::{TlsAcceptor, server::TlsStream};

/// Environment variable naming the fingerprint to accept.
pub const FINGERPRINT_ENV: &str = "MELD_CONTROLLER_FINGERPRINT";

const CERTIFICATE_FILE: &str = "tls-cert.pem";
const KEY_FILE: &str = "tls-key.pem";
const FINGERPRINT_FILE: &str = "tls-fingerprint";

/// The sha256 of a certificate, as 64 lowercase hex digits.
pub fn fingerprint_of(certificate: &CertificateDer<'_>) -> Sha256Digest {
    Sha256Digest::from_bytes(Sha256::digest(certificate.as_ref()).into())
}

/// Whether `url` would send secrets across the network unencrypted: plain
/// `http` to somewhere other than this machine.
pub fn is_cleartext_beyond_loopback(url: &str) -> bool {
    let Some(rest) = url.trim().strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let authority = authority.rsplit('@').next().unwrap_or_default();
    let host = match authority.strip_prefix('[') {
        // [::1]:3000
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => authority.split(':').next().unwrap_or_default(),
    };
    !(host.eq_ignore_ascii_case("localhost") || host == "::1" || host.starts_with("127."))
}

/// A fingerprint a client was told to accept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint(Sha256Digest);

impl Fingerprint {
    /// Accepts the hex form, with or without `:` separators and in any case,
    /// so a value copied from other tools works.
    pub fn parse(value: &str) -> Result<Self, InvalidFingerprint> {
        let cleaned: String = value
            .trim()
            .chars()
            .filter(|character| *character != ':')
            .map(|character| character.to_ascii_lowercase())
            .collect();
        cleaned
            .parse::<Sha256Digest>()
            .map(Self)
            .map_err(|_| InvalidFingerprint)
    }

    /// Reads [`FINGERPRINT_ENV`]. Unset or empty is `None`; a value that is
    /// not a fingerprint is an error, not a silent fall back to no pinning.
    pub fn from_env() -> Result<Option<Self>, InvalidFingerprint> {
        match env::var(FINGERPRINT_ENV) {
            Ok(value) if value.trim().is_empty() => Ok(None),
            Ok(value) => Self::parse(&value).map(Some),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => Err(InvalidFingerprint),
        }
    }

    pub fn digest(&self) -> &Sha256Digest {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidFingerprint;

impl fmt::Display for InvalidFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{FINGERPRINT_ENV} must be the 64 hex digits of the controller certificate's sha256"
        )
    }
}

impl std::error::Error for InvalidFingerprint {}

/// Accepts exactly one certificate, whatever its name or dates say.
///
/// The handshake signatures are still checked, so a peer must also hold the
/// certificate's private key, not just replay the certificate.
#[derive(Debug)]
struct PinnedCertificate {
    fingerprint: Sha256Digest,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        if fingerprint_of(end_entity) == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::General(
                "the controller's certificate does not match the pinned fingerprint".to_owned(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A client configuration that accepts only the certificate with `fingerprint`.
pub fn pinned_client_config(fingerprint: &Fingerprint) -> ClientConfig {
    let provider = Arc::new(ring::default_provider());
    ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports the default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertificate {
            fingerprint: fingerprint.0.clone(),
            provider,
        }))
        .with_no_client_auth()
}

/// The controller's certificate and key.
pub struct ServerIdentity {
    acceptor: TlsAcceptor,
    fingerprint: Sha256Digest,
}

impl ServerIdentity {
    /// Loads the certificate and key in `directory`, or makes them on first
    /// start. A pair put there by the operator is used as it is.
    ///
    /// The fingerprint is also written to a file there, for people and
    /// scripts to read.
    pub fn load_or_create(directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let certificate_path = directory.join(CERTIFICATE_FILE);
        let key_path = directory.join(KEY_FILE);
        match (certificate_path.exists(), key_path.exists()) {
            (true, true) => {}
            (false, false) => create(&certificate_path, &key_path)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} and {} must exist together; remove the one that is left to make a new pair",
                        certificate_path.display(),
                        key_path.display()
                    ),
                ));
            }
        }

        let certificate = CertificateDer::from_pem_file(&certificate_path)
            .map_err(|error| invalid(&certificate_path, error))?;
        let key =
            PrivateKeyDer::from_pem_file(&key_path).map_err(|error| invalid(&key_path, error))?;
        let fingerprint = fingerprint_of(&certificate);

        let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .map_err(|error| invalid(&key_path, error))?;

        // Rewritten each start, so it can never disagree with the certificate.
        fs::write(directory.join(FINGERPRINT_FILE), format!("{fingerprint}\n"))?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            fingerprint,
        })
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        self.acceptor.clone()
    }

    pub fn fingerprint(&self) -> &Sha256Digest {
        &self.fingerprint
    }
}

fn invalid(path: &Path, error: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{} is not usable: {error}", path.display()),
    )
}

fn create(certificate_path: &Path, key_path: &Path) -> io::Result<()> {
    let generated = rcgen::generate_simple_self_signed(vec!["meld-controller".to_owned()])
        .map_err(|error| io::Error::other(format!("could not make a certificate: {error}")))?;

    // The key first, readable by its owner only from the moment it exists.
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(key_path)?
        .write_all(generated.signing_key.serialize_pem().as_bytes())?;
    fs::write(certificate_path, generated.cert.pem())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use tokio_rustls::TlsConnector;

    use super::*;

    #[test]
    fn only_plain_http_to_another_machine_is_cleartext_beyond_loopback() {
        for url in [
            "http://192.168.1.10:3000",
            "http://controller.local:3000/",
            "http://user@controller:3000",
        ] {
            assert!(is_cleartext_beyond_loopback(url), "{url}");
        }
        for url in [
            "https://192.168.1.10:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://[::1]:3000",
            "http://LOCALHOST",
        ] {
            assert!(!is_cleartext_beyond_loopback(url), "{url}");
        }
    }

    #[test]
    fn the_certificate_is_made_once_and_kept() {
        let directory = tempdir().expect("temp dir");

        let first = ServerIdentity::load_or_create(directory.path()).expect("created");
        let again = ServerIdentity::load_or_create(directory.path()).expect("reloaded");

        assert_eq!(first.fingerprint(), again.fingerprint());
        let written = fs::read_to_string(directory.path().join(FINGERPRINT_FILE)).unwrap();
        assert_eq!(written.trim(), first.fingerprint().as_str());
    }

    #[cfg(unix)]
    #[test]
    fn the_private_key_is_readable_by_its_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempdir().expect("temp dir");
        ServerIdentity::load_or_create(directory.path()).expect("created");

        let mode = fs::metadata(directory.path().join(KEY_FILE))
            .unwrap()
            .permissions()
            .mode();

        assert_eq!(mode & 0o077, 0);
    }

    #[test]
    fn half_a_pair_is_an_error_and_is_not_replaced() {
        let directory = tempdir().expect("temp dir");
        fs::write(directory.path().join(KEY_FILE), "left over").unwrap();

        let error = ServerIdentity::load_or_create(directory.path())
            .err()
            .expect("half a pair must not be replaced silently");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            fs::read_to_string(directory.path().join(KEY_FILE)).unwrap(),
            "left over"
        );
    }

    #[test]
    fn a_fingerprint_is_read_in_any_common_spelling() {
        let digest = "ab".repeat(32);
        let colons = (0..32).map(|_| "AB").collect::<Vec<_>>().join(":");

        assert_eq!(
            Fingerprint::parse(&digest).unwrap(),
            Fingerprint::parse(&colons).unwrap()
        );
        assert!(Fingerprint::parse("abc").is_err());
        assert!(Fingerprint::parse(&"g".repeat(64)).is_err());
    }

    /// Serves one hello over TLS and returns what the client got, or why not.
    async fn exchange(identity: &ServerIdentity, pinned: &Fingerprint) -> Result<String, String> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = identity.acceptor();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            if let Ok(mut stream) = acceptor.accept(stream).await {
                let _ = stream.write_all(b"hello").await;
                let _ = stream.shutdown().await;
            }
        });

        let connector = TlsConnector::from(Arc::new(pinned_client_config(pinned)));
        let stream = TcpStream::connect(address).await.unwrap();
        // The name is not what is checked, so any will do.
        let name = ServerName::try_from("anything").unwrap();
        let mut stream = connector
            .connect(name, stream)
            .await
            .map_err(|error| error.to_string())?;
        let mut received = String::new();
        stream
            .read_to_string(&mut received)
            .await
            .map_err(|error| error.to_string())?;
        Ok(received)
    }

    #[tokio::test]
    async fn a_client_talks_to_the_controller_whose_fingerprint_it_pinned() {
        let directory = tempdir().expect("temp dir");
        let identity = ServerIdentity::load_or_create(directory.path()).expect("created");
        let pinned = Fingerprint::parse(identity.fingerprint().as_str()).unwrap();

        assert_eq!(exchange(&identity, &pinned).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn a_client_refuses_a_certificate_with_another_fingerprint() {
        let (real, impostor) = (tempdir().unwrap(), tempdir().unwrap());
        let real = ServerIdentity::load_or_create(real.path()).expect("created");
        let impostor = ServerIdentity::load_or_create(impostor.path()).expect("created");
        let pinned = Fingerprint::parse(real.fingerprint().as_str()).unwrap();

        let error = exchange(&impostor, &pinned).await.unwrap_err();

        assert!(error.contains("pinned fingerprint"), "{error}");
    }
}
