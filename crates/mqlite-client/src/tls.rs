//! TLS connection handling using rustls.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::config::TlsConfig;
use crate::error::{ClientError, Result};

/// A TLS-wrapped TCP stream.
///
/// This provides a synchronous TLS stream for blocking I/O use cases.
/// For non-blocking I/O with mio, use ClientStream in the client module.
#[allow(dead_code)]
pub struct TlsStream {
    inner: StreamOwned<ClientConnection, TcpStream>,
}

#[allow(dead_code)]
impl TlsStream {
    /// Create a new TLS connection wrapping the given TCP stream.
    pub fn new(tcp_stream: TcpStream, config: &TlsConfig, hostname: &str) -> Result<Self> {
        let tls_config = build_client_config(config)?;

        let server_name = config
            .server_name
            .as_deref()
            .unwrap_or(hostname);

        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|_| ClientError::Tls(format!("Invalid server name: {}", server_name)))?;

        let conn = ClientConnection::new(Arc::new(tls_config), server_name)
            .map_err(|e| ClientError::Tls(e.to_string()))?;

        let stream = StreamOwned::new(conn, tcp_stream);

        Ok(Self { inner: stream })
    }

    /// Get a reference to the underlying TCP stream.
    pub fn get_ref(&self) -> &TcpStream {
        self.inner.get_ref()
    }

    /// Get a mutable reference to the underlying TCP stream.
    pub fn get_mut(&mut self) -> &mut TcpStream {
        self.inner.get_mut()
    }

    /// Check if the TLS handshake is complete.
    pub fn is_handshaking(&self) -> bool {
        self.inner.conn.is_handshaking()
    }
}

impl Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Build a rustls ClientConfig from our TlsConfig.
pub fn build_client_config(config: &TlsConfig) -> Result<ClientConfig> {
    // Handle insecure mode (accept any certificate)
    if config.accept_invalid_certs {
        return build_insecure_config();
    }

    let mut root_store = RootCertStore::empty();

    // Load custom CA certificate if provided
    if let Some(ca_path) = &config.ca_cert {
        let file = File::open(ca_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open CA cert: {}", e)))?;
        let mut reader = BufReader::new(file);

        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClientError::Tls(format!("Failed to parse CA cert: {}", e)))?;

        for cert in certs {
            root_store.add(cert)
                .map_err(|e| ClientError::Tls(format!("Failed to add CA cert: {}", e)))?;
        }
    } else {
        // Use system root certificates
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder()
        .with_root_certificates(root_store);

    // Load client certificate for mutual TLS if provided
    let tls_config = if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
        let cert_file = File::open(cert_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open client cert: {}", e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClientError::Tls(format!("Failed to parse client cert: {}", e)))?;

        let key_file = File::open(key_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open client key: {}", e)))?;
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| ClientError::Tls(format!("Failed to parse client key: {}", e)))?
            .ok_or_else(|| ClientError::Tls("No private key found in file".to_string()))?;

        builder.with_client_auth_cert(certs, key)
            .map_err(|e| ClientError::Tls(format!("Failed to configure client auth: {}", e)))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(tls_config)
}

/// Danger: A certificate verifier that accepts any certificate.
/// Only use for testing with self-signed certificates.
#[cfg(feature = "tls")]
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub struct NoCertificateVerification;

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ]
        }
    }
}

/// Build an insecure TLS config that accepts any certificate.
#[cfg(feature = "tls")]
pub fn build_insecure_config() -> Result<ClientConfig> {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification))
        .with_no_client_auth();
    Ok(config)
}
