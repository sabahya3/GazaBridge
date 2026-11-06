//! TLS configuration with libpq-compatible SSL modes.
//!
//! | mode | encryption | certificate chain | host name |
//! |---|---|---|---|
//! | `disable` | no | – | – |
//! | `prefer` | if the server offers it | not verified unless a CA is configured | no |
//! | `require` | yes | not verified unless a CA is configured | no |
//! | `verify-ca` | yes | verified | no |
//! | `verify-full` (default) | yes | verified | verified |
//!
//! Handshake signatures are always verified, so even unverified modes
//! require the server to hold the certificate's private key.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use gazabridge_domain::connection::{PgSslMode, TlsSettings};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio_postgres_rustls::MakeRustlsConnect;

fn tls_error(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ErrorKind::InvalidInput, msg)
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_certs(path: &Path) -> ConnectorResult<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| tls_error(format!("cannot open certificate {}: {e}", path.display())))?;
    let certs = CertificateDer::pem_reader_iter(BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_error(format!("invalid PEM in {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(tls_error(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> ConnectorResult<PrivateKeyDer<'static>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| tls_error(format!("cannot open key {}: {e}", path.display())))?;
    if text.contains("ENCRYPTED") {
        return Err(tls_error(
            "encrypted client keys are not supported yet; provide an unencrypted PKCS#8 or PKCS#1 key with restrictive file permissions",
        ));
    }
    PrivateKeyDer::from_pem_slice(text.as_bytes()).map_err(|e| match e {
        pem::Error::NoItemsFound => {
            tls_error(format!("no private key found in {}", path.display()))
        }
        e => tls_error(format!("invalid key PEM: {e}")),
    })
}

fn roots(tls: &TlsSettings) -> ConnectorResult<RootCertStore> {
    let mut store = RootCertStore::empty();
    match &tls.ca_certificate {
        Some(ca) => {
            for cert in load_certs(ca)? {
                store
                    .add(cert)
                    .map_err(|e| tls_error(format!("invalid CA certificate: {e}")))?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                // Unparseable platform certificates are skipped, as libpq does.
                let _ = store.add(cert);
            }
            if store.is_empty() {
                return Err(tls_error(
                    "no trusted root certificates are available; configure a CA certificate",
                ));
            }
        }
    }
    Ok(store)
}

/// Verifies handshake signatures but not the certificate (encryption only).
#[derive(Debug)]
struct EncryptOnly(Arc<CryptoProvider>);

impl ServerCertVerifier for EncryptOnly {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Verifies the chain against trusted roots but accepts any host name.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// Builds the TLS connector and the driver-level SSL mode.
pub fn connector(
    mode: PgSslMode,
    tls: &TlsSettings,
) -> ConnectorResult<(MakeRustlsConnect, tokio_postgres::config::SslMode)> {
    use tokio_postgres::config::SslMode;
    let provider = provider();
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_error(e.to_string()))?;

    let verifier: Arc<dyn ServerCertVerifier> = match mode {
        PgSslMode::Disable => Arc::new(EncryptOnly(provider.clone())),
        PgSslMode::Prefer | PgSslMode::Require if tls.ca_certificate.is_none() => {
            Arc::new(EncryptOnly(provider.clone()))
        }
        PgSslMode::Prefer | PgSslMode::Require | PgSslMode::VerifyCa => {
            let webpki = WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots(tls)?),
                provider.clone(),
            )
            .build()
            .map_err(|e| tls_error(e.to_string()))?;
            Arc::new(ChainOnly(webpki))
        }
        PgSslMode::VerifyFull => {
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots(tls)?), provider.clone())
                .build()
                .map_err(|e| tls_error(e.to_string()))?
        }
    };
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let config = match (&tls.client_certificate, &tls.client_key) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(load_certs(cert)?, load_key(key)?)
            .map_err(|e| tls_error(format!("invalid client certificate or key: {e}")))?,
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(tls_error(
                "a client certificate and its key must be configured together",
            ));
        }
    };
    let ssl_mode = match mode {
        PgSslMode::Disable => SslMode::Disable,
        PgSslMode::Prefer => SslMode::Prefer,
        _ => SslMode::Require,
    };
    Ok((MakeRustlsConnect::new(config), ssl_mode))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_build_and_validate_inputs() {
        let tls = TlsSettings::default();
        let (_, m) = connector(PgSslMode::Disable, &tls).unwrap();
        assert!(matches!(m, tokio_postgres::config::SslMode::Disable));
        let (_, m) = connector(PgSslMode::Require, &tls).unwrap();
        assert!(matches!(m, tokio_postgres::config::SslMode::Require));

        let half = TlsSettings {
            client_certificate: Some("/nonexistent/cert.pem".into()),
            ..Default::default()
        };
        assert!(connector(PgSslMode::Require, &half).is_err());
        let missing_ca = TlsSettings {
            ca_certificate: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };
        assert!(connector(PgSslMode::VerifyFull, &missing_ca).is_err());
    }
}
