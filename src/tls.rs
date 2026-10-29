use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, RootCertStore};

#[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
compile_error!("enable the `aws-lc-rs` or the `ring` feature of feishu-ws");

/// The TLS settings of the endpoint request and the WebSocket: the process's default
/// crypto provider when one is installed, else the one this crate's features pick. It is
/// named explicitly, so a dependency graph that enables both of rustls's providers cannot
/// leave rustls unable to choose.
pub fn config() -> Result<Arc<ClientConfig>, rustls::Error> {
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(provider()));
    let roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    Ok(Arc::new(
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

#[cfg(feature = "aws-lc-rs")]
fn provider() -> CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
fn provider() -> CryptoProvider {
    rustls::crypto::ring::default_provider()
}
