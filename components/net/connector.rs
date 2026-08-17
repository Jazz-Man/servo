/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use log::warn;
use parking_lot::Mutex;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls_pki_types::CertificateDer;
use wreq::dns::{Addrs, GaiResolver, Name, Resolve, Resolving};

use crate::hosts::replace_host_ip;

pub const BUF_SIZE: usize = 32768;

#[derive(Clone, Debug)]
pub struct TlsHandshakeInfo {
    pub protocol_version: Option<String>,
    pub cipher_suite: Option<String>,
    pub kea_group_name: Option<String>,
    pub signature_scheme_name: Option<String>,
    pub alpn_protocol: Option<String>,
    pub certificate_chain_der: Vec<Vec<u8>>,
    pub used_ech: bool,
}

#[derive(Clone, Debug, Default)]
struct CertificateErrorOverrideManagerInternal {
    /// A list of certificates that should be accepted despite encountering verification
    /// errors.
    overrides: Vec<CertificateDer<'static>>,
}

/// This data structure is used to track certificate overrides.
/// It tracks:
///  - A list of [Certificate]s for which to ignore verification errors.
#[derive(Clone, Debug, Default)]
pub struct CertificateErrorOverrideManager(Arc<Mutex<CertificateErrorOverrideManagerInternal>>);

impl CertificateErrorOverrideManager {
    pub fn new() -> Self {
        Self(Default::default())
    }

    /// Add a certificate to this manager's list of certificates for which to ignore
    /// validation errors.
    ///
    /// Write-only since the wreq swap — no verifier hook consumes overrides
    /// yet; revisit when wreq exposes one.
    pub fn add_override(&self, certificate: &CertificateDer<'static>) {
        self.0.lock().overrides.push(certificate.clone());
    }
}

#[derive(Clone, Debug, Default)]
pub enum CACertificates<'de> {
    #[default]
    Default,
    Override(Vec<CertificateDer<'de>>),
}

static CRYPTO_PROVIDER_CACHE: LazyLock<Arc<CryptoProvider>> = LazyLock::new(|| {
    CryptoProvider::get_default()
        .cloned()
        // The embedder should have initialized the default crypto provider before
        // initializing servo, so this should never fail.
        .unwrap_or_else(|| {
            warn!("Default crypto provider not initialized before first access in connector.");
            Arc::new(aws_lc_rs::default_provider())
        })
});

/// Prewarm the TLS stack to speed up the first connection
///
/// Currently, this force-seeds the crypto provider (from aws_lc_rs),
/// which on my system takes around 30-50ms according to samply, spent in
/// `tree_jitter_initialize_once`. If we don't call this function, then
/// the initialization will happen much later, on a tokio runtime thread.
#[inline]
pub fn prewarm_tls() {
    #[servo_tracing::instrument]
    fn prewarm_tls_impl() {
        let mut sink = [0u8; 32];
        // The first access can be slow, if the provider needs to gather entropy.
        let _ = CRYPTO_PROVIDER_CACHE.secure_random.fill(&mut sink);
    }

    if let Err(error) = std::thread::Builder::new()
        .name("Net-TLS-prewarm".into())
        .spawn(prewarm_tls_impl)
    {
        warn!("Failed to spawn thread to prewarm TLS: {error:?}");
    }
}

pub type BoxedBody = BoxBody<Bytes, wreq::Error>;

/// A wreq DNS resolver that applies servo's host table before falling back
/// to the system resolver: hostnames listed in the hosts file / host table
/// connect to their overridden IP, while the Host header, TLS SNI, and the
/// request URL keep the original hostname.
///
/// The port in a returned address is 0: wreq substitutes the port from the
/// request URI (or the scheme's conventional port) — see the [`Resolve`]
/// trait docs (vendors/wreq/src/dns/resolve.rs).
#[derive(Clone, Default)]
pub struct ServoDnsResolver {
    fallback: GaiResolver,
}

impl ServoDnsResolver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Resolve for ServoDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        match replace_host_ip(name.as_str()) {
            Some(ip) => {
                let addrs: Addrs = Box::new(std::iter::once(SocketAddr::new(ip, 0)));
                Box::pin(std::future::ready(Ok(addrs)))
            },
            None => self.fallback.resolve(name),
        }
    }
}
