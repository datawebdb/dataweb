use std::io::{self};

use crate::error::{MeshError, Result};
use rustls::Certificate;
use rustls_pemfile::certs;
use sha2::{Digest, Sha256};
use x509_parser::{certificate::X509Certificate, oid_registry::asn1_rs::FromDer};

/// Computes sha256 fingerprint of a [Certificate] and extracts the
/// subject and issuer distinguished names.
pub fn parse_certificate(cert: &Certificate) -> Result<(String, String, String)> {
    let mut hasher = Sha256::new();
    hasher.update(&cert.0);
    let client_cert_fingerprint = format!("{:X}", hasher.finalize());

    let (_, parsed_cert) = X509Certificate::from_der(&cert.0)
        .map_err(|_e| MeshError::SerDe("Unable to parse certificate!".to_string()))?;

    let subject_dn = parsed_cert.subject().to_string();
    let issuer_dn = parsed_cert.issuer().to_string();

    Ok((client_cert_fingerprint, subject_dn, issuer_dn))
}

/// Extracts a Vec of all contained [Certificate]s in reader
pub fn load_certificate_from_reader(reader: &mut dyn io::BufRead) -> Result<Vec<Certificate>> {
    Ok(certs(reader)?.into_iter().map(Certificate).collect())
}
