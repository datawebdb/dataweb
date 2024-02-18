use std::io::{self, BufReader};

use crate::error::{MeshError, Result};
use rustls::Certificate;
use rustls_pemfile::certs;
use sha2::{Digest, Sha256};
use tracing::debug;
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

/// Extracts the first certificate from a urlencoded in memory str representation of a PEM file.
/// Calls [parse_certificate] to obtain desired information from the cert. All other certificates
/// in the str are ignored.
pub fn parse_urlencoded_pemstr(pemstr: &str) -> Result<(String, String, String)> {
    debug!("Got urlencoded pemstr {pemstr}");
    let decoded = urlencoding::decode(pemstr).map_err(|_e| {
        MeshError::SerDe("Unable to url decode client cert from header".to_string())
    })?;
    debug!("Got pemstr decoded: {decoded}");
    let rustls_cert = rustls::Certificate(
        certs(&mut BufReader::new(decoded.as_bytes()))
            .map_err(|e| {
                MeshError::SerDe(format!(
                    "Unable to extract der encoded certs from header with error {e}"
                ))
            })?
            .remove(0),
    );
    parse_certificate(&rustls_cert).map_err(|e| {
        MeshError::SerDe(format!(
            "Found client cert in urlencoded pemstr, but unable to parse with error: {e}"
        ))
    })
}
