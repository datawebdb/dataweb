use actix_web::HttpRequest;

use mesh::pki::{parse_certificate, parse_urlencoded_pemstr};
use rustls::Certificate;

use crate::error::{RelayError, Result};

/// Extracts client certificates from [HttpRequest], see [parse_certificate] for more information
/// on the return values.
pub(crate) fn parse_certs_direct_tls(req: HttpRequest) -> Result<(String, String, String)> {
    let client_cert = req.conn_data::<Certificate>().ok_or(RelayError {
        msg: "Got query request with no client cert!".into(),
    })?;

    let (fingerprint, subject_dn, issuer_dn) = parse_certificate(client_cert)?;
    Ok((fingerprint, subject_dn, issuer_dn))
}

/// Extracts client certificates from [HttpRequest], see [parse_certificate] for more information
/// on the return values.
pub(crate) fn parse_certs_from_header(
    req: HttpRequest,
    client_cert_header: &String,
) -> Result<(String, String, String)> {
    let cert = req
        .headers()
        .get(client_cert_header)
        .ok_or(RelayError::new(
            "Specified client_cert_header is empty. Unable to authenticate client.",
        ))?;
    let inner = cert.to_str().map_err(|_e| {
        RelayError::new("Invalid client_cert_header value. Unable to authenticate client.")
    })?;
    parse_urlencoded_pemstr(inner).map_err(|e| RelayError::new(&e.to_string()))
}

/// Extracts client certificates from [HttpRequest]. May extract certificate from passed header or direct TLS depending
/// on the value of client_cert_header.
///
/// See [parse_certificate] for more information on the return values.
pub(crate) fn parse_certs_from_req(
    req: HttpRequest,
    client_cert_header: &Option<String>,
) -> Result<(String, String, String)> {
    match client_cert_header {
        Some(header) => parse_certs_from_header(req, header),
        None => parse_certs_direct_tls(req),
    }
}
