//! HTTPS client for job kinds that read external sources.
//!
//! The worker sits on the same network as Postgres, RustFS and Lakekeeper, so a URL taken
//! from a job payload must not reach them. Every request (and every redirect) is checked:
//! - only `https://`;
//! - only hosts listed in `INGEST_HTTP_ALLOWED_HOSTS` (comma-separated, exact match);
//! - the body is read up to `INGEST_HTTP_MAX_BYTES`;
//! - the whole request is bounded by `INGEST_HTTP_TIMEOUT_SECS`.
//!
//! Proxies come from the usual `HTTPS_PROXY` / `NO_PROXY` variables.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::{redirect, StatusCode, Url};
use serde_json::Value;

use crate::jobs::permanent;

pub struct HttpSource {
    client: reqwest::Client,
    allowed_hosts: Arc<Vec<String>>,
    max_bytes: usize,
}

/// Why a URL may not be fetched; `None` when it may.
fn rejection(url: &Url, allowed_hosts: &[String]) -> Option<String> {
    if url.scheme() != "https" {
        return Some(format!("only https URLs are allowed, got {url}"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Some("credentials in the URL are not allowed".to_string());
    }
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if !allowed_hosts.iter().any(|allowed| *allowed == host) {
        return Some(format!(
            "host {host:?} is not in INGEST_HTTP_ALLOWED_HOSTS ({})",
            if allowed_hosts.is_empty() { "empty".to_string() } else { allowed_hosts.join(",") }
        ));
    }
    None
}

impl HttpSource {
    pub fn from_env() -> Result<Self> {
        let allowed_hosts: Vec<String> = std::env::var("INGEST_HTTP_ALLOWED_HOSTS")
            .unwrap_or_default()
            .split(',')
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .collect();
        let max_bytes: usize = std::env::var("INGEST_HTTP_MAX_BYTES")
            .unwrap_or_else(|_| "33554432".to_string())
            .parse()
            .context("INGEST_HTTP_MAX_BYTES must be an integer")?;
        let timeout: u64 = std::env::var("INGEST_HTTP_TIMEOUT_SECS")
            .unwrap_or_else(|_| "60".to_string())
            .parse()
            .context("INGEST_HTTP_TIMEOUT_SECS must be an integer")?;
        let allowed_hosts = Arc::new(allowed_hosts);
        let for_redirects = allowed_hosts.clone();
        let client = reqwest::Client::builder()
            .user_agent(concat!("rust-datalake-lake-worker/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(timeout))
            .connect_timeout(Duration::from_secs(10))
            .redirect(redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 5 {
                    return attempt.error("more than 5 redirects");
                }
                match rejection(attempt.url(), &for_redirects) {
                    Some(reason) => attempt.error(format!("redirect refused: {reason}")),
                    None => attempt.follow(),
                }
            }))
            .build()
            .context("build HTTP client")?;
        Ok(Self { client, allowed_hosts, max_bytes })
    }

    pub fn allowed_hosts(&self) -> &[String] {
        &self.allowed_hosts
    }

    /// GETs `url` and parses the body as JSON.
    ///
    /// Errors that retrying cannot fix (bad URL, host not allowed, 4xx other than 408/429,
    /// body too large) are permanent; network errors, timeouts and 5xx are retryable.
    pub async fn get_json(&self, url: &str) -> Result<Value> {
        let url = Url::parse(url).map_err(|err| permanent(format!("invalid url {url:?}: {err}")))?;
        if let Some(reason) = rejection(&url, &self.allowed_hosts) {
            return Err(permanent(reason));
        }
        let mut response = self
            .client
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            // APIs usually explain a 4xx in the body: keep its start for last_error.
            let body = response.text().await.unwrap_or_default();
            let detail = body.trim().chars().take(300).collect::<String>();
            let message = if detail.is_empty() {
                format!("GET {url}: HTTP {status}")
            } else {
                format!("GET {url}: HTTP {status}: {detail}")
            };
            let retryable = status.is_server_error()
                || status == StatusCode::TOO_MANY_REQUESTS
                || status == StatusCode::REQUEST_TIMEOUT;
            if retryable {
                bail!(message);
            }
            return Err(permanent(message));
        }
        let too_large = || permanent(format!("GET {url}: body larger than INGEST_HTTP_MAX_BYTES ({})", self.max_bytes));
        if response.content_length().is_some_and(|len| len > self.max_bytes as u64) {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.with_context(|| format!("read body of {url}"))? {
            if body.len() + chunk.len() > self.max_bytes {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        // A 200 with a non-JSON body is often a transient maintenance page: retryable.
        serde_json::from_slice(&body).with_context(|| format!("GET {url}: body is not valid JSON"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_https_on_allowed_hosts() {
        let allowed = vec!["api.bcb.gov.br".to_string()];
        let check = |u: &str| rejection(&Url::parse(u).unwrap(), &allowed);
        assert_eq!(check("https://api.bcb.gov.br/dados?x=1"), None);
        assert_eq!(check("https://API.BCB.gov.br/dados"), None);
        assert!(check("http://api.bcb.gov.br/dados").is_some());
        assert!(check("https://evil.example/dados").is_some());
        assert!(check("https://api.bcb.gov.br.evil.example/").is_some());
        assert!(check("https://user:pw@api.bcb.gov.br/").is_some());
        assert!(check("https://lakekeeper:8181/catalog").is_some());
        assert!(rejection(&Url::parse("https://api.bcb.gov.br/").unwrap(), &[]).is_some());
    }
}
