//! This vendor's device-code authorization flow.
//!
//! Two things here are easy to get wrong and expensive to get wrong.
//!
//! **The refresh token rotates.** Every refresh returns a new one, and
//! discarding it breaks the *next* refresh rather than this one — so the failure
//! surfaces hours later, as a session that expired for no visible reason. Every
//! caller of a refresh persists what comes back.
//!
//! **Expiry is not the same as validity.** A token can be rejected while its
//! recorded expiry still looks fresh — a revocation on the vendor's side, or a
//! clock that drifted. Deciding whether to ask a human to sign in again means
//! actually attempting a refresh, not trusting the local timestamp.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::providers::http;

const OAUTH_DEVICE_AUTH_URL: &str = "https://auth.kimi.com/api/oauth/device_authorization";
const OAUTH_TOKEN_URL: &str = "https://auth.kimi.com/api/oauth/token";
const OAUTH_CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const OAUTH_DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const OAUTH_REFRESH_GRANT: &str = "refresh_token";
const REQUEST_TIMEOUT: Duration = Duration::from_mins(2);

/// How long before the recorded expiry a token counts as expiring. A margin
/// wide enough that a request started now will not have its token expire
/// mid-flight.
const EXPIRY_MARGIN_MS: i64 = 60_000;

/// What a human needs in order to approve this authorization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAuth {
    /// The code this process exchanges once approval lands.
    pub device_code: String,
    /// The code the human types.
    pub user_code: String,
    /// Where the human goes to type it.
    pub verification_uri: String,
    /// The same place with the code already filled in, where the vendor offers
    /// one.
    pub verification_uri_complete: Option<String>,
    /// How long the codes stay valid, in seconds.
    pub expires_in: u64,
    /// How often the vendor wants to be asked, in seconds.
    pub interval: u64,
}

/// The tokens an approved authorization yields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    /// The access token.
    pub access_token: String,
    /// The refresh token — a NEW one each time, which must be persisted.
    pub refresh_token: String,
    /// The token type the vendor reports.
    pub token_type: String,
    /// How long the access token lasts, in seconds.
    pub expires_in: u64,
}

/// Everything that can go wrong authorizing.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// The transport failed.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// The authorization server answered with a non-success status.
    #[error("oauth error {status}: {message}")]
    Api {
        /// The status it answered with.
        status: u16,
        /// What it said.
        message: String,
    },

    /// The device code expired before anyone approved it.
    #[error("device code expired")]
    Expired,

    /// A human declined.
    #[error("authorization denied")]
    Denied,

    /// The refresh token is no longer accepted, so a human must sign in again.
    #[error("the session has expired — sign in again")]
    SessionExpired,

    /// A response did not parse.
    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Percent-encode one key or value for a form body.
///
/// Everything outside the unreserved set is escaped, and a space becomes `+`,
/// which is what `application/x-www-form-urlencoded` means by a space. The
/// characters that matter most here are `+`, `&` and `=`: a token carrying one
/// of them, pasted verbatim into the body, ends the value early or invents a
/// pair — and the server answers about a parameter nobody sent.
fn encode_form_component(value: &str) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push('+'),
            // Writing into a string cannot fail, so the result is discarded
            // rather than propagated through a function that cannot report it.
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

fn form_body(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_form_component(k), encode_form_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Begin the flow, returning what the human needs.
///
/// # Errors
///
/// If the request fails or the authorization server refuses it.
pub async fn start_device_auth() -> Result<DeviceAuth, OAuthError> {
    let client = http::bounded_client(REQUEST_TIMEOUT);
    let body = form_body(&[("client_id", OAUTH_CLIENT_ID)]);

    let response = client
        .post(OAUTH_DEVICE_AUTH_URL)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .body(body)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if !status.is_success() {
        return Err(OAuthError::Api {
            status: status.as_u16(),
            message: text,
        });
    }

    let auth: DeviceAuth = serde_json::from_str(&text)?;
    info!("device auth started");
    Ok(auth)
}

/// Wait for a human to approve, honouring the server's pacing.
///
/// It backs off when told to slow down and stops when the code expires, because
/// a poll loop that ignores either is how a client gets its credentials revoked.
///
/// # Errors
///
/// If the code expires, a human declines, or the request fails.
pub async fn poll_device_token(device: &DeviceAuth) -> Result<TokenResponse, OAuthError> {
    let client = http::bounded_client(REQUEST_TIMEOUT);
    let mut interval = std::cmp::max(1, device.interval) * 1000;
    let deadline = std::time::Instant::now() + Duration::from_secs(device.expires_in);

    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(interval)).await;

        let body = form_body(&[
            ("client_id", OAUTH_CLIENT_ID),
            ("device_code", &device.device_code),
            ("grant_type", OAUTH_DEVICE_GRANT),
        ]);

        let response = client
            .post(OAUTH_TOKEN_URL)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if status.is_success() {
            let tokens: TokenResponse = serde_json::from_str(&text)?;
            info!("device auth completed");
            return Ok(tokens);
        }

        let error_json: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        let error_code = error_json["error"].as_str().unwrap_or("");

        match error_code {
            "authorization_pending" => {}
            "slow_down" => interval += 5000,
            "expired_token" => return Err(OAuthError::Expired),
            "access_denied" => return Err(OAuthError::Denied),
            _ => {
                return Err(OAuthError::Api {
                    status: status.as_u16(),
                    message: error_json["error_description"]
                        .as_str()
                        .unwrap_or(&text)
                        .to_string(),
                });
            }
        }
    }

    Err(OAuthError::Expired)
}

/// Exchange a refresh token for a fresh pair.
///
/// # Errors
///
/// [`OAuthError::SessionExpired`] if the refresh token is no longer accepted, so
/// a caller can ask for a fresh sign-in rather than retrying forever. Otherwise
/// whatever the request failed with.
pub async fn do_refresh_token(refresh: &str) -> Result<TokenResponse, OAuthError> {
    let client = http::bounded_client(REQUEST_TIMEOUT);
    let body = form_body(&[
        ("client_id", OAUTH_CLIENT_ID),
        ("refresh_token", refresh),
        ("grant_type", OAUTH_REFRESH_GRANT),
    ]);

    let response = client
        .post(OAUTH_TOKEN_URL)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .body(body)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if !status.is_success() {
        // Tell a revoked or expired refresh token apart from every other
        // failure, so the caller knows to prompt rather than to retry.
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text)
            && json["error"].as_str() == Some("invalid_grant")
        {
            return Err(OAuthError::SessionExpired);
        }
        return Err(OAuthError::Api {
            status: status.as_u16(),
            message: text,
        });
    }

    let tokens: TokenResponse = serde_json::from_str(&text)?;
    info!("token refreshed");
    Ok(tokens)
}

/// Whether a token is close enough to its expiry to refresh now.
///
/// An unknown expiry counts as expiring: refreshing unnecessarily costs one
/// request, while using a dead token costs the turn.
#[must_use]
pub fn is_token_expiring(expires_at: Option<i64>) -> bool {
    match expires_at {
        Some(ts) => ts - chrono::Utc::now().timestamp_millis() < EXPIRY_MARGIN_MS,
        None => true,
    }
}

/// Ensure the access token is fresh, refreshing when it is not.
///
/// Returns the token to use plus the refresh token and expiry to persist. The
/// caller MUST persist both when they change: the refresh token rotates, and
/// dropping the new one breaks the next refresh rather than this one.
///
/// # Errors
///
/// If no refresh token is available, or the refresh itself fails.
pub async fn ensure_fresh_token(
    access_token: Option<String>,
    refresh: Option<String>,
    expires_at: Option<i64>,
) -> Result<(String, Option<String>, Option<i64>), OAuthError> {
    if !is_token_expiring(expires_at)
        && let Some(token) = access_token
    {
        return Ok((token, refresh, expires_at));
    }

    let refresh_token = refresh.ok_or_else(|| OAuthError::Api {
        status: 401,
        message: "No refresh token available".to_string(),
    })?;

    let tokens = do_refresh_token(&refresh_token).await?;
    let new_expires_at = chrono::Utc::now().timestamp_millis()
        + i64::try_from(tokens.expires_in).unwrap_or(0) * 1000;

    Ok((
        tokens.access_token,
        Some(tokens.refresh_token),
        Some(new_expires_at),
    ))
}

#[cfg(test)]
mod form_body_tests {
    use super::*;

    /// A refresh token is opaque text the vendor chose, and these three
    /// characters are the ones that corrupt a form body: `+` decodes as a
    /// space, `&` starts a new pair, `=` splits a key from a value. Encoded,
    /// the pair survives the round trip exactly.
    #[test]
    fn a_token_with_plus_ampersand_and_equals_survives_encoding() {
        let body = form_body(&[
            ("client_id", OAUTH_CLIENT_ID),
            ("refresh_token", "a+b&c=d/e"),
            ("grant_type", OAUTH_REFRESH_GRANT),
        ]);

        assert_eq!(
            body,
            format!(
                "client_id={OAUTH_CLIENT_ID}&refresh_token=a%2Bb%26c%3Dd%2Fe&grant_type=refresh_token"
            )
        );

        // The pairs still parse as three, and the token comes back whole.
        let pairs: Vec<&str> = body.split('&').collect();
        assert_eq!(pairs.len(), 3, "no character invented a fourth pair");
        assert_eq!(pairs[1], "refresh_token=a%2Bb%26c%3Dd%2Fe");
    }

    /// Spaces and non-ASCII text encode too, so no byte reaches the wire raw.
    #[test]
    fn spaces_and_non_ascii_are_escaped() {
        assert_eq!(encode_form_component("a b"), "a+b");
        assert_eq!(encode_form_component("ü"), "%C3%BC");
        assert_eq!(encode_form_component("safe-._~09AZ"), "safe-._~09AZ");
    }
}
