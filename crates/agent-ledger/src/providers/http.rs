//! The HTTP the vendor modules share.
//!
//! Every client in this library is built here, and that is the point rather
//! than a convenience: the idle window, the connect timeout and the identity
//! this library presents are single answers, and a vendor module that built its
//! own client would be a second answer nobody would notice had drifted.
//!
//! It is also what makes "no test reaches a vendor" a property of the code. In
//! test builds this constructor binds every client's socket to the loopback
//! interface, so a connection to anything else cannot be established and no
//! packet leaves the machine. A guard that lives in one constructor covers a
//! vendor module written next year; a guard that lives in the test runner
//! covers only the tests someone remembered to run under it.

use std::time::Duration;

/// What this library calls itself when it talks to a provider.
const USER_AGENT: &str = concat!("agent-ledger/", env!("CARGO_PKG_VERSION"));

/// Per-read inactivity window for a streaming connection. If no bytes arrive
/// within this span the read fails fast instead of hanging on a half-open
/// socket.
///
/// This is the single source of truth for the stream-idle window: the
/// application-level watchdog in the bind loop — the primary, typed detector —
/// derives from it, so the transport backstop and the watchdog cannot drift
/// apart and start disagreeing about when a stream is dead.
pub const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(90);

/// Connection-establishment timeout. Bounded so a dead host fails promptly
/// rather than waiting out the operating system's own default.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The one builder every client in this library starts from.
fn builder() -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(CONNECT_TIMEOUT);

    // Test builds bind the socket to the loopback interface. The operating
    // system then refuses to open a connection from it to any other address, so
    // an outbound request fails at connect and nothing is transmitted. A test
    // that quietly talked to a real vendor would be slow, flaky, dependent on a
    // credential, and — the part that actually costs — would look like it was
    // testing this library.
    #[cfg(test)]
    let builder = builder.local_address(std::net::IpAddr::from([127, 0, 0, 1]));

    builder
}

/// The general-purpose client, for the requests that are not a turn: listing
/// models, exchanging a credential.
///
/// # Panics
///
/// If the client cannot be constructed at all, which means the process has no
/// working TLS or resolver and nothing here could proceed anyway.
#[must_use]
pub fn client() -> reqwest::Client {
    builder().build().expect("failed to build HTTP client")
}

/// The client a turn streams over.
///
/// It sets a per-read inactivity timeout, NOT an overall request timeout —
/// which would kill a legitimately long stream — so a stalled, half-open
/// connection surfaces a transport error instead of hanging indefinitely. Safe
/// to reuse for non-streaming calls against the same provider: the read timeout
/// bounds gaps between bytes, never the total duration.
///
/// # Panics
///
/// If the client cannot be constructed at all.
#[must_use]
pub fn streaming_client() -> reqwest::Client {
    builder()
        .read_timeout(STREAM_READ_TIMEOUT)
        .build()
        .expect("failed to build streaming HTTP client")
}

/// A client with an overall request timeout, for exchanges that are a single
/// bounded round trip rather than a stream.
///
/// # Panics
///
/// If the client cannot be constructed at all.
#[must_use]
pub fn bounded_client(timeout: Duration) -> reqwest::Client {
    builder()
        .timeout(timeout)
        .build()
        .expect("failed to build bounded HTTP client")
}

/// Read a `Retry-After` header as whole seconds, when the response carries one.
///
/// Shared by every vendor's rate-limit classification so backoff honours the
/// server's own hint rather than each vendor guessing separately.
#[must_use]
pub fn retry_after(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard, asserted rather than assumed: a request through a client this
    /// module built cannot leave the machine. Any future test that tries to
    /// talk to a vendor fails here instead of quietly succeeding.
    ///
    /// The destination is a reserved address written as a literal, so not even
    /// a name lookup happens: the attempt dies when the socket, bound to
    /// loopback, is asked to reach somewhere else. No server answers, which is
    /// why the error carries no HTTP status.
    #[tokio::test]
    async fn a_client_from_this_module_cannot_reach_the_network() {
        const RESERVED: &str = "http://198.51.100.1/models";

        let err = client()
            .get(RESERVED)
            .send()
            .await
            .expect_err("an outbound request must not succeed in the test build");
        assert!(
            err.status().is_none(),
            "nothing answered, because nothing was reached"
        );

        let err = streaming_client()
            .post(RESERVED)
            .send()
            .await
            .expect_err("the streaming client is guarded too");
        assert!(err.status().is_none());

        let err = bounded_client(Duration::from_secs(5))
            .post(RESERVED)
            .send()
            .await
            .expect_err("so is the bounded one");
        assert!(err.status().is_none());
    }

    /// The transport backstop and the bind loop's typed watchdog are one
    /// number. Two numbers here would mean a stall detected twice, or — worse —
    /// a transport that gives up before the watchdog can call the drop
    /// recoverable, turning every stall into a terminal error.
    #[test]
    fn the_idle_window_is_one_value() {
        assert_eq!(
            STREAM_READ_TIMEOUT,
            super::super::bind::STREAM_IDLE,
            "the transport backstop and the typed watchdog are the same number"
        );
    }
}
