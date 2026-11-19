//! The WebSocket transport the relay pool connects with: the library's own
//! direct connection, plus a `User-Agent` header on the opening handshake.
//!
//! rust-nostr's pool connects through async-wsocket, whose direct connect
//! builds the handshake from the URL alone, so it sends no `User-Agent`. Some
//! relays refuse that: one we read answers such an upgrade with `403 Forbidden`
//! and the same upgrade with any `User-Agent` with `101 Switching Protocols`
//! (measured 2026-10-14; `docs/nostr-relay-pool-limitations.md` entry #10). The pool takes
//! a pluggable transport, so this one is async-wsocket 0.13.2's
//! `connect_direct` and its Happy Eyeballs connect, copied, with the header
//! added, and the library's own sink wrapper after it. It calls the same
//! tokio-tungstenite release async-wsocket does, so the stream is an
//! `async_wsocket::WebSocket::Tokio` and everything after the handshake (TLS,
//! framing, ping) is the library's, as are the error strings the pool logs
//! (`Connection failed.` with `HTTP error: 403 Forbidden`, `timeout`).
//!
//! `Cargo.toml` pins async-wsocket to the release this was copied from, and
//! keeps tokio-tungstenite on that release's: on any other, `WebSocket::Tokio`
//! no longer type-checks, so a mismatch fails the build rather than the
//! connection. On an upgrade, re-diff the copies against the new sources.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_wsocket::futures_util::stream::SplitSink;
use async_wsocket::futures_util::{Sink, SinkExt, StreamExt, TryStreamExt};
use async_wsocket::{ConnectionMode, Error, Message, WebSocket};
use nostr::Url;
use nostr::util::BoxedFuture;
use nostr_relay_pool::transport::error::TransportError;
use nostr_relay_pool::transport::websocket::{
    DefaultWebsocketTransport, WebSocketSink, WebSocketStream, WebSocketTransport,
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, USER_AGENT as USER_AGENT_HEADER};

/// The `User-Agent` nozzl sends to relays: the product name and the
/// workspace version, nothing that identifies the build or the operator. The
/// relay handshakes and the NIP-11 fetch (`nip11.rs`) both send it.
pub const USER_AGENT: &str = concat!("nozzl/", env!("CARGO_PKG_VERSION"));

/// The pool's WebSocket transport: async-wsocket's direct connect with a
/// `User-Agent`. A proxy or Tor connection mode, which nozzl never
/// configures, goes to the library's own transport unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct UserAgentTransport;

impl WebSocketTransport for UserAgentTransport {
    fn support_ping(&self) -> bool {
        true
    }

    fn connect<'a>(
        &'a self,
        url: &'a Url,
        mode: &'a ConnectionMode,
        timeout: Duration,
    ) -> BoxedFuture<'a, Result<(WebSocketSink, WebSocketStream), TransportError>> {
        Box::pin(async move {
            if *mode != ConnectionMode::Direct {
                return DefaultWebsocketTransport.connect(url, mode, timeout).await;
            }
            let socket: WebSocket = connect_direct(url, timeout)
                .await
                .map_err(TransportError::backend)?;

            // From here on, as in the library's `DefaultWebsocketTransport`.
            let (tx, rx) = socket.split();
            // NOTE: don't use sink_map_err here, as it may cause panics!
            // Issue: https://github.com/rust-nostr/nostr/issues/984 (Panic in relay connection)
            let sink: WebSocketSink = Box::new(TransportSink(tx)) as WebSocketSink;
            let stream: WebSocketStream =
                Box::pin(rx.map_err(TransportError::backend)) as WebSocketStream;
            Ok((sink, stream))
        })
    }
}

/// The opening handshake for `url`: the request tungstenite builds from the
/// URL (Host, Connection, Upgrade, Sec-WebSocket-Version and -Key), plus the
/// `User-Agent`.
// The error is async-wsocket's own, large because tungstenite's is; kept so the pool logs
// what it always has. Built once per connection attempt, never on a hot path.
#[allow(clippy::result_large_err)]
pub(crate) fn handshake_request(url: &Url) -> Result<Request, Error> {
    let mut request = url.as_str().into_client_request().map_err(Error::Ws)?;
    request
        .headers_mut()
        .insert(USER_AGENT_HEADER, HeaderValue::from_static(USER_AGENT));
    Ok(request)
}

// ---------------------------------------------------------------------------------------------
// Copied from async-wsocket 0.13.2, src/native/mod.rs (`connect_direct`, `happy_eyeballs_connect`,
// `try_addrs_sequential`), whose header reads:
//
//   Copyright (c) 2022-2024 Yuki Kishimoto
//   Distributed under the MIT software license
//
// Its full MIT notice is in THIRD_PARTY_NOTICES.md, at the repository root.
//
// Changed: the handshake is `handshake_request(url)` instead of `url.as_str()` (built before the
// TCP connect, so a URL tungstenite rejects fails before any network I/O, with the same
// `Error::Ws`); the errors are built from async-wsocket's public variants, as its private
// helpers build them; and Happy Eyeballs is split at the DNS lookup (`connect_resolved`), so it
// can be tested with explicit addresses. The logic is otherwise the original's.
// ---------------------------------------------------------------------------------------------

/// Happy Eyeballs connection delay (RFC 8305).
const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

async fn connect_direct(url: &Url, timeout: Duration) -> Result<WebSocket, Error> {
    let host: &str = url
        .host_str()
        .ok_or(Error::Url(url::ParseError::EmptyHost))?;
    let port: u16 = url
        .port_or_known_default()
        .ok_or(Error::Url(url::ParseError::InvalidPort))?;
    let request = handshake_request(url)?;

    let conn_fut = async {
        let tcp_stream = happy_eyeballs_connect(host, port).await?;

        // NOT REMOVE `Box::pin`!
        // Use `Box::pin` to fix stack overflow on windows targets due to large `Future`
        let (stream, _) = Box::pin(tokio_tungstenite::client_async_tls(request, tcp_stream))
            .await
            .map_err(Error::Ws)?;

        Ok::<_, Error>(stream)
    };

    let stream = tokio::time::timeout(timeout, conn_fut)
        .await
        .map_err(|_| Error::Timeout)??;

    Ok(WebSocket::Tokio(stream))
}

/// Connect to a host using the Happy Eyeballs algorithm (RFC 8305).
///
/// When DNS returns both IPv6 and IPv4 addresses, tries the preferred family
/// first and starts the other family after a 250ms delay if the first hasn't
/// connected yet. Uses whichever connection succeeds first.
async fn happy_eyeballs_connect(host: &str, port: u16) -> Result<TcpStream, Error> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(Error::Io)?
        .collect();
    connect_resolved(addrs).await
}

/// Happy Eyeballs over the addresses DNS returned, in its order.
async fn connect_resolved(addrs: Vec<SocketAddr>) -> Result<TcpStream, Error> {
    if addrs.is_empty() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "DNS resolution returned no addresses",
        )));
    }

    // Separate into IPv6 and IPv4, preserving order within each group
    let (ipv6, ipv4): (Vec<SocketAddr>, Vec<SocketAddr>) =
        addrs.into_iter().partition(SocketAddr::is_ipv6);

    // If only one family, try addresses sequentially
    if ipv4.is_empty() {
        return try_addrs_sequential(&ipv6).await;
    }
    if ipv6.is_empty() {
        return try_addrs_sequential(&ipv4).await;
    }

    // Both families available: Happy Eyeballs
    // Try first IPv6 address, after delay start first IPv4 in parallel
    let ipv6_fut = TcpStream::connect(ipv6[0]);
    tokio::pin!(ipv6_fut);

    // Phase 1: Give IPv6 a 250ms head start
    tokio::select! {
        result = &mut ipv6_fut => {
            return match result {
                Ok(stream) => Ok(stream),
                // IPv6 failed fast, try IPv4 directly
                Err(_) => try_addrs_sequential(&ipv4).await,
            }
        }
        () = tokio::time::sleep(HAPPY_EYEBALLS_DELAY) => {
            // Timer fired, IPv6 still pending. Start IPv4 and race both.
        }
    }

    // Phase 2: Race the still-pending IPv6 against a new IPv4 attempt.
    // Use a loop so that if one fails, we keep waiting for the other.
    let ipv4_fut = TcpStream::connect(ipv4[0]);
    tokio::pin!(ipv4_fut);

    let mut ipv6_done = false;
    let mut ipv4_done = false;

    loop {
        tokio::select! {
            result = &mut ipv6_fut, if !ipv6_done => {
                match result {
                    Ok(stream) => return Ok(stream),
                    Err(_) => { ipv6_done = true; }
                }
            }
            result = &mut ipv4_fut, if !ipv4_done => {
                match result {
                    Ok(stream) => return Ok(stream),
                    Err(_) => { ipv4_done = true; }
                }
            }
        }
        if ipv6_done && ipv4_done {
            break;
        }
    }

    // Both initial attempts failed, try remaining addresses sequentially
    // Interleave remaining IPv4 and IPv6 per RFC 8305
    let mut remaining = Vec::new();
    let mut ipv4_iter = ipv4.iter().skip(1).peekable();
    let mut ipv6_iter = ipv6.iter().skip(1).peekable();

    // Interleave: take one from ipv4, then one from ipv6, alternating
    while ipv4_iter.peek().is_some() || ipv6_iter.peek().is_some() {
        if let Some(addr) = ipv4_iter.next() {
            remaining.push(*addr);
        }
        if let Some(addr) = ipv6_iter.next() {
            remaining.push(*addr);
        }
    }

    try_addrs_sequential(&remaining).await
}

/// Try connecting to addresses sequentially, returning the first success.
async fn try_addrs_sequential(addrs: &[SocketAddr]) -> Result<TcpStream, Error> {
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(Error::Io(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "no addresses to connect to",
        )
    })))
}

// ---------------------------------------------------------------------------------------------
// Copied from the pool's own transport (nostr-relay-pool 0.44, whisperhash/nostr fork,
// src/transport/websocket.rs, `TransportSink`), whose header reads:
//
//   Copyright (c) 2022-2023 Yuki Kishimoto
//   Copyright (c) 2023-2025 Rust Nostr Developers
//   Distributed under the MIT software license
//
// Its full MIT notice is in THIRD_PARTY_NOTICES.md, at the repository root.
// ---------------------------------------------------------------------------------------------

/// The library's sink wrapper: maps the socket's errors without
/// `sink_map_err` (rust-nostr/nostr#984 (Panic in relay connection)).
struct TransportSink(SplitSink<WebSocket, Message>);

impl Sink<Message> for TransportSink {
    type Error = TransportError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_ready_unpin(cx)
            .map_err(TransportError::backend)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.0)
            .start_send_unpin(item)
            .map_err(TransportError::backend)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_flush_unpin(cx)
            .map_err(TransportError::backend)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_close_unpin(cx)
            .map_err(TransportError::backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_is_what_relays_see() {
        // A version bump changes what every relay sees; change this with it, deliberately.
        assert_eq!(USER_AGENT, "nozzl/0.1.0");
    }

    #[test]
    fn handshake_request_carries_the_user_agent_and_the_upgrade() {
        let url = Url::parse("wss://relay.example.org/").unwrap();
        let request = handshake_request(&url).unwrap();
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .map(|v| v.to_str().unwrap().to_owned())
        };
        assert_eq!(header("user-agent").as_deref(), Some(USER_AGENT));
        // What tungstenite builds from the URL is still all there.
        assert_eq!(header("host").as_deref(), Some("relay.example.org"));
        assert_eq!(header("upgrade").as_deref(), Some("websocket"));
        assert_eq!(header("connection").as_deref(), Some("Upgrade"));
        assert_eq!(header("sec-websocket-version").as_deref(), Some("13"));
        assert!(header("sec-websocket-key").is_some_and(|k| !k.is_empty()));
        assert_eq!(request.uri().to_string(), "wss://relay.example.org/");
        // One User-Agent, not one appended to another.
        assert_eq!(request.headers().get_all("user-agent").iter().count(), 1);
    }

    #[test]
    fn handshake_request_keeps_a_non_default_port_and_path() {
        let url = Url::parse("ws://127.0.0.1:7777/nostr").unwrap();
        let request = handshake_request(&url).unwrap();
        assert_eq!(request.uri().to_string(), "ws://127.0.0.1:7777/nostr");
        assert_eq!(
            request.headers().get("host").unwrap().to_str().unwrap(),
            "127.0.0.1:7777"
        );
        assert_eq!(
            request
                .headers()
                .get("user-agent")
                .unwrap()
                .to_str()
                .unwrap(),
            USER_AGENT
        );
    }

    #[test]
    fn pings_are_supported_as_in_the_library() {
        assert_eq!(
            UserAgentTransport.support_ping(),
            DefaultWebsocketTransport.support_ping()
        );
    }

    /// The pool puts no timeout of its own around `connect` (its connect only
    /// races termination), so the transport's is the one bound on a peer that
    /// accepts TCP and never answers the upgrade; relays time out like that
    /// every day. The error must be the library's, word for word.
    #[tokio::test]
    async fn a_silent_peer_times_out_with_the_librarys_error_string() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _hold = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = listener.accept().await {
                held.push(s);
            }
        });
        let url = Url::parse(&format!("ws://{addr}/")).unwrap();
        // The test's own deadline, so a transport without one fails instead of hanging.
        let ours = tokio::time::timeout(
            Duration::from_secs(5),
            UserAgentTransport.connect(&url, &ConnectionMode::Direct, Duration::from_millis(300)),
        )
        .await
        .expect("the transport must bound a silent peer itself")
        .err()
        .expect("a peer that never answers must time out");
        let library = DefaultWebsocketTransport
            .connect(&url, &ConnectionMode::Direct, Duration::from_millis(300))
            .await
            .err()
            .expect("the library's transport times out too");
        assert_eq!(ours.to_string(), "timeout");
        assert_eq!(ours.to_string(), library.to_string());
    }

    /// On a host with no IPv6 route, for every relay with both families
    /// the IPv6 attempt fails at once and the IPv4 fallback is the path taken.
    /// Here IPv6 fails fast on any machine: nothing listens on `[::1]`, and a
    /// host without IPv6 refuses the address outright.
    #[tokio::test]
    async fn happy_eyeballs_falls_back_to_ipv4_when_ipv6_fails_fast() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addrs: Vec<SocketAddr> = vec![
            SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, port)),
            SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port)),
        ];
        let stream = tokio::time::timeout(Duration::from_secs(5), connect_resolved(addrs))
            .await
            .expect("no hang")
            .expect("falls back to IPv4");
        assert!(stream.peer_addr().unwrap().is_ipv4());
    }

    #[tokio::test]
    async fn no_addresses_is_an_error_not_a_hang() {
        let err = connect_resolved(Vec::new()).await.expect_err("an error");
        assert!(err.to_string().contains("no addresses"), "got: {err}");
    }
}
