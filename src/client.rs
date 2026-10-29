use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use crate::Error;
use crate::frame::{self, Frame, Header};

/// The open platform of Feishu (China).
pub const FEISHU_DOMAIN: &str = "https://open.feishu.cn";
/// The open platform of Lark (international).
pub const LARK_DOMAIN: &str = "https://open.larksuite.com";

const ENDPOINT_PATH: &str = "/callback/ws/endpoint";

// Codes of the endpoint response.
const OK: i64 = 0;
const SYSTEM_BUSY: i64 = 1;
const INTERNAL_ERROR: i64 = 1_000_040_343;

// Handshake failures, from the `handshake-*` response headers.
const FORBIDDEN: i64 = 403;
const AUTH_FAILED: i64 = 514;
const EXCEED_CONN_LIMIT: i64 = 1_000_040_350;

/// How long the parts of a split message wait for the rest.
const PARTS_TTL: Duration = Duration::from_secs(5);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The app to receive events for.
#[derive(Clone, Debug)]
pub struct Config {
    app_id: String,
    app_secret: String,
    domain: String,
}

impl Config {
    pub fn new(app_id: impl Into<String>, app_secret: impl Into<String>) -> Self {
        Config {
            app_id: app_id.into(),
            app_secret: app_secret.into(),
            domain: FEISHU_DOMAIN.into(),
        }
    }

    /// The open platform's base URL: [`FEISHU_DOMAIN`] (the default), [`LARK_DOMAIN`], or
    /// another server speaking the same protocol.
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = domain.into().trim_end_matches('/').to_string();
        self
    }
}

/// An event Feishu pushed, such as `im.message.receive_v1`.
#[derive(Clone, Debug)]
pub struct Event {
    /// The id of the frame that carried it; Feishu sends it again under the same id when
    /// it was not acknowledged.
    pub message_id: String,
    pub trace_id: String,
    /// The event as Feishu sent it: `{"schema": "2.0", "header": {...}, "event": {...}}`.
    pub payload: Value,
}

impl Event {
    /// The event's type, such as `im.message.receive_v1`.
    pub fn event_type(&self) -> Option<&str> {
        self.payload
            .pointer("/header/event_type")
            .and_then(Value::as_str)
    }
}

/// Where the connection stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Connecting,
    Connected,
    /// The connection ended or could not be made, and why; the client retries unless the
    /// reason is permanent.
    Disconnected(String),
}

/// What receives the events.
pub trait Handler: Send + Sync {
    /// An event Feishu pushed. The client acknowledges it when this returns, and Feishu
    /// sends it again if that takes more than 3 seconds: hand the event to other work
    /// instead of processing it here.
    fn on_event(&self, event: Event);

    /// The connection's state changed.
    fn on_state(&self, state: State) {
        let _ = state;
    }
}

/// The connection settings Feishu hands out with the endpoint and in pongs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Settings {
    /// Attempts per reconnect, -1 for no limit.
    reconnect_count: i64,
    /// Seconds between attempts.
    reconnect_interval: u64,
    /// Up to this many seconds before the first attempt, so clients do not all reconnect
    /// at once.
    reconnect_nonce: u64,
    /// Seconds between pings.
    ping_interval: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            reconnect_count: -1,
            reconnect_interval: 120,
            reconnect_nonce: 30,
            ping_interval: 120,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ClientConfig {
    reconnect_count: Option<i64>,
    reconnect_interval: Option<u64>,
    reconnect_nonce: Option<u64>,
    ping_interval: Option<u64>,
}

impl Settings {
    fn apply(&mut self, config: &ClientConfig) {
        self.reconnect_count = config.reconnect_count.unwrap_or(self.reconnect_count);
        self.reconnect_interval = config.reconnect_interval.unwrap_or(self.reconnect_interval);
        self.reconnect_nonce = config.reconnect_nonce.unwrap_or(self.reconnect_nonce);
        // A zero interval would ping without pause.
        self.ping_interval = config
            .ping_interval
            .filter(|s| *s > 0)
            .unwrap_or(self.ping_interval);
    }
}

#[derive(Deserialize)]
struct EndpointResponse {
    code: i64,
    #[serde(default)]
    msg: String,
    data: Option<Endpoint>,
}

#[derive(Deserialize)]
struct Endpoint {
    #[serde(rename = "URL")]
    url: Option<String>,
    #[serde(rename = "ClientConfig")]
    client_config: Option<ClientConfig>,
}

/// Receives an app's events over Feishu's long connection, reconnecting as Feishu
/// directs.
pub struct Client {
    config: Config,
    http: reqwest::Client,
    tls: Arc<rustls::ClientConfig>,
}

impl Client {
    pub fn new(config: Config) -> Result<Self, Error> {
        let tls = crate::tls::config()?;
        let http = reqwest::Client::builder()
            .tls_backend_preconfigured((*tls).clone())
            .user_agent(concat!("feishu-ws/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Client { config, http, tls })
    }

    /// Receive events until `shutdown` completes (`Ok`) or Feishu refuses the app for good
    /// (`Err`); other failures are retried.
    pub async fn run<H: Handler>(
        &self,
        handler: &H,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error> {
        tokio::select! {
            result = self.keep_connected(handler) => result,
            () = shutdown => Ok(()),
        }
    }

    async fn keep_connected<H: Handler>(&self, handler: &H) -> Result<(), Error> {
        let mut settings = Settings::default();
        // After a connection ends or the first one fails: a random pause, then attempts
        // `reconnect_interval` apart, at most `reconnect_count` of them.
        let mut reconnecting = false;
        let mut failures: u32 = 0;
        loop {
            if reconnecting {
                if failures == 0 {
                    sleep(jitter(settings.reconnect_nonce)).await;
                } else {
                    if settings.reconnect_count >= 0
                        && i64::from(failures) >= settings.reconnect_count
                    {
                        let error = Error::Unreachable(failures);
                        handler.on_state(State::Disconnected(error.to_string()));
                        return Err(error);
                    }
                    sleep(Duration::from_secs(settings.reconnect_interval)).await;
                }
            }
            handler.on_state(State::Connecting);
            match self.connect(&mut settings).await {
                Ok((socket, service, device)) => {
                    tracing::info!(device_id = %device, "connected to feishu");
                    handler.on_state(State::Connected);
                    let reason = Session::new(service, handler)
                        .serve(socket, &mut settings)
                        .await;
                    tracing::info!(device_id = %device, %reason, "feishu connection ended");
                    handler.on_state(State::Disconnected(reason));
                    reconnecting = true;
                    failures = 0;
                }
                Err(error) if error.is_permanent() => {
                    tracing::warn!(%error, "feishu refused the connection");
                    handler.on_state(State::Disconnected(error.to_string()));
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(%error, "connecting to feishu failed");
                    handler.on_state(State::Disconnected(error.to_string()));
                    if reconnecting {
                        failures += 1;
                    }
                    reconnecting = true;
                }
            }
        }
    }

    /// Open a connection: the socket, its service id and its device id.
    async fn connect(&self, settings: &mut Settings) -> Result<(Socket, i32, String), Error> {
        let url = self.endpoint(settings).await?;
        let parsed =
            url::Url::parse(&url).map_err(|e| Error::Protocol(format!("connection URL: {e}")))?;
        let query = |key: &str| {
            parsed
                .query_pairs()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.into_owned())
        };
        let service = query("service_id")
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Error::Protocol("connection URL has no service_id".into()))?;
        let device = query("device_id").unwrap_or_default();
        let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
            url.as_str(),
            None,
            false,
            Some(Connector::Rustls(self.tls.clone())),
        )
        .await
        .map_err(handshake_error)?;
        Ok((socket, service, device))
    }

    /// The URL to connect to, applying the settings that come with it. The URL carries
    /// credentials: it is never logged.
    async fn endpoint(&self, settings: &mut Settings) -> Result<String, Error> {
        let response = self
            .http
            .post(format!("{}{ENDPOINT_PATH}", self.config.domain))
            .header("locale", "zh")
            .json(&json!({"AppID": self.config.app_id, "AppSecret": self.config.app_secret}))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(Error::Unavailable {
                code: i64::from(response.status().as_u16()),
                message: "endpoint request failed".into(),
            });
        }
        let body: EndpointResponse = response.json().await?;
        match body.code {
            OK => {}
            SYSTEM_BUSY | INTERNAL_ERROR => {
                return Err(Error::Unavailable {
                    code: body.code,
                    message: body.msg,
                });
            }
            code => {
                return Err(Error::Rejected {
                    code,
                    message: body.msg,
                });
            }
        }
        let endpoint = body
            .data
            .ok_or_else(|| Error::Protocol("endpoint response has no data".into()))?;
        if let Some(config) = &endpoint.client_config {
            settings.apply(config);
        }
        endpoint
            .url
            .filter(|u| !u.is_empty())
            .ok_or_else(|| Error::Protocol("endpoint response has no URL".into()))
    }
}

/// A failed WebSocket handshake: Feishu explains it in `handshake-*` headers.
fn handshake_error(error: tungstenite::Error) -> Error {
    if let tungstenite::Error::Http(response) = &error {
        let header = |key: &str| response.headers().get(key).and_then(|v| v.to_str().ok());
        if let (Some(code), Some(message)) = (header("handshake-status"), header("handshake-msg")) {
            let code: i64 = code.parse().unwrap_or(-1);
            let message = message.to_string();
            let auth = header("handshake-autherrcode").and_then(|c| c.parse::<i64>().ok());
            return match code {
                FORBIDDEN => Error::Rejected { code, message },
                AUTH_FAILED if auth == Some(EXCEED_CONN_LIMIT) => Error::Rejected { code, message },
                _ => Error::Unavailable { code, message },
            };
        }
    }
    error.into()
}

/// Up to `max_secs` seconds, varying between clients.
fn jitter(max_secs: u64) -> Duration {
    if max_secs == 0 {
        return Duration::ZERO;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    Duration::from_millis(u64::from(nanos) % (max_secs * 1000))
}

/// The pieces of a message Feishu split into several frames.
struct Parts {
    pieces: Vec<Option<Vec<u8>>>,
    expires: Instant,
}

/// One open connection.
struct Session<'a, H> {
    service: i32,
    handler: &'a H,
    parts: HashMap<String, Parts>,
}

impl<'a, H: Handler> Session<'a, H> {
    fn new(service: i32, handler: &'a H) -> Self {
        Session {
            service,
            handler,
            parts: HashMap::new(),
        }
    }

    /// Ping, answer and acknowledge until the connection ends; why it ended.
    async fn serve(mut self, mut socket: Socket, settings: &mut Settings) -> String {
        let mut next_ping = Instant::now();
        loop {
            tokio::select! {
                () = sleep_until(next_ping) => {
                    let ping = Frame::ping(self.service).encode_to_vec();
                    if let Err(error) = socket.send(Message::Binary(ping.into())).await {
                        return format!("ping failed: {error}");
                    }
                    next_ping = Instant::now() + Duration::from_secs(settings.ping_interval);
                }
                message = socket.next() => match message {
                    None => return "closed".into(),
                    Some(Err(error)) => return format!("receive failed: {error}"),
                    Some(Ok(Message::Binary(data))) => {
                        let Some(reply) = self.handle(&data, settings) else { continue };
                        if let Err(error) = socket.send(Message::Binary(reply.encode_to_vec().into())).await {
                            return format!("acknowledge failed: {error}");
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return match frame {
                            Some(frame) => format!("closed by feishu: {} {}", frame.code, frame.reason),
                            None => "closed by feishu".into(),
                        };
                    }
                    Some(Ok(_)) => {}
                },
            }
        }
    }

    /// Handle one frame; the reply to send, if any.
    fn handle(&mut self, data: &[u8], settings: &mut Settings) -> Option<Frame> {
        let frame = match Frame::decode(data) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(%error, "undecodable feishu frame");
                return None;
            }
        };
        match frame.method {
            frame::CONTROL => {
                if frame.header(frame::TYPE) == Some("pong")
                    && let Some(payload) = frame.payload.as_deref().filter(|p| !p.is_empty())
                {
                    match serde_json::from_slice::<ClientConfig>(payload) {
                        Ok(config) => settings.apply(&config),
                        Err(error) => tracing::debug!(%error, "unreadable feishu pong"),
                    }
                }
                None
            }
            frame::DATA => self.handle_data(frame),
            method => {
                tracing::debug!(method, "unknown feishu frame");
                None
            }
        }
    }

    /// An event (or the last part of one): hand it over and acknowledge it.
    fn handle_data(&mut self, mut frame: Frame) -> Option<Frame> {
        let (Some(message_id), Some(kind)) = (
            frame.header(frame::MESSAGE_ID).map(str::to_string),
            frame.header(frame::TYPE).map(str::to_string),
        ) else {
            tracing::warn!("feishu data frame without message_id or type");
            return None;
        };
        let trace_id = frame
            .header(frame::TRACE_ID)
            .unwrap_or_default()
            .to_string();
        let sum = frame
            .header(frame::SUM)
            .and_then(|v| v.parse().ok())
            .unwrap_or(1usize);
        let seq = frame
            .header(frame::SEQ)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0usize);
        let piece = frame.payload.take().unwrap_or_default();
        let payload = if sum > 1 {
            self.combine(&message_id, sum, seq, piece)?
        } else {
            piece
        };
        // Card callbacks are not received; like the official SDKs, they go unanswered.
        if kind != "event" {
            tracing::debug!(%message_id, %kind, "ignoring feishu data frame");
            return None;
        }
        let started = std::time::Instant::now();
        let code = match serde_json::from_slice::<Value>(&payload) {
            Ok(payload) => {
                tracing::debug!(%message_id, %trace_id, "feishu event");
                self.handler.on_event(Event {
                    message_id,
                    trace_id,
                    payload,
                });
                200
            }
            Err(error) => {
                tracing::warn!(%message_id, %error, "feishu event is not JSON");
                500
            }
        };
        frame.headers.push(Header {
            key: frame::BIZ_RT.into(),
            value: started.elapsed().as_millis().to_string(),
        });
        frame.payload = Some(json!({ "code": code }).to_string().into_bytes());
        Some(frame)
    }

    /// Keep a piece of a split message; the whole message once every piece is in.
    fn combine(
        &mut self,
        message_id: &str,
        sum: usize,
        seq: usize,
        piece: Vec<u8>,
    ) -> Option<Vec<u8>> {
        let now = Instant::now();
        self.parts.retain(|_, parts| parts.expires > now);
        if seq >= sum {
            tracing::warn!(%message_id, sum, seq, "feishu frame part out of range");
            return None;
        }
        let parts = self
            .parts
            .entry(message_id.to_string())
            .or_insert_with(|| Parts {
                pieces: vec![None; sum],
                expires: now + PARTS_TTL,
            });
        if parts.pieces.len() != sum {
            parts.pieces = vec![None; sum];
        }
        parts.pieces[seq] = Some(piece);
        parts.expires = now + PARTS_TTL;
        if parts.pieces.iter().any(Option::is_none) {
            return None;
        }
        let parts = self.parts.remove(message_id)?;
        Some(parts.pieces.into_iter().flatten().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ignore;
    impl Handler for Ignore {
        fn on_event(&self, _: Event) {}
    }

    fn data(message_id: &str, sum: usize, seq: usize, payload: &[u8]) -> Frame {
        let header = |key: &str, value: String| Header {
            key: key.into(),
            value,
        };
        Frame {
            method: frame::DATA,
            headers: vec![
                header(frame::TYPE, "event".into()),
                header(frame::MESSAGE_ID, message_id.into()),
                header(frame::SUM, sum.to_string()),
                header(frame::SEQ, seq.to_string()),
            ],
            payload: Some(payload.to_vec()),
            ..Frame::default()
        }
    }

    #[tokio::test]
    async fn split_messages_are_combined_in_order() {
        let mut session = Session::new(1, &Ignore);
        let mut settings = Settings::default();
        assert!(
            session
                .handle(&data("m1", 2, 1, b"2}").encode_to_vec(), &mut settings)
                .is_none()
        );
        let reply = session
            .handle(&data("m1", 2, 0, b"{\"a\":").encode_to_vec(), &mut settings)
            .unwrap();
        assert_eq!(reply.payload.as_deref(), Some(&b"{\"code\":200}"[..]));
        assert!(reply.header(frame::BIZ_RT).is_some());
        assert!(session.parts.is_empty());
    }

    #[tokio::test]
    async fn events_that_are_not_json_are_refused() {
        let mut session = Session::new(1, &Ignore);
        let reply = session
            .handle(
                &data("m1", 1, 0, b"oops").encode_to_vec(),
                &mut Settings::default(),
            )
            .unwrap();
        assert_eq!(reply.payload.as_deref(), Some(&b"{\"code\":500}"[..]));
    }

    #[test]
    fn pongs_and_endpoints_update_the_settings() {
        let mut settings = Settings::default();
        let config: ClientConfig =
            serde_json::from_str(r#"{"PingInterval": 90, "ReconnectCount": 3}"#).unwrap();
        settings.apply(&config);
        assert_eq!(
            (
                settings.ping_interval,
                settings.reconnect_count,
                settings.reconnect_interval
            ),
            (90, 3, 120)
        );
        settings.apply(&ClientConfig {
            ping_interval: Some(0),
            ..ClientConfig::default()
        });
        assert_eq!(settings.ping_interval, 90);
    }

    #[test]
    fn jitter_stays_below_the_nonce() {
        assert_eq!(jitter(0), Duration::ZERO);
        assert!(jitter(1) < Duration::from_secs(1));
    }
}
