//! Receive [Feishu](https://open.feishu.cn) / [Lark](https://open.larksuite.com) events over
//! the long connection ("使用长连接接收事件"), the WebSocket the official SDKs open so an app
//! can receive events without a public callback URL.
//!
//! Events arrive as the JSON Feishu sent, so fields Feishu leaves out or sends as `null`
//! (such as `sender.sender_id.user_id`, present only with the user ID permission) cannot
//! fail them; pick out what you need yourself.
//!
//! ```no_run
//! use feishu_ws::{Client, Config, Event, Handler};
//!
//! struct Print;
//!
//! impl Handler for Print {
//!     fn on_event(&self, event: Event) {
//!         // Return quickly: Feishu sends the event again if it is not acknowledged in 3 seconds.
//!         println!("{:?}: {}", event.event_type(), event.payload);
//!     }
//! }
//!
//! # async fn run() -> Result<(), feishu_ws::Error> {
//! let client = Client::new(Config::new("cli_xxx", "secret"))?;
//! client.run(&Print, async { let _ = tokio::signal::ctrl_c().await; }).await
//! # }
//! ```
//!
//! The protocol follows the official SDKs: fetch a connection URL from
//! `/callback/ws/endpoint`, then exchange protobuf frames ([`frame`]), ping on Feishu's
//! interval, acknowledge every event, put split messages back together, and reconnect as
//! Feishu's settings say. Feishu delivers each event to one of an app's connections, at
//! most 50.

mod client;
mod error;
pub mod frame;
mod tls;

pub use client::{Client, Config, Event, FEISHU_DOMAIN, Handler, LARK_DOMAIN, State};
pub use error::Error;
