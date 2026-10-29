# feishu-ws

Receive [Feishu](https://open.feishu.cn) / [Lark](https://open.larksuite.com) events over the
long connection (使用长连接接收事件): the WebSocket the official SDKs open so an app can receive
events without a public callback URL.

Events arrive as the JSON Feishu sent. Nothing is parsed into fixed types, so fields Feishu
leaves out or sends as `null` cannot make an event fail. For example, `sender.sender_id.user_id`
is `null` unless the app has the permission to read user IDs.

```toml
[dependencies]
feishu-ws = "0.1"
```

```rust
use feishu_ws::{Client, Config, Event, Handler, State};

struct Bot;

impl Handler for Bot {
    fn on_event(&self, event: Event) {
        // Return quickly: Feishu sends the event again if it is not acknowledged in 3 seconds.
        if event.event_type() == Some("im.message.receive_v1") {
            let sender = &event.payload["event"]["sender"]["sender_id"]["open_id"];
            println!("message from {sender}");
        }
    }

    fn on_state(&self, state: State) {
        println!("{state:?}");
    }
}

#[tokio::main]
async fn main() -> Result<(), feishu_ws::Error> {
    let client = Client::new(Config::new("cli_xxx", "app secret"))?;
    // Lark: Config::new(..).domain(feishu_ws::LARK_DOMAIN)
    client.run(&Bot, async { let _ = tokio::signal::ctrl_c().await; }).await
}
```

## Behaviour

The protocol follows the official SDKs:

- **Connecting.** The client gets a connection URL from `/callback/ws/endpoint` with the app's
  credentials, then opens the WebSocket.
- **Frames.** Messages are protobuf frames (`pbbp2.Frame`, see `feishu_ws::frame`).
- **Pings.** The client pings on the interval Feishu sets, and takes new settings from pongs.
- **Acknowledgements.** Every event is acknowledged (`{"code": 200}`) once your handler
  returns. Large events Feishu splits into several frames are put back together first.
- **Reconnecting.** After a connection ends, the client waits a random pause, then retries as
  often and as far apart as Feishu's settings say.
- **Rejection.** When Feishu refuses the app (wrong credentials, no permission, too many
  connections), `run` returns the error instead of retrying.

Feishu delivers each event to only one of an app's connections, and an app can hold at most
50. Long connections work for self-built (企业自建) apps; subscribe to the events you want and
choose "使用长连接接收事件" in the app's event settings.

## TLS

The client uses the process's default rustls crypto provider when one is installed, and
otherwise the provider its features pick: `aws-lc-rs` (default) or `ring`. Because it names a
provider itself, a dependency graph that enables both cannot leave rustls unable to choose.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
