//! The client against a fake Feishu: the endpoint over HTTP, then a WebSocket that pushes
//! an event, checks the acknowledgement and the pings, and drops the connection.

use std::sync::Mutex;
use std::time::Duration;

use feishu_ws::frame::{self, Frame, Header};
use feishu_ws::{Client, Config, Error, Event, Handler, State};
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Recorder {
    events: Mutex<Vec<Event>>,
    states: mpsc::UnboundedSender<State>,
}

impl Handler for Recorder {
    fn on_event(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }

    fn on_state(&self, state: State) {
        let _ = self.states.send(state);
    }
}

async fn endpoint(server: &MockServer, url: &str) {
    Mock::given(method("POST"))
        .and(path("/callback/ws/endpoint"))
        .and(header("locale", "zh"))
        .and(body_partial_json(json!({"AppID": "cli_test", "AppSecret": "secret"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "msg": "ok",
            "data": {
                "URL": url,
                "ClientConfig": {"ReconnectCount": -1, "ReconnectInterval": 1, "ReconnectNonce": 0, "PingInterval": 1}
            }
        })))
        .mount(server)
        .await;
}

fn event_frame(message_id: &str, sum: usize, seq: usize, payload: &[u8]) -> Vec<u8> {
    let header = |key: &str, value: &str| Header {
        key: key.into(),
        value: value.into(),
    };
    Frame {
        service: 7,
        method: frame::DATA,
        headers: vec![
            header(frame::TYPE, "event"),
            header(frame::MESSAGE_ID, message_id),
            header(frame::TRACE_ID, "trace-1"),
            header(frame::SUM, &sum.to_string()),
            header(frame::SEQ, &seq.to_string()),
        ],
        payload: Some(payload.to_vec()),
        ..Frame::default()
    }
    .encode_to_vec()
}

#[tokio::test]
async fn receives_acknowledges_pings_and_reconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = MockServer::start().await;
    endpoint(
        &http,
        &format!("ws://{address}/ws?device_id=d1&service_id=7&access_key=k"),
    )
    .await;

    // What Feishu sends when the app may not read user IDs: `user_id` is null.
    let event = json!({
        "schema": "2.0",
        "header": {"event_id": "e1", "event_type": "im.message.receive_v1", "app_id": "cli_test"},
        "event": {
            "sender": {"sender_id": {"open_id": "ou_1", "union_id": "on_1", "user_id": null}, "sender_type": "user"},
            "message": {"message_id": "om_1", "chat_type": "p2p", "message_type": "text", "content": "{\"text\":\"hi\"}"}
        }
    });
    let bytes = event.to_string().into_bytes();
    let (first, rest) = bytes.split_at(bytes.len() / 2);
    let (first, rest) = (first.to_vec(), rest.to_vec());

    let (reconnected_tx, reconnected_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        // The event, split in two frames as Feishu does with large ones.
        socket
            .send(Message::Binary(event_frame("msg-1", 2, 0, &first).into()))
            .await
            .unwrap();
        socket
            .send(Message::Binary(event_frame("msg-1", 2, 1, &rest).into()))
            .await
            .unwrap();
        let (mut pinged, mut acknowledged) = (false, None);
        while !(pinged && acknowledged.is_some()) {
            let Some(Ok(Message::Binary(data))) = socket.next().await else {
                panic!("the client closed the connection");
            };
            let frame = Frame::decode(data.as_ref()).unwrap();
            match frame.method {
                frame::CONTROL => {
                    assert_eq!(
                        (frame.header(frame::TYPE), frame.service),
                        (Some("ping"), 7)
                    );
                    pinged = true;
                }
                _ => acknowledged = Some(frame),
            }
        }
        let ack = acknowledged.unwrap();
        assert_eq!(ack.header(frame::MESSAGE_ID), Some("msg-1"));
        assert!(ack.header(frame::BIZ_RT).is_some());
        let body: Value = serde_json::from_slice(ack.payload.as_deref().unwrap()).unwrap();
        assert_eq!(body, json!({"code": 200}));
        socket.close(None).await.unwrap();

        // The client comes back.
        let (stream, _) = listener.accept().await.unwrap();
        let _socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = reconnected_tx.send(());
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (states_tx, mut states_rx) = mpsc::unbounded_channel();
    let recorder = Recorder {
        events: Mutex::default(),
        states: states_tx,
    };
    let client = Client::new(Config::new("cli_test", "secret").domain(http.uri())).unwrap();
    let run = client.run(&recorder, async {
        let _ = reconnected_rx.await;
    });
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the client reconnected")
        .unwrap();
    server.abort();

    let events = recorder.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].message_id, "msg-1");
    assert_eq!(events[0].event_type(), Some("im.message.receive_v1"));
    assert_eq!(events[0].payload, event);

    let mut states = Vec::new();
    while let Ok(state) = states_rx.try_recv() {
        states.push(state);
    }
    assert_eq!(states[..2], [State::Connecting, State::Connected]);
    assert!(matches!(states[2], State::Disconnected(_)), "{states:?}");
    assert_eq!(states[3..5], [State::Connecting, State::Connected]);
}

#[tokio::test]
async fn refused_apps_stop_instead_of_retrying() {
    let http = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/callback/ws/endpoint"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"code": 10003, "msg": "invalid app secret"})),
        )
        .expect(1)
        .mount(&http)
        .await;
    let (states, _) = mpsc::unbounded_channel();
    let recorder = Recorder {
        events: Mutex::default(),
        states,
    };
    let client = Client::new(Config::new("cli_test", "wrong").domain(http.uri())).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        client.run(&recorder, std::future::pending()),
    )
    .await
    .expect("the client stopped");
    match result {
        Err(Error::Rejected { code, message }) => {
            assert_eq!((code, message.as_str()), (10003, "invalid app secret"))
        }
        other => panic!("{other:?}"),
    }
}
