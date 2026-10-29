//! The frames of the long connection: protobuf messages `pbbp2.Frame` and `pbbp2.Header`,
//! as the official SDKs define them.

/// A key-value header of a frame.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Header {
    #[prost(string, required, tag = "1")]
    pub key: String,
    #[prost(string, required, tag = "2")]
    pub value: String,
}

/// One binary WebSocket message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Frame {
    #[prost(uint64, required, tag = "1")]
    pub seq_id: u64,
    #[prost(uint64, required, tag = "2")]
    pub log_id: u64,
    #[prost(int32, required, tag = "3")]
    pub service: i32,
    /// [`CONTROL`] or [`DATA`].
    #[prost(int32, required, tag = "4")]
    pub method: i32,
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<Header>,
    #[prost(string, optional, tag = "6")]
    pub payload_encoding: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub payload_type: Option<String>,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub payload: Option<Vec<u8>>,
    #[prost(string, optional, tag = "9")]
    pub log_id_new: Option<String>,
}

/// Pings and pongs.
pub const CONTROL: i32 = 0;
/// Events and card callbacks.
pub const DATA: i32 = 1;

pub const TYPE: &str = "type";
pub const MESSAGE_ID: &str = "message_id";
pub const TRACE_ID: &str = "trace_id";
pub const SUM: &str = "sum";
pub const SEQ: &str = "seq";
pub const BIZ_RT: &str = "biz_rt";

impl Frame {
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.value.as_str())
    }

    /// The ping the client sends every ping interval.
    pub fn ping(service: i32) -> Self {
        Frame {
            service,
            method: CONTROL,
            headers: vec![Header {
                key: TYPE.into(),
                value: "ping".into(),
            }],
            ..Frame::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn frames_round_trip() {
        let mut frame = Frame::ping(7);
        frame.payload = Some(b"{}".to_vec());
        let decoded = Frame::decode(frame.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(decoded.header(TYPE), Some("ping"));
        assert_eq!(decoded.header(SUM), None);
    }

    #[test]
    fn a_ping_encodes_like_the_official_sdks() {
        // Python: Frame(SeqID=0, LogID=0, service=7, method=0, headers=[type: ping]).SerializeToString()
        let expected = [
            0x08, 0x00, 0x10, 0x00, 0x18, 0x07, 0x20, 0x00, 0x2a, 0x0c, 0x0a, 0x04, b't', b'y',
            b'p', b'e', 0x12, 0x04, b'p', b'i', b'n', b'g',
        ];
        assert_eq!(Frame::ping(7).encode_to_vec(), expected);
    }
}
