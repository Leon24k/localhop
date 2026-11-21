use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Compact binary frame tags for zero-overhead multiplexed tunneling.
///
/// Hot-path body frames (`RequestBody`, `ResponseBody`) carry a 9-byte header
/// (`1B tag + 8B req_id`) followed immediately by raw payload bytes—zero JSON
/// or base64 serialization overhead.
pub mod tag {
    pub const HELLO: u8 = 0x01;
    pub const WELCOME: u8 = 0x02;
    pub const ERROR: u8 = 0x03;
    pub const REQUEST_HEAD: u8 = 0x04;
    pub const REQUEST_BODY: u8 = 0x05;
    pub const REQUEST_END: u8 = 0x06;
    pub const RESPONSE_HEAD: u8 = 0x07;
    pub const RESPONSE_BODY: u8 = 0x08;
    pub const RESPONSE_END: u8 = 0x09;
    pub const PING: u8 = 0x0A;
    pub const PONG: u8 = 0x0B;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub requested_id: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Welcome {
    pub tunnel_id: String,
    pub public_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolErrorMsg {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHead {
    pub req_id: u64,
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseHead {
    pub req_id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// Time spent waiting for the local upstream app in microseconds.
    pub local_latency_us: u64,
}

#[derive(Debug, Clone)]
pub enum Frame {
    Hello(Hello),
    Welcome(Welcome),
    Error(ProtocolErrorMsg),
    RequestHead(RequestHead),
    RequestBody { req_id: u64, chunk: Bytes },
    RequestEnd { req_id: u64 },
    ResponseHead(ResponseHead),
    ResponseBody { req_id: u64, chunk: Bytes },
    ResponseEnd { req_id: u64 },
    Ping { ts_us: u64 },
    Pong { ts_us: u64 },
}

#[derive(Debug)]
pub enum CodecError {
    EmptyFrame,
    UnknownTag(u8),
    TruncatedFrame,
    Json(serde_json::Error),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::EmptyFrame => write!(f, "empty binary frame"),
            CodecError::UnknownTag(t) => write!(f, "unknown frame tag: 0x{t:02x}"),
            CodecError::TruncatedFrame => write!(f, "truncated binary frame"),
            CodecError::Json(e) => write!(f, "json codec error: {e}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<serde_json::Error> for CodecError {
    fn from(err: serde_json::Error) -> Self {
        CodecError::Json(err)
    }
}

impl Frame {
    pub fn encode(&self) -> Result<Bytes, CodecError> {
        match self {
            Frame::Hello(v) => encode_json(tag::HELLO, v),
            Frame::Welcome(v) => encode_json(tag::WELCOME, v),
            Frame::Error(v) => encode_json(tag::ERROR, v),
            Frame::RequestHead(v) => encode_json(tag::REQUEST_HEAD, v),
            Frame::RequestBody { req_id, chunk } => Ok(encode_body(tag::REQUEST_BODY, *req_id, chunk)),
            Frame::RequestEnd { req_id } => Ok(encode_u64(tag::REQUEST_END, *req_id)),
            Frame::ResponseHead(v) => encode_json(tag::RESPONSE_HEAD, v),
            Frame::ResponseBody { req_id, chunk } => Ok(encode_body(tag::RESPONSE_BODY, *req_id, chunk)),
            Frame::ResponseEnd { req_id } => Ok(encode_u64(tag::RESPONSE_END, *req_id)),
            Frame::Ping { ts_us } => Ok(encode_u64(tag::PING, *ts_us)),
            Frame::Pong { ts_us } => Ok(encode_u64(tag::PONG, *ts_us)),
        }
    }

    pub fn decode(raw: Bytes) -> Result<Self, CodecError> {
        if raw.is_empty() {
            return Err(CodecError::EmptyFrame);
        }
        let frame_tag = raw[0];
        let payload = raw.slice(1..);

        match frame_tag {
            tag::HELLO => Ok(Frame::Hello(serde_json::from_slice(&payload)?)),
            tag::WELCOME => Ok(Frame::Welcome(serde_json::from_slice(&payload)?)),
            tag::ERROR => Ok(Frame::Error(serde_json::from_slice(&payload)?)),
            tag::REQUEST_HEAD => Ok(Frame::RequestHead(serde_json::from_slice(&payload)?)),
            tag::REQUEST_BODY => {
                let (req_id, chunk) = decode_body(payload)?;
                Ok(Frame::RequestBody { req_id, chunk })
            }
            tag::REQUEST_END => Ok(Frame::RequestEnd {
                req_id: decode_u64(&payload)?,
            }),
            tag::RESPONSE_HEAD => Ok(Frame::ResponseHead(serde_json::from_slice(&payload)?)),
            tag::RESPONSE_BODY => {
                let (req_id, chunk) = decode_body(payload)?;
                Ok(Frame::ResponseBody { req_id, chunk })
            }
            tag::RESPONSE_END => Ok(Frame::ResponseEnd {
                req_id: decode_u64(&payload)?,
            }),
            tag::PING => Ok(Frame::Ping {
                ts_us: decode_u64(&payload)?,
            }),
            tag::PONG => Ok(Frame::Pong {
                ts_us: decode_u64(&payload)?,
            }),
            other => Err(CodecError::UnknownTag(other)),
        }
    }
}

fn encode_json<T: Serialize>(frame_tag: u8, value: &T) -> Result<Bytes, CodecError> {
    let json = serde_json::to_vec(value)?;
    let mut buf = BytesMut::with_capacity(1 + json.len());
    buf.put_u8(frame_tag);
    buf.put_slice(&json);
    Ok(buf.freeze())
}

fn encode_body(frame_tag: u8, req_id: u64, chunk: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(9 + chunk.len());
    buf.put_u8(frame_tag);
    buf.put_u64(req_id);
    buf.put_slice(chunk);
    buf.freeze()
}

fn encode_u64(frame_tag: u8, val: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(frame_tag);
    buf.put_u64(val);
    buf.freeze()
}

fn decode_u64(payload: &[u8]) -> Result<u64, CodecError> {
    if payload.len() < 8 {
        return Err(CodecError::TruncatedFrame);
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&payload[..8]);
    Ok(u64::from_be_bytes(arr))
}

fn decode_body(payload: Bytes) -> Result<(u64, Bytes), CodecError> {
    if payload.len() < 8 {
        return Err(CodecError::TruncatedFrame);
    }
    let req_id = decode_u64(&payload[..8])?;
    let chunk = payload.slice(8..);
    Ok((req_id, chunk))
}

/// Headers that must not be blindly forwarded across HTTP hops.
pub fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

/// Validates a custom tunnel ID (lowercase alphanumeric + hyphens, 3..=32 chars).
pub fn is_valid_tunnel_id(id: &str) -> bool {
    let len = id.len();
    if !(3..=32).contains(&len) {
        return false;
    }
    if id.starts_with('-') || id.ends_with('-') {
        return false;
    }
    id.bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_body_frame_zero_copy() {
        let payload = Bytes::from_static(b"hello localhop");
        let encoded = Frame::RequestBody {
            req_id: 42,
            chunk: payload.clone(),
        }
        .encode()
        .unwrap();

        assert_eq!(encoded.len(), 9 + payload.len());
        match Frame::decode(encoded).unwrap() {
            Frame::RequestBody { req_id, chunk } => {
                assert_eq!(req_id, 42);
                assert_eq!(chunk, payload);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn roundtrip_request_head() {
        let head = RequestHead {
            req_id: 7,
            method: "POST".into(),
            path_and_query: "/webhooks/stripe?v=1".into(),
            headers: vec![("content-type".into(), "application/json".into())],
        };
        let encoded = Frame::RequestHead(head.clone()).encode().unwrap();
        match Frame::decode(encoded).unwrap() {
            Frame::RequestHead(decoded) => {
                assert_eq!(decoded.req_id, head.req_id);
                assert_eq!(decoded.method, head.method);
                assert_eq!(decoded.path_and_query, head.path_and_query);
                assert_eq!(decoded.headers, head.headers);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn validates_tunnel_ids() {
        assert!(is_valid_tunnel_id("my-app-1"));
        assert!(is_valid_tunnel_id("abc"));
        assert!(!is_valid_tunnel_id("ab"));
        assert!(!is_valid_tunnel_id("-bad"));
        assert!(!is_valid_tunnel_id("BadUpper"));
        assert!(!is_valid_tunnel_id("no/slash"));
    }
}
