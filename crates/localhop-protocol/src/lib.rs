use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum payload size (16 KiB) eligible for single-frame inline transport.
pub const INLINE_BODY_LIMIT: usize = 16 * 1024;

/// Compact binary frame tags for zero-overhead multiplexed tunneling.
///
/// Control handshake frames (`Hello`, `Welcome`, `Error`, `WsOpen`, `WsOpened`)
/// use JSON. All hot-path HTTP and WebSocket frames (`RequestHead`, `RequestInline`,
/// `RequestBody`, `RequestEnd`, `ResponseHead`, `ResponseInline`, `ResponseBody`,
/// `ResponseEnd`, `WsMessage`, `WsClose`, `Ping`, `Pong`, `Cancel`) use pure
/// binary encoding with zero JSON serialization or base64 overhead.
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
    pub const WS_OPEN: u8 = 0x0C;
    pub const WS_OPENED: u8 = 0x0D;
    pub const WS_MESSAGE: u8 = 0x0E;
    pub const WS_CLOSE: u8 = 0x0F;
    pub const CANCEL: u8 = 0x10;
    pub const REQUEST_INLINE: u8 = 0x11;
    pub const RESPONSE_INLINE: u8 = 0x12;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub requested_id: Option<String>,
    pub token: Option<String>,
    /// Optional HTTP Basic Auth credential in `user:pass` format enforced at the relay edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic_auth: Option<String>,
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

#[derive(Debug, Clone)]
pub struct RequestHead {
    pub req_id: u64,
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    /// When `false`, the request has no body and no `RequestEnd` frame will follow.
    pub has_body: bool,
}

#[derive(Debug, Clone)]
pub struct RequestInline {
    pub req_id: u64,
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct ResponseHead {
    pub req_id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// Time spent waiting for the local upstream app in microseconds.
    pub local_latency_us: u64,
}

#[derive(Debug, Clone)]
pub struct ResponseInline {
    pub req_id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// Time spent waiting for the local upstream app in microseconds.
    pub local_latency_us: u64,
    pub body: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOpen {
    pub req_id: u64,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOpened {
    pub req_id: u64,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Frame {
    Hello(Hello),
    Welcome(Welcome),
    Error(ProtocolErrorMsg),
    RequestHead(RequestHead),
    RequestInline(RequestInline),
    RequestBody {
        req_id: u64,
        chunk: Bytes,
    },
    RequestEnd {
        req_id: u64,
    },
    ResponseHead(ResponseHead),
    ResponseInline(ResponseInline),
    ResponseBody {
        req_id: u64,
        chunk: Bytes,
    },
    ResponseEnd {
        req_id: u64,
    },
    Ping {
        ts_us: u64,
    },
    Pong {
        ts_us: u64,
    },
    WsOpen(WsOpen),
    WsOpened(WsOpened),
    WsMessage {
        req_id: u64,
        is_binary: bool,
        payload: Bytes,
    },
    WsClose {
        req_id: u64,
    },
    /// Sent by the relay when the public client disconnects before a stream completes.
    Cancel {
        req_id: u64,
    },
}

#[derive(Debug)]
pub enum CodecError {
    EmptyFrame,
    UnknownTag(u8),
    TruncatedFrame,
    InvalidUtf8,
    LimitExceeded,
    Json(serde_json::Error),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::EmptyFrame => write!(f, "empty binary frame"),
            CodecError::UnknownTag(t) => write!(f, "unknown frame tag: 0x{t:02x}"),
            CodecError::TruncatedFrame => write!(f, "truncated binary frame"),
            CodecError::InvalidUtf8 => write!(f, "invalid UTF-8 in binary header"),
            CodecError::LimitExceeded => write!(f, "header or field length exceeds u16 limit"),
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
            Frame::RequestHead(v) => encode_request_head(v),
            Frame::RequestInline(v) => encode_request_inline(v),
            Frame::RequestBody { req_id, chunk } => {
                Ok(encode_body(tag::REQUEST_BODY, *req_id, chunk))
            }
            Frame::RequestEnd { req_id } => Ok(encode_u64(tag::REQUEST_END, *req_id)),
            Frame::ResponseHead(v) => encode_response_head(v),
            Frame::ResponseInline(v) => encode_response_inline(v),
            Frame::ResponseBody { req_id, chunk } => {
                Ok(encode_body(tag::RESPONSE_BODY, *req_id, chunk))
            }
            Frame::ResponseEnd { req_id } => Ok(encode_u64(tag::RESPONSE_END, *req_id)),
            Frame::Ping { ts_us } => Ok(encode_u64(tag::PING, *ts_us)),
            Frame::Pong { ts_us } => Ok(encode_u64(tag::PONG, *ts_us)),
            Frame::WsOpen(v) => encode_json(tag::WS_OPEN, v),
            Frame::WsOpened(v) => encode_json(tag::WS_OPENED, v),
            Frame::WsMessage {
                req_id,
                is_binary,
                payload,
            } => Ok(encode_ws_message(*req_id, *is_binary, payload)),
            Frame::WsClose { req_id } => Ok(encode_u64(tag::WS_CLOSE, *req_id)),
            Frame::Cancel { req_id } => Ok(encode_u64(tag::CANCEL, *req_id)),
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
            tag::REQUEST_HEAD => Ok(Frame::RequestHead(decode_request_head(&payload)?)),
            tag::REQUEST_INLINE => Ok(Frame::RequestInline(decode_request_inline(payload)?)),
            tag::REQUEST_BODY => {
                let (req_id, chunk) = decode_body(payload)?;
                Ok(Frame::RequestBody { req_id, chunk })
            }
            tag::REQUEST_END => Ok(Frame::RequestEnd {
                req_id: decode_u64(&payload)?,
            }),
            tag::RESPONSE_HEAD => Ok(Frame::ResponseHead(decode_response_head(&payload)?)),
            tag::RESPONSE_INLINE => Ok(Frame::ResponseInline(decode_response_inline(payload)?)),
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
            tag::WS_OPEN => Ok(Frame::WsOpen(serde_json::from_slice(&payload)?)),
            tag::WS_OPENED => Ok(Frame::WsOpened(serde_json::from_slice(&payload)?)),
            tag::WS_MESSAGE => {
                if payload.len() < 9 {
                    return Err(CodecError::TruncatedFrame);
                }
                let req_id = decode_u64(&payload[..8])?;
                let is_binary = payload[8] != 0;
                let msg_payload = payload.slice(9..);
                Ok(Frame::WsMessage {
                    req_id,
                    is_binary,
                    payload: msg_payload,
                })
            }
            tag::WS_CLOSE => Ok(Frame::WsClose {
                req_id: decode_u64(&payload)?,
            }),
            tag::CANCEL => Ok(Frame::Cancel {
                req_id: decode_u64(&payload)?,
            }),
            other => Err(CodecError::UnknownTag(other)),
        }
    }
}

fn headers_encoded_len(headers: &[(String, String)]) -> usize {
    2 + headers
        .iter()
        .map(|(k, v)| 4 + k.len() + v.len())
        .sum::<usize>()
}

fn put_str_u16(buf: &mut BytesMut, s: &str) -> Result<(), CodecError> {
    let len = u16::try_from(s.len()).map_err(|_| CodecError::LimitExceeded)?;
    buf.put_u16(len);
    buf.put_slice(s.as_bytes());
    Ok(())
}

fn put_headers(buf: &mut BytesMut, headers: &[(String, String)]) -> Result<(), CodecError> {
    let count = u16::try_from(headers.len()).map_err(|_| CodecError::LimitExceeded)?;
    buf.put_u16(count);
    for (k, v) in headers {
        put_str_u16(buf, k)?;
        put_str_u16(buf, v)?;
    }
    Ok(())
}

struct Cursor<'a> {
    slice: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(slice: &'a [u8]) -> Self {
        Self { slice, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, CodecError> {
        if self.pos >= self.slice.len() {
            return Err(CodecError::TruncatedFrame);
        }
        let b = self.slice[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_u16(&mut self) -> Result<u16, CodecError> {
        if self.pos + 2 > self.slice.len() {
            return Err(CodecError::TruncatedFrame);
        }
        let v = u16::from_be_bytes([self.slice[self.pos], self.slice[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, CodecError> {
        if self.pos + 4 > self.slice.len() {
            return Err(CodecError::TruncatedFrame);
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.slice[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_be_bytes(arr))
    }

    fn read_u64(&mut self) -> Result<u64, CodecError> {
        if self.pos + 8 > self.slice.len() {
            return Err(CodecError::TruncatedFrame);
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.slice[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_be_bytes(arr))
    }

    fn read_str_u16(&mut self) -> Result<String, CodecError> {
        let len = usize::from(self.read_u16()?);
        if self.pos + len > self.slice.len() {
            return Err(CodecError::TruncatedFrame);
        }
        let s = std::str::from_utf8(&self.slice[self.pos..self.pos + len])
            .map_err(|_| CodecError::InvalidUtf8)?;
        self.pos += len;
        Ok(s.to_string())
    }

    fn read_headers(&mut self) -> Result<Vec<(String, String)>, CodecError> {
        let count = usize::from(self.read_u16()?);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let k = self.read_str_u16()?;
            let v = self.read_str_u16()?;
            out.push((k, v));
        }
        Ok(out)
    }
}

fn encode_request_head(v: &RequestHead) -> Result<Bytes, CodecError> {
    let cap = 1
        + 8
        + 1
        + 2
        + v.method.len()
        + 2
        + v.path_and_query.len()
        + headers_encoded_len(&v.headers);
    let mut buf = BytesMut::with_capacity(cap);
    buf.put_u8(tag::REQUEST_HEAD);
    buf.put_u64(v.req_id);
    buf.put_u8(u8::from(v.has_body));
    put_str_u16(&mut buf, &v.method)?;
    put_str_u16(&mut buf, &v.path_and_query)?;
    put_headers(&mut buf, &v.headers)?;
    Ok(buf.freeze())
}

fn decode_request_head(payload: &[u8]) -> Result<RequestHead, CodecError> {
    let mut c = Cursor::new(payload);
    let req_id = c.read_u64()?;
    let has_body = c.read_u8()? != 0;
    let method = c.read_str_u16()?;
    let path_and_query = c.read_str_u16()?;
    let headers = c.read_headers()?;
    Ok(RequestHead {
        req_id,
        method,
        path_and_query,
        headers,
        has_body,
    })
}

fn encode_request_inline(v: &RequestInline) -> Result<Bytes, CodecError> {
    let body_len = u32::try_from(v.body.len()).map_err(|_| CodecError::LimitExceeded)?;
    let cap = 1
        + 8
        + 2
        + v.method.len()
        + 2
        + v.path_and_query.len()
        + headers_encoded_len(&v.headers)
        + 4
        + v.body.len();
    let mut buf = BytesMut::with_capacity(cap);
    buf.put_u8(tag::REQUEST_INLINE);
    buf.put_u64(v.req_id);
    put_str_u16(&mut buf, &v.method)?;
    put_str_u16(&mut buf, &v.path_and_query)?;
    put_headers(&mut buf, &v.headers)?;
    buf.put_u32(body_len);
    buf.put_slice(&v.body);
    Ok(buf.freeze())
}

fn decode_request_inline(payload: Bytes) -> Result<RequestInline, CodecError> {
    let (req_id, method, path_and_query, headers, body_start, body_len) = {
        let mut c = Cursor::new(&payload);
        let req_id = c.read_u64()?;
        let method = c.read_str_u16()?;
        let path_and_query = c.read_str_u16()?;
        let headers = c.read_headers()?;
        let body_len = c.read_u32()? as usize;
        if c.pos + body_len > payload.len() {
            return Err(CodecError::TruncatedFrame);
        }
        (req_id, method, path_and_query, headers, c.pos, body_len)
    };
    let body = payload.slice(body_start..body_start + body_len);
    Ok(RequestInline {
        req_id,
        method,
        path_and_query,
        headers,
        body,
    })
}

fn encode_response_head(v: &ResponseHead) -> Result<Bytes, CodecError> {
    let cap = 1 + 8 + 2 + 8 + headers_encoded_len(&v.headers);
    let mut buf = BytesMut::with_capacity(cap);
    buf.put_u8(tag::RESPONSE_HEAD);
    buf.put_u64(v.req_id);
    buf.put_u16(v.status);
    buf.put_u64(v.local_latency_us);
    put_headers(&mut buf, &v.headers)?;
    Ok(buf.freeze())
}

fn decode_response_head(payload: &[u8]) -> Result<ResponseHead, CodecError> {
    let mut c = Cursor::new(payload);
    let req_id = c.read_u64()?;
    let status = c.read_u16()?;
    let local_latency_us = c.read_u64()?;
    let headers = c.read_headers()?;
    Ok(ResponseHead {
        req_id,
        status,
        headers,
        local_latency_us,
    })
}

fn encode_response_inline(v: &ResponseInline) -> Result<Bytes, CodecError> {
    let body_len = u32::try_from(v.body.len()).map_err(|_| CodecError::LimitExceeded)?;
    let cap = 1 + 8 + 2 + 8 + headers_encoded_len(&v.headers) + 4 + v.body.len();
    let mut buf = BytesMut::with_capacity(cap);
    buf.put_u8(tag::RESPONSE_INLINE);
    buf.put_u64(v.req_id);
    buf.put_u16(v.status);
    buf.put_u64(v.local_latency_us);
    put_headers(&mut buf, &v.headers)?;
    buf.put_u32(body_len);
    buf.put_slice(&v.body);
    Ok(buf.freeze())
}

fn decode_response_inline(payload: Bytes) -> Result<ResponseInline, CodecError> {
    let (req_id, status, local_latency_us, headers, body_start, body_len) = {
        let mut c = Cursor::new(&payload);
        let req_id = c.read_u64()?;
        let status = c.read_u16()?;
        let local_latency_us = c.read_u64()?;
        let headers = c.read_headers()?;
        let body_len = c.read_u32()? as usize;
        if c.pos + body_len > payload.len() {
            return Err(CodecError::TruncatedFrame);
        }
        (req_id, status, local_latency_us, headers, c.pos, body_len)
    };
    let body = payload.slice(body_start..body_start + body_len);
    Ok(ResponseInline {
        req_id,
        status,
        headers,
        local_latency_us,
        body,
    })
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

fn encode_ws_message(req_id: u64, is_binary: bool, payload: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(10 + payload.len());
    buf.put_u8(tag::WS_MESSAGE);
    buf.put_u64(req_id);
    buf.put_u8(u8::from(is_binary));
    buf.put_slice(payload);
    buf.freeze()
}

#[inline]
fn encode_u64(frame_tag: u8, val: u64) -> Bytes {
    let mut arr = [0u8; 9];
    arr[0] = frame_tag;
    arr[1..9].copy_from_slice(&val.to_be_bytes());
    Bytes::copy_from_slice(&arr)
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

/// Headers that must not be blindly forwarded across HTTP hops (zero heap allocations).
#[inline]
pub fn is_hop_by_hop_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("keep-alive")
        || name.eq_ignore_ascii_case("proxy-authenticate")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("te")
        || name.eq_ignore_ascii_case("trailers")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("upgrade")
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

/// Constant-time equality check to avoid timing side-channels on auth tokens & credentials.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Builds the expected `Authorization` header value (`Basic <base64>`) for a `user:pass` string.
pub fn format_basic_auth_header(user_pass: &str) -> String {
    format!("Basic {}", base64_encode(user_pass.as_bytes()))
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;

        out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
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
    fn roundtrip_binary_request_and_response_heads() {
        let head = RequestHead {
            req_id: 7,
            method: "POST".into(),
            path_and_query: "/webhooks/stripe?v=1".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            has_body: true,
        };
        let encoded = Frame::RequestHead(head.clone()).encode().unwrap();
        match Frame::decode(encoded).unwrap() {
            Frame::RequestHead(decoded) => {
                assert_eq!(decoded.req_id, head.req_id);
                assert_eq!(decoded.method, head.method);
                assert_eq!(decoded.path_and_query, head.path_and_query);
                assert_eq!(decoded.headers, head.headers);
                assert!(decoded.has_body);
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let rhead = ResponseHead {
            req_id: 7,
            status: 201,
            headers: vec![("x-test".into(), "ok".into())],
            local_latency_us: 420,
        };
        let encoded = Frame::ResponseHead(rhead.clone()).encode().unwrap();
        match Frame::decode(encoded).unwrap() {
            Frame::ResponseHead(decoded) => {
                assert_eq!(decoded.req_id, 7);
                assert_eq!(decoded.status, 201);
                assert_eq!(decoded.local_latency_us, 420);
                assert_eq!(decoded.headers, rhead.headers);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn roundtrip_inline_single_frame_http() {
        let req = Frame::RequestInline(RequestInline {
            req_id: 11,
            method: "POST".into(),
            path_and_query: "/hooks".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body: Bytes::from_static(br#"{"ok":true}"#),
        })
        .encode()
        .unwrap();
        match Frame::decode(req).unwrap() {
            Frame::RequestInline(r) => {
                assert_eq!(r.req_id, 11);
                assert_eq!(r.method, "POST");
                assert_eq!(r.path_and_query, "/hooks");
                assert_eq!(r.body.as_ref(), br#"{"ok":true}"#);
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let resp = Frame::ResponseInline(ResponseInline {
            req_id: 11,
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            local_latency_us: 280,
            body: Bytes::from_static(br#"{"received":true}"#),
        })
        .encode()
        .unwrap();
        match Frame::decode(resp).unwrap() {
            Frame::ResponseInline(r) => {
                assert_eq!(r.req_id, 11);
                assert_eq!(r.status, 200);
                assert_eq!(r.local_latency_us, 280);
                assert_eq!(r.body.as_ref(), br#"{"received":true}"#);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn roundtrip_ws_frames() {
        let open = Frame::WsOpen(WsOpen {
            req_id: 99,
            path_and_query: "/ws/chat?room=1".into(),
            headers: vec![("origin".into(), "https://example.com".into())],
        })
        .encode()
        .unwrap();
        assert!(matches!(Frame::decode(open).unwrap(), Frame::WsOpen(w) if w.req_id == 99));

        let msg = Frame::WsMessage {
            req_id: 99,
            is_binary: true,
            payload: Bytes::from_static(&[1, 2, 3, 4]),
        }
        .encode()
        .unwrap();
        assert_eq!(msg.len(), 14);
        match Frame::decode(msg).unwrap() {
            Frame::WsMessage {
                req_id,
                is_binary,
                payload,
            } => {
                assert_eq!(req_id, 99);
                assert!(is_binary);
                assert_eq!(payload.as_ref(), &[1, 2, 3, 4]);
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let close = Frame::WsClose { req_id: 99 }.encode().unwrap();
        assert_eq!(close.len(), 9);
        assert!(matches!(
            Frame::decode(close).unwrap(),
            Frame::WsClose { req_id: 99 }
        ));

        let cancel = Frame::Cancel { req_id: 77 }.encode().unwrap();
        assert_eq!(cancel.len(), 9);
        assert!(matches!(
            Frame::decode(cancel).unwrap(),
            Frame::Cancel { req_id: 77 }
        ));
    }

    #[test]
    fn rejects_malformed_frames() {
        assert!(matches!(
            Frame::decode(Bytes::new()),
            Err(CodecError::EmptyFrame)
        ));
        assert!(matches!(
            Frame::decode(Bytes::from_static(&[0xFF])),
            Err(CodecError::UnknownTag(0xFF))
        ));
        assert!(matches!(
            Frame::decode(Bytes::from_static(&[tag::REQUEST_END, 1, 2, 3])),
            Err(CodecError::TruncatedFrame)
        ));
        assert!(matches!(
            Frame::decode(Bytes::from_static(&[
                tag::WS_MESSAGE,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1
            ])),
            Err(CodecError::TruncatedFrame)
        ));
    }

    #[test]
    fn validates_tunnel_ids() {
        assert!(is_valid_tunnel_id("my-app-1"));
        assert!(is_valid_tunnel_id("abc"));
        assert!(!is_valid_tunnel_id("ab"));
        assert!(!is_valid_tunnel_id("-bad"));
        assert!(!is_valid_tunnel_id("bad-"));
        assert!(!is_valid_tunnel_id("BadUpper"));
        assert!(!is_valid_tunnel_id("no/slash"));
    }

    #[test]
    fn constant_time_and_basic_auth() {
        assert!(constant_time_eq(b"secret123", b"secret123"));
        assert!(!constant_time_eq(b"secret123", b"secret124"));
        assert!(!constant_time_eq(b"short", b"longer"));
        assert_eq!(
            format_basic_auth_header("admin:s3cr3t"),
            "Basic YWRtaW46czNjcjN0"
        );
        assert_eq!(format_basic_auth_header("a:b"), "Basic YTpi");
        assert_eq!(format_basic_auth_header("user:p"), "Basic dXNlcjpw");
    }
}
