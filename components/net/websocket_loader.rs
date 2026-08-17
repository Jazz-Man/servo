/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The websocket handler has three main responsibilities:
//! 1) initiate the initial HTTP connection and process the response
//! 2) ensure any DOM requests for sending/closing are propagated to the network
//! 3) transmit any incoming messages/closing to the DOM
//!
//! In order to accomplish this, the handler uses a long-running loop that selects
//! over events from the network and events from the DOM, using async/await to avoid
//! the need for a dedicated thread per websocket.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::StreamExt;
use headers::{Authorization, HeaderMapExt};
use http::HeaderMap;
use http::header::{self, HeaderName, HeaderValue};
use http::{Method, StatusCode};
use ipc_channel::ipc::IpcSender;
use log::{debug, trace, warn};
use net_traits::request::{RequestBuilder, RequestMode};
use net_traits::{MessageData, WebSocketDomAction, WebSocketNetworkEvent};
use servo_base::generic_channel::CallbackSetter;
use servo_url::ServoUrl;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tungstenite::Message;
use tungstenite::error::{Error, ProtocolError};
use wreq::redirect;
use wreq::ws::WebSocket;
use wreq::ws::message::CloseFrame as WreqCloseFrame;
use wreq::ws::message::Message as WreqMessage;

use crate::async_runtime::spawn_task;
use crate::http_loader::HttpState;
use crate::ws_map;

/// Create a Request object for the initial HTTP request.
/// This request contains `Origin`, `Sec-WebSocket-Protocol`, `Authorization`,
/// and `Cookie` headers as appropriate.
/// Returns an error if any header values are invalid or tungstenite cannot create
/// the desired request.
pub fn create_handshake_request(
    request: RequestBuilder,
    http_state: Arc<HttpState>,
) -> Result<net_traits::request::Request, Error> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Origin",
        HeaderValue::from_str(&request.url.origin().ascii_serialization())?,
    );

    // https://websockets.spec.whatwg.org/#concept-websocket-establish
    // 3./7. Append (`Upgrade`, `websocket`) and (`Sec-WebSocket-Version`, `13`).
    // wreq re-sets both inside `send()` — they ride here as request METADATA:
    // the engine's delegation seam detects WS handshakes by this pair
    // (client `policy::is_websocket_handshake`) to deny them before any
    // network work. Connection/Sec-WebSocket-Key stay wreq's alone.
    headers.insert("Upgrade", HeaderValue::from_static("websocket"));
    headers.insert("Sec-WebSocket-Version", HeaderValue::from_static("13"));

    // 8. For each protocol in protocols, combine (`Sec-WebSocket-Protocol`, protocol) in request’s
    // header list.
    // wreq joins a `protocols()` list with ", " — servo's "," join rides as
    // one plain header value instead.
    let protocols = match request.mode {
        RequestMode::WebSocket {
            ref protocols,
            original_url: _,
        } => protocols,
        _ => unreachable!("How did we get here?"),
    };
    if !protocols.is_empty() {
        let protocols = protocols.join(",");
        headers.insert("Sec-WebSocket-Protocol", HeaderValue::from_str(&protocols)?);
    }

    if let Some(cookie_list) = http_state
        .cookie_jar
        .cookies_for_url(request.url.as_url(), cookie_jar::CookieSource::Http)
    {
        headers.insert("Cookie", HeaderValue::from_str(&cookie_list)?);
    }

    if request.url.password().is_some() || request.url.username() != "" {
        headers.typed_insert(Authorization::basic(
            request.url.username(),
            request.url.password().unwrap_or(""),
        ));
    }
    Ok(request.headers(headers).build())
}

/// Process an HTTP response resulting from a WS handshake.
/// This ensures that any `Cookie` or HSTS headers are recognized.
/// Returns an error if the protocol selected by the handshake doesn't
/// match the list of provided protocols in the original request.
fn process_ws_response(
    http_state: &HttpState,
    response_headers: &HeaderMap,
    resource_url: &ServoUrl,
    protocols: &[String],
) -> Result<Option<String>, Error> {
    trace!("processing websocket http response for {}", resource_url);
    let mut protocol_in_use = None;
    if let Some(protocol_name) = response_headers.get("Sec-WebSocket-Protocol") {
        let protocol_name = protocol_name.to_str().unwrap_or("");
        if !protocols.is_empty() && !protocols.iter().any(|p| protocol_name == (*p)) {
            return Err(Error::Protocol(ProtocolError::InvalidHeader(Box::new(
                HeaderName::from_static("sec-websocket-protocol"),
            ))));
        }
        protocol_in_use = Some(protocol_name.to_string());
    }

    // TODO(eijebong): Replace thise once typed headers settled on a cookie impl
    for cookie in response_headers.get_all(header::SET_COOKIE) {
        let cookie_bytes = cookie.as_bytes();
        if !cookie_jar::StoredCookie::is_valid_name_or_value(cookie_bytes) {
            continue;
        }
        if let Ok(s) = std::str::from_utf8(cookie_bytes) {
            http_state
                .cookie_jar
                .set_cookie_string(resource_url.as_url(), s, cookie_jar::CookieSource::Http);
        }
    }

    http_state
        .hsts_list
        .write()
        .update_hsts_list_from_response(resource_url, response_headers);

    Ok(protocol_in_use)
}

#[derive(Debug)]
enum DomMsg {
    Send(Message),
    Close(Option<(u16, String)>),
}

/// Initialize a listener for DOM actions. These are routed from the IPC channel
/// to a tokio channel that the main WS client task uses to receive them.
fn setup_dom_listener(
    dom_action_receiver: CallbackSetter<WebSocketDomAction>,
    initiated_close: Arc<AtomicBool>,
) -> UnboundedReceiver<DomMsg> {
    let (sender, receiver) = unbounded_channel();

    dom_action_receiver.set_callback(move |message| {
        let dom_action = message.expect("Ws dom_action message to deserialize");
        trace!("handling WS DOM action: {:?}", dom_action);
        match dom_action {
            WebSocketDomAction::SendMessage(MessageData::Text(data)) => {
                if let Err(e) = sender.send(DomMsg::Send(Message::Text(data.into()))) {
                    warn!("Error sending websocket message: {:?}", e);
                }
            },
            WebSocketDomAction::SendMessage(MessageData::Binary(data)) => {
                if let Err(e) = sender.send(DomMsg::Send(Message::Binary(data.into()))) {
                    warn!("Error sending websocket message: {:?}", e);
                }
            },
            WebSocketDomAction::Close(code, reason) => {
                if initiated_close.fetch_or(true, Ordering::SeqCst) {
                    return;
                }
                let frame = code.map(move |c| (c, reason.unwrap_or_default()));
                if let Err(e) = sender.send(DomMsg::Close(frame)) {
                    warn!("Error closing websocket: {:?}", e);
                }
            },
        }
    });

    receiver
}

/// Listen for WS events from the DOM and the network until one side
/// closes the connection or an error occurs. Since this is an async
/// function that uses the select operation, it will run as a task
/// on the WS tokio runtime.
async fn run_ws_loop(
    mut dom_receiver: UnboundedReceiver<DomMsg>,
    resource_event_sender: IpcSender<WebSocketNetworkEvent>,
    mut stream: WebSocket,
) {
    loop {
        select! {
            dom_msg = dom_receiver.recv() => {
                trace!("processing dom msg: {:?}", dom_msg);
                let dom_msg = match dom_msg {
                    Some(msg) => msg,
                    None => break,
                };
                match dom_msg {
                    DomMsg::Send(m) => {
                        if let Err(e) = stream.send(ws_map::dom_to_wreq(m)).await {
                            warn!("error sending websocket message: {:?}", e);
                        }
                    },
                    DomMsg::Close(frame) => {
                        // A close frame sent through the normal sink keeps the
                        // loop running to receive the server's echo, exactly
                        // as `WebSocketStream::close` did.
                        if let Err(e) = stream.send(WreqMessage::Close(frame.map(|(code, reason)| {
                            WreqCloseFrame {
                                code: code.into(),
                                reason: reason.into(),
                            }
                        }))).await {
                            warn!("error closing websocket: {:?}", e);
                        }
                    },
                }
            }
            ws_msg = stream.next() => {
                trace!("processing WS stream: {:?}", ws_msg);
                let msg = match ws_msg {
                    Some(Ok(msg)) => msg,
                    Some(Err(e)) => {
                        warn!("Error in WebSocket communication: {:?}", e);
                        let _ = resource_event_sender.send(WebSocketNetworkEvent::Fail);
                        break;
                    },
                    None => {
                        warn!("Error in WebSocket communication");
                        let _ = resource_event_sender.send(WebSocketNetworkEvent::Fail);
                        break;
                    }
                };
                match msg {
                    WreqMessage::Close(frame) => {
                        let (reason, code) = match frame {
                            Some(frame) => (frame.reason, Some(u16::from(frame.code))),
                            None => ("".into(), None),
                        };
                        debug!("Websocket connection closing due to ({:?}) {}", code, reason);
                        let _ = resource_event_sender.send(WebSocketNetworkEvent::Close(
                            code,
                            reason.to_string(),
                        ));
                        break;
                    }

                    other => {
                        if let Some(message) = ws_map::wreq_to_message_data(other) {
                            if let Err(e) = resource_event_sender
                                .send(WebSocketNetworkEvent::MessageReceived(message))
                            {
                                warn!("Error sending websocket notification: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Everything the fetch pipeline needs from a completed WS handshake: the
/// status and headers to build its page-visible response. The message stream
/// stays behind `run_ws_loop`.
pub(crate) struct WsHandshake {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
}

/// Why a WS connection could not start. `wreq::Error` exposes no public
/// constructor able to carry `process_ws_response`'s protocol failure, so the
/// two failure families ride side by side instead of a lossy conversion.
/// The payloads are consumed only through the derived `Debug` that the
/// fetch arm formats — rustc's dead-code analysis skips derived impls,
/// hence the allow.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum WsStartError {
    Handshake(wreq::Error),
    Protocol(Error),
}

impl From<wreq::Error> for WsStartError {
    fn from(error: wreq::Error) -> Self {
        WsStartError::Handshake(error)
    }
}

/// Initiate a new async WS connection. Returns an error if the connection fails
/// for any reason, or if the response isn't valid. Otherwise, the endless WS
/// listening loop will be started.
pub(crate) async fn start_websocket(
    http_state: Arc<HttpState>,
    resource_event_sender: IpcSender<WebSocketNetworkEvent>,
    protocols: &[String],
    client: &net_traits::request::Request,
    dom_action_receiver: CallbackSetter<WebSocketDomAction>,
) -> Result<WsHandshake, WsStartError> {
    trace!("starting WS connection to {}", client.url());

    let initiated_close = Arc::new(AtomicBool::new(false));
    let dom_receiver = setup_dom_listener(dom_action_receiver, initiated_close.clone());

    let url = client.url();

    // The handshake URI is the original ws/wss URL (scheme repaired for a
    // handshake that ended on https); wreq's `send()` maps ws→http /
    // wss→https itself. Host-table mapping happens in `ServoDnsResolver`,
    // like every other wreq transport fetch.
    let mut ws_url = client.original_url();
    if ws_url.scheme() == "ws" && url.scheme() == "https" {
        ws_url.as_mut_url().set_scheme("wss").unwrap();
    }

    // Built via `WebSocketRequestBuilder::new` rather than `Client::websocket`
    // so the handshake carries the same no-follow redirect policy as the
    // HTTP transport (see obtain_response in http_loader.rs) — a non-101
    // must fail in `into_websocket`, never be followed.
    let mut ws_response = wreq::ws::WebSocketRequestBuilder::new(
        http_state
            .wreq_client
            .request(Method::GET, ws_url.as_str())
            .redirect(redirect::Policy::none()),
    )
    .headers(client.headers.clone())
    .send()
    .await?;

    // `WebSocketResponse` derefs to `wreq::Response`: status and headers
    // read before the stream is consumed.
    let status = ws_response.status();
    let response_headers = ws_response.headers().clone();

    // `into_websocket` enforces a stricter subprotocol policy than the spec:
    // an echoed `Sec-WebSocket-Protocol` with no builder-level `protocols()`
    // list is an error, and so is a missing echo when a list was set. servo's
    // spec-compliant check in `process_ws_response` owns that decision — the
    // header is removed so wreq's policy stays out of it.
    ws_response
        .headers_mut()
        .remove(http::header::SEC_WEBSOCKET_PROTOCOL);

    // Validate the handshake where tungstenite's used to (101, upgrade
    // headers, accept key) — before any cookie/HSTS side effects run.
    let websocket = ws_response.into_websocket().await?;

    let protocol_in_use =
        process_ws_response(&http_state, &response_headers, &url, protocols)
            .map_err(WsStartError::Protocol)?;

    if !initiated_close.load(Ordering::SeqCst) {
        if resource_event_sender
            .send(WebSocketNetworkEvent::ConnectionEstablished { protocol_in_use })
            .is_err()
        {
            return Ok(WsHandshake {
                status,
                headers: response_headers,
            });
        }

        trace!("about to start ws loop for {}", url);
        spawn_task(run_ws_loop(dom_receiver, resource_event_sender, websocket));
    } else {
        trace!("client closed connection for {}, not running loop", url);
    }
    Ok(WsHandshake {
        status,
        headers: response_headers,
    })
}
