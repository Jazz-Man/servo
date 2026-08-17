/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! servo ↔ wreq WebSocket message mapping — fully ours. wreq's own
//! tungstenite conversions are `pub(super)` (wreq `client/ws/message.rs`
//! `into_tungstenite`/`from_tungstenite`), so nothing crosses that boundary
//! for free; every variant maps explicitly and payloads move by ownership.
//! Both sides share one `bytes` crate, so `Bytes` payloads transfer without
//! copying; `Utf8Bytes` round-trips through `Bytes` (a zero-copy newtype
//! unwrap/rewrap — the `try_from` cannot fail on tungstenite-validated
//! UTF-8).

use bytes::Bytes;
use net_traits::MessageData;
use tungstenite::Message as TungsteniteMessage;
use tungstenite::Utf8Bytes as ServoUtf8Bytes;
use wreq::ws::message::CloseCode as WreqCloseCode;
use wreq::ws::message::CloseFrame as WreqCloseFrame;
use wreq::ws::message::Message as WreqMessage;
use wreq::ws::message::Utf8Bytes as WreqUtf8Bytes;

/// Outgoing (dom → wire): servo/script's tungstenite-0.30 message becomes
/// wreq's message.
pub(crate) fn dom_to_wreq(message: TungsteniteMessage) -> WreqMessage {
    match message {
        TungsteniteMessage::Text(text) => WreqMessage::Text(utf8_to_wreq(text)),
        TungsteniteMessage::Binary(data) => WreqMessage::Binary(data),
        TungsteniteMessage::Ping(data) => WreqMessage::Ping(data),
        TungsteniteMessage::Pong(data) => WreqMessage::Pong(data),
        TungsteniteMessage::Close(frame) => WreqMessage::Close(frame.map(|frame| {
            WreqCloseFrame {
                // The two CloseCode types sit on distinct tungstenite lines
                // (servo: 0.30, wreq: 0.29) — u16 is the common ground.
                code: WreqCloseCode::from(u16::from(frame.code)),
                reason: utf8_to_wreq(frame.reason),
            }
        })),
        TungsteniteMessage::Frame(_) => {
            unreachable!("tungstenite streams never surface raw frames to this layer")
        },
    }
}

/// Incoming (wire → dom): wreq's message becomes the script-thread payload.
/// Returns `None` for frames this layer already handled or must skip, exactly
/// as the tungstenite loop did. Close maps to `None` because `run_ws_loop`
/// consumes it to emit the close event itself.
pub(crate) fn wreq_to_message_data(message: WreqMessage) -> Option<MessageData> {
    match message {
        WreqMessage::Text(text) => Some(MessageData::Text(text.to_string())),
        WreqMessage::Binary(data) => Some(MessageData::Binary(data.to_vec())),
        // Ping/Pong are transport-level: answered inside wreq/tungstenite,
        // never surfaced — parity with the old loop's skip arms.
        WreqMessage::Ping(_) | WreqMessage::Pong(_) => None,
        WreqMessage::Close(_) => None,
    }
}

/// tungstenite's `Utf8Bytes` and wreq's are distinct types over the same
/// `bytes::Bytes`; unwrap one and rewrap the other, moving the buffer.
fn utf8_to_wreq(text: ServoUtf8Bytes) -> WreqUtf8Bytes {
    WreqUtf8Bytes::try_from(Bytes::from(text))
        .expect("tungstenite Utf8Bytes is valid UTF-8 by construction")
}
