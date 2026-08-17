/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Pins the H2 cancellation contract of the wreq transport: a response
//! dropped mid-body resets only its own H2 stream — the pooled connection
//! stays healthy, so a second request on the SAME client must succeed.

use futures::StreamExt;
use hyper::Request as HyperRequest;
use hyper::Response as HyperResponse;
use hyper::body::{Bytes, Incoming};
use http_body_util::combinators::BoxBody;
use net::async_runtime::spawn_blocking_task;
use net::test_util::{make_body, make_h2_server};

#[test]
fn dropped_h2_stream_does_not_poison_the_connection_pool() {
    // A body far larger than one H2 DATA frame, so a single chunk read
    // leaves the stream mid-body when the response is dropped.
    let body = vec![b'a'; 8 * 1024 * 1024];
    let handler =
        move |_: HyperRequest<Incoming>, response: &mut HyperResponse<BoxBody<Bytes, hyper::Error>>| {
            *response.body_mut() = make_body(body.clone());
        };

    let (server, mut url) = make_h2_server(handler);
    url.as_mut_url().set_scheme("https").unwrap();
    let url_string = url.as_url().to_string();

    let second_response = spawn_blocking_task::<_, Option<(u16, bool)>>(async move {
        let client = wreq::Client::builder()
            // The test certificate is self-signed.
            .tls_cert_verification(false)
            .build()
            .unwrap();

        // Request 1: read one body chunk, then drop the response mid-body.
        let response = client
            .get(url_string.as_str())
            .send()
            .await
            .expect("request 1 send failed");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.version(), http::Version::HTTP_2);
        let mut stream = response.bytes_stream();
        let _first_chunk = stream.next().await.expect("first body chunk missing");
        drop(stream);

        // Request 2 on the SAME client: the dropped stream must not have
        // poisoned the pooled H2 connection.
        let response = client
            .get(url_string.as_str())
            .send()
            .await
            .expect("request 2 send failed");
        let status = response.status().as_u16();
        let is_h2 = response.version() == http::Version::HTTP_2;
        // Drain so the server observes a completed exchange before shutdown.
        let _ = response.bytes().await;
        Some((status, is_h2))
    });

    server.close();

    let (status, is_h2) = second_response.expect("no second response recorded");
    assert_eq!(status, 200);
    assert!(is_h2, "request 2 did not speak HTTP/2");
}
