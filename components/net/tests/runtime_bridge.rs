/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Runtime-bridge spike: proves (or refutes) that a `wreq::Client` built on a
//! FOREIGN tokio runtime can be driven by servo-net's own async runtime with a
//! shared connection pool — Form A of the transport design (spec §3.4).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use net::async_runtime::spawn_task;
use net::test_util::create_embedder_proxy;

/// One OS thread serving HTTP/1.1 keep-alive on a loopback listener.
/// Counts accepted TCP connections — the pool-reuse signal.
fn spawn_counting_http_server() -> (String, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&connections);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8192];
            loop {
                let read = match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if std::str::from_utf8(&buf[..read]).is_ok_and(|r| r.starts_with("GET ")) {
                    let body = b"bridge-ok";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(body);
                }
            }
        }
    });
    (addr, connections)
}

#[test]
fn wreq_client_pool_survives_cross_runtime_drive() {
    let (addr, connections) = spawn_counting_http_server();

    // Guarded lazy init of servo-net's async runtime — the harness idiom
    // (test_util.rs:36-46). Never call init_async_runtime directly here:
    // a second init panics (async_runtime.rs:60-62).
    let _embedder_proxy = create_embedder_proxy();

    // Foreign runtime: the client is BUILT while this runtime is ambient,
    // the way crates/client builds its wreq handle today.
    let foreign_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(1)
        .build()
        .unwrap();
    let client = foreign_runtime
        .block_on(async { wreq::Client::builder().build() })
        .unwrap();

    // servo-net's own runtime drives both requests.
    let (tx, rx) = mpsc::channel();
    spawn_task(async move {
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let response = client
                .get(format!("http://{addr}/"))
                .send()
                .await
                .unwrap();
            statuses.push(response.status().as_u16());
        }
        tx.send(statuses).unwrap();
    });

    let statuses = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("spike hung — cross-runtime drive failed (Form B trigger)");

    assert_eq!(statuses, vec![200, 200]);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "two sequential GETs must reuse one pooled connection across runtimes"
    );
}
