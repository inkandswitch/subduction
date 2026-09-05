//! The framework-neutral `upgrade` module driven from a bare hyper service —
//! no axum anywhere. This is the path poem / salvo / tower users take.

#![allow(clippy::expect_used, reason = "test-only assertions")]
#![allow(clippy::unwrap_used, reason = "test-only assertions")]

use std::{convert::Infallible, net::SocketAddr};

use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use subduction_hyper::upgrade::{self, HyperIo};
use tokio::{net::TcpListener, sync::mpsc};
use tungstenite::{protocol::WebSocketConfig, Message};

type Accepted = mpsc::UnboundedSender<async_tungstenite::WebSocketStream<HyperIo>>;

fn handle(mut req: Request<Incoming>, accepted: Accepted) -> Response<Full<Bytes>> {
    let key = match upgrade::validate(&req) {
        Ok(key) => key,
        Err(rejection) => return rejection.response().map(|s| Full::new(Bytes::from(s))),
    };

    let on_upgrade = hyper::upgrade::on(&mut req);
    upgrade::spawn_upgrade(
        on_upgrade,
        WebSocketConfig::default(),
        move |ws| async move {
            drop(accepted.send(ws));
        },
    );

    upgrade::accept_response(&key).map(|()| Full::new(Bytes::new()))
}

async fn serve() -> (
    SocketAddr,
    mpsc::UnboundedReceiver<async_tungstenite::WebSocketStream<HyperIo>>,
) {
    let (accepted, rx) = mpsc::unbounded_channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.expect("accept");
            let accepted = accepted.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req| {
                    let accepted = accepted.clone();
                    async move { Ok::<_, Infallible>(handle(req, accepted)) }
                });
                // `with_upgrades` is what makes hyper populate `OnUpgrade`.
                let served = http1::Builder::new()
                    .serve_connection(TokioIo::new(tcp), svc)
                    .with_upgrades()
                    .await;
                drop(served);
            });
        }
    });

    (addr, rx)
}

#[tokio::test]
async fn upgrade_from_bare_hyper_service() {
    let (addr, mut accepted) = serve().await;

    let (mut client, resp) = async_tungstenite::tokio::connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("connect");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);

    let mut server = accepted.recv().await.expect("server side of the upgrade");

    client
        .send(Message::Binary(b"raw".as_slice().into()))
        .await
        .expect("client send");
    let got = server.next().await.expect("server recv").expect("frame");
    assert_eq!(got, Message::Binary(b"raw".as_slice().into()));

    server
        .send(Message::Binary(b"hyper".as_slice().into()))
        .await
        .expect("server send");
    let got = client.next().await.expect("client recv").expect("frame");
    assert_eq!(got, Message::Binary(b"hyper".as_slice().into()));
}
