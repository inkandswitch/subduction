//! Subduction server supporting WebSocket, HTTP long-poll, and Iroh (QUIC) transports.

extern crate alloc;

use alloc::sync::Arc;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use eyre::Result;
use iroh::EndpointAddr;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, id::SedimentreeId, sedimentree::Sedimentree,
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    connection::{
        handshake::{self, Audience, DiscoveryId},
        nonce_cache::NonceCache,
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::metrics::{MetricsStorage, RefreshMetrics},
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};
use subduction_http_longpoll::server::LongPollHandler;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    handshake::WebSocketHandshake,
    timeout::FuturesTimerTimeout,
    tokio::{TokioSpawn, unified::UnifiedWebSocket},
    websocket::WebSocket,
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tungstenite::{handshake::server::NoCallback, http::Uri, protocol::WebSocketConfig};

use crate::{key, metrics, transport::UnifiedTransport};

/// Type alias for the unified Subduction instance.
type CliSubduction = Arc<
    Subduction<
        'static,
        future_form::Sendable,
        MetricsStorage<FsStorage>,
        UnifiedTransport<FuturesTimerTimeout>,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
    >,
>;

/// Arguments for the server command.
#[derive(Debug, clap::Parser)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ServerArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    pub(crate) socket: String,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    #[command(flatten)]
    pub(crate) key: key::KeyArgs,

    /// Maximum clock drift allowed during handshake (in seconds)
    #[arg(long, default_value = "600")]
    pub(crate) handshake_max_drift: u64,

    /// Service name for discovery mode (e.g., `sync.example.com`).
    /// Clients can connect without knowing the server's peer ID.
    /// The name is hashed to a 32-byte identifier for the handshake.
    /// Defaults to the socket address if not specified.
    /// Omit the protocol so the same name works across `wss://`, `https://`, etc.
    #[arg(long)]
    pub(crate) service_name: Option<String>,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    pub(crate) timeout: u64,

    /// Maximum WebSocket message size in bytes (default: 50 MB)
    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_SIZE)]
    pub(crate) max_message_size: usize,

    /// Metrics server port (Prometheus endpoint)
    #[arg(long, default_value = "9090")]
    pub(crate) metrics_port: u16,

    /// Enable the Prometheus metrics server
    #[arg(long, default_value_t = false)]
    pub(crate) metrics: bool,

    /// Interval in seconds for refreshing storage metrics from disk
    #[arg(long, default_value_t = DEFAULT_METRICS_REFRESH_SECS)]
    pub(crate) metrics_refresh_interval: u64,

    /// Enable the WebSocket transport
    #[arg(long, default_value_t = true)]
    pub(crate) websocket: bool,

    /// Enable the HTTP long-poll transport
    #[arg(long, default_value_t = true)]
    pub(crate) longpoll: bool,

    /// WebSocket peer URLs to connect to on startup
    #[arg(long = "ws-peer", value_name = "URL")]
    pub(crate) ws_peers: Vec<String>,

    /// Enable the Iroh (QUIC) transport for NAT-traversing P2P connections
    #[arg(long, default_value_t = false)]
    pub(crate) iroh: bool,

    /// Iroh peer node IDs to connect to on startup (z32-encoded public key)
    #[arg(long = "iroh-peer", value_name = "NODE_ID")]
    pub(crate) iroh_peers: Vec<String>,

    /// Direct socket addresses for iroh peers (e.g., `127.0.0.1:12345`).
    /// Added to each `--iroh-peer` as a direct transport address hint.
    #[arg(long = "iroh-peer-addr", value_name = "IP:PORT")]
    pub(crate) iroh_peer_addrs: Vec<SocketAddr>,

    /// Skip iroh relay servers and only use direct connections
    #[arg(long = "iroh-direct-only")]
    pub(crate) iroh_direct_only: bool,

    /// URL of an iroh relay server to route through instead of the public default
    /// (e.g. a self-hosted `iroh-relay` instance)
    #[arg(long = "iroh-relay-url", value_name = "URL")]
    pub(crate) iroh_relay_url: Option<String>,

    /// Write a JSON file on startup with the assigned port, peer ID, and iroh node ID.
    /// Useful for integration tests that need to discover the server's address.
    #[arg(long = "ready-file", value_name = "PATH")]
    pub(crate) ready_file: Option<PathBuf>,
}

/// Default interval for refreshing storage metrics (1 minute).
const DEFAULT_METRICS_REFRESH_SECS: u64 = 60;

/// Run the server with both WebSocket and HTTP long-poll transports.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(args: ServerArgs, token: CancellationToken) -> Result<()> {
    tracing::warn!("Subduction server v{}", env!("CARGO_PKG_VERSION"));

    let addr: SocketAddr = args.socket.parse()?;
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./data"));

    // Initialize and start metrics server if enabled
    if args.metrics {
        let metrics_handle = metrics::init_metrics();
        let metrics_addr: SocketAddr = ([0, 0, 0, 0], args.metrics_port).into();
        metrics::start_metrics_server(metrics_addr, metrics_handle).await?;
    }

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let fs_storage = FsStorage::new(data_dir)?;
    let storage = MetricsStorage::new(fs_storage);

    // Background metrics refresh
    if args.metrics {
        storage.refresh_metrics().await?;

        let metrics_storage = storage.clone();
        let metrics_token = token.clone();
        let refresh_interval = Duration::from_secs(args.metrics_refresh_interval);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = metrics_storage.refresh_metrics().await {
                            tracing::warn!("Failed to refresh storage metrics: {e}");
                        }
                    }
                    () = metrics_token.cancelled() => {
                        tracing::debug!("Stopping metrics refresh task");
                        break;
                    }
                }
            }
        });
    }

    let signer = key::load_signer(&args.key)?;
    let peer_id = PeerId::from(signer.verifying_key());
    let default_time_limit = Duration::from_secs(args.timeout);
    let handshake_max_drift = Duration::from_secs(args.handshake_max_drift);

    let service_name = args
        .service_name
        .clone()
        .unwrap_or_else(|| args.socket.clone());

    let discovery_id = Some(DiscoveryId::new(service_name.as_bytes()));
    let discovery_audience: Option<Audience> = discovery_id.map(Audience::discover_id);

    let sedimentrees: ShardedMap<SedimentreeId, Sedimentree> = ShardedMap::new();

    // Create the unified Subduction instance (parameterized over UnifiedTransport)
    let (subduction, listener_fut, manager_fut): (CliSubduction, _, _) = Subduction::new(
        discovery_id,
        signer.clone(),
        storage,
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        sedimentrees,
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    let server_peer_id = subduction.peer_id();

    // Set up the HTTP long-poll handler (uses its own NonceCache)
    let lp_handler = LongPollHandler::new(
        signer.clone(),
        Arc::new(NonceCache::default()),
        server_peer_id,
        discovery_audience,
        handshake_max_drift,
        default_time_limit,
        FuturesTimerTimeout,
    );

    // Bind the TCP listener
    let tcp_listener = TcpListener::bind(addr).await?;
    let assigned_address = tcp_listener.local_addr()?;

    let ws_enabled = args.websocket;
    let lp_enabled = args.longpoll;
    let iroh_enabled = args.iroh;

    if !ws_enabled && !lp_enabled && !iroh_enabled {
        eyre::bail!("At least one transport must be enabled (--websocket, --longpoll, or --iroh)");
    }

    let transports: Vec<&str> = [
        ws_enabled.then_some("WebSocket"),
        lp_enabled.then_some("HTTP long-poll"),
        iroh_enabled.then_some("Iroh (QUIC)"),
    ]
    .into_iter()
    .flatten()
    .collect();

    tracing::info!(
        "Server started on {assigned_address} ({})",
        transports.join(" + ")
    );
    tracing::info!("Peer ID: {peer_id}");

    // Spawn background tasks
    let actor_cancel = token.clone();
    let listener_cancel = token.clone();

    tokio::spawn(async move {
        tokio::select! {
            _ = manager_fut => {},
            () = actor_cancel.cancelled() => {}
        }
    });

    tokio::spawn(async move {
        tokio::select! {
            _ = listener_fut => {},
            () = listener_cancel.cancelled() => {}
        }
    });

    // Spawn the accept loop
    let accept_cancel = token.child_token();
    let accept_subduction = subduction.clone();
    let accept_handler = lp_handler;
    let max_message_size = args.max_message_size;

    let accept_task = tokio::spawn(async move {
        accept_loop(
            tcp_listener,
            accept_subduction,
            accept_handler,
            accept_cancel,
            FuturesTimerTimeout,
            default_time_limit,
            handshake_max_drift,
            max_message_size,
            server_peer_id,
            discovery_audience,
            ws_enabled,
            lp_enabled,
        )
        .await;
    });

    // ── Iroh (QUIC) transport ────────────────────────────────────────────────
    let mut iroh_node_id: Option<String> = None;
    let mut iroh_addrs: Vec<SocketAddr> = Vec::new();
    let iroh_accept_task = if iroh_enabled {
        let relay_mode = if args.iroh_direct_only {
            iroh::endpoint::RelayMode::Disabled
        } else if let Some(url) = &args.iroh_relay_url {
            let relay_map =
                iroh::RelayMap::try_from_iter([url.as_str()]).map_err(|e| eyre::eyre!(e))?;
            iroh::endpoint::RelayMode::Custom(relay_map)
        } else {
            iroh::endpoint::RelayMode::Default
        };

        let iroh_endpoint = iroh::Endpoint::builder()
            .alpns(vec![subduction_iroh::ALPN.to_vec()])
            .relay_mode(relay_mode)
            .bind()
            .await?;

        let iroh_addr = iroh_endpoint.addr();
        iroh_node_id = Some(iroh_addr.id.to_string());
        iroh_addrs = iroh_addr.ip_addrs().copied().collect();
        tracing::info!("Iroh endpoint bound: node ID = {}", iroh_addr.id);
        for addr in &iroh_addr.addrs {
            tracing::info!("  transport address: {addr:?}");
        }

        // Spawn iroh accept loop
        let iroh_subduction = subduction.clone();
        let iroh_signer = signer.clone();
        let iroh_nonce_cache = NonceCache::default();
        let iroh_ep = iroh_endpoint.clone();
        let iroh_cancel = token.child_token();

        let task = tokio::spawn({
            let cancel = iroh_cancel.clone();
            async move {
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => {
                            tracing::info!("iroh accept loop canceled");
                            break;
                        }
                        result = subduction_iroh::server::accept_one(
                            &iroh_ep,
                            default_time_limit,
                            FuturesTimerTimeout,
                            &iroh_signer,
                            &iroh_nonce_cache,
                            server_peer_id,
                            discovery_audience,
                            handshake_max_drift,
                        ) => {
                            match result {
                                Ok(accepted) => {
                                    let remote = accepted.peer_id;
                                    tokio::spawn(accepted.listener_task);
                                    tokio::spawn(accepted.sender_task);

                                    let auth = accepted.authenticated.map(UnifiedTransport::Iroh);
                                    if let Err(e) = iroh_subduction.attach(auth).await {
                                        tracing::error!("failed to attach iroh connection: {e}");
                                    } else {
                                        tracing::info!("iroh: attached peer {remote}");
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("iroh accept error: {e}");
                                }
                            }
                        }
                    }
                }
            }
        });

        // Connect to iroh peers
        for iroh_peer_str in &args.iroh_peers {
            let node_id: iroh::PublicKey = match iroh_peer_str.parse() {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!("invalid iroh peer node ID '{iroh_peer_str}': {e}");
                    continue;
                }
            };

            let mut peer_addr = EndpointAddr::new(node_id);
            for addr in &args.iroh_peer_addrs {
                peer_addr = peer_addr.with_ip_addr(*addr);
            }
            let peer_ep = iroh_endpoint.clone();
            let peer_subduction = subduction.clone();
            let peer_signer = signer.clone();
            let peer_cancel = token.clone();
            let peer_service_name = service_name.clone();

            tokio::spawn(async move {
                match try_connect_iroh(
                    &peer_ep,
                    peer_addr,
                    &peer_subduction,
                    &peer_signer,
                    &peer_service_name,
                    default_time_limit,
                    peer_cancel,
                )
                .await
                {
                    Ok(remote_id) => {
                        tracing::info!(
                            "iroh: connected to peer {node_id} (subduction ID: {remote_id})"
                        );
                    }
                    Err(e) => {
                        tracing::error!("iroh: failed to connect to peer {node_id}: {e}");
                    }
                }
            });
        }

        // Background sync task: periodically reconcile with all connected peers
        let sync_subduction = subduction.clone();
        let sync_cancel = token.child_token();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.tick().await; // skip the first immediate tick
            loop {
                tokio::select! {
                    () = sync_cancel.cancelled() => {
                        tracing::debug!("iroh: background sync task canceled");
                        break;
                    }
                    _ = interval.tick() => {
                        let timeout = Some(Duration::from_secs(10));
                        let (had_success, _stats, call_errs, io_errs) =
                            sync_subduction.full_sync(timeout).await;
                        if had_success {
                            tracing::debug!("iroh: background full_sync completed");
                        }
                        for e in &call_errs {
                            tracing::warn!("iroh: background sync call error: {e:?}");
                        }
                        for e in &io_errs {
                            tracing::warn!("iroh: background sync io error: {e:?}");
                        }
                    }
                }
            }
        });

        Some(task)
    } else {
        None
    };

    // Connect to configured WebSocket peers for bidirectional sync
    for peer_url in &args.ws_peers {
        let uri: Uri = match peer_url.parse() {
            Ok(uri) => uri,
            Err(e) => {
                tracing::error!("Invalid peer URL '{peer_url}': {e}");
                continue;
            }
        };

        let peer_subduction = subduction.clone();
        let peer_signer = signer.clone();
        let peer_service_name = service_name.clone();
        let peer_cancel = token.clone();

        tokio::spawn(async move {
            match try_connect_ws(
                uri.clone(),
                &peer_subduction,
                &peer_signer,
                &peer_service_name,
                FuturesTimerTimeout,
                default_time_limit,
                peer_cancel,
            )
            .await
            {
                Ok(remote_id) => {
                    tracing::info!("Connected to peer at {uri} (peer ID: {remote_id})");
                }
                Err(e) => {
                    tracing::error!("Failed to connect to peer at {uri}: {e}");
                }
            }
        });
    }

    // Write ready file if requested (for integration tests)
    if let Some(ref ready_path) = args.ready_file {
        let iroh_line = iroh_node_id
            .as_deref()
            .map_or(String::new(), |id| format!("iroh_node_id={id}\n"));
        let iroh_addrs_line = if iroh_addrs.is_empty() {
            String::new()
        } else {
            let addrs: Vec<String> = iroh_addrs.iter().map(ToString::to_string).collect();
            format!("iroh_addrs={}\n", addrs.join(","))
        };
        let content = format!(
            "port={}\npeer_id={}\n{iroh_line}{iroh_addrs_line}",
            assigned_address.port(),
            peer_id,
        );
        std::fs::write(ready_path, content)
            .map_err(|e| eyre::eyre!("failed to write ready file: {e}"))?;
        tracing::info!("Ready file written to {}", ready_path.display());
    }

    // Wait for cancellation signal
    token.cancelled().await;
    tracing::info!("Shutting down server...");
    accept_task.abort();
    if let Some(iroh_task) = iroh_accept_task {
        iroh_task.abort();
    }

    Ok(())
}

/// Accept loop: routes incoming TCP connections to WebSocket or HTTP long-poll.
#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    tcp_listener: TcpListener,
    subduction: CliSubduction,
    lp_handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
    cancel: CancellationToken,
    timeout: FuturesTimerTimeout,
    default_time_limit: Duration,
    handshake_max_drift: Duration,
    max_message_size: usize,
    server_peer_id: PeerId,
    discovery_audience: Option<Audience>,
    ws_enabled: bool,
    lp_enabled: bool,
) {
    let mut conns = JoinSet::new();

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::info!("accept loop canceled");
                break;
            }
            res = tcp_listener.accept() => {
                match res {
                    Ok((tcp, addr)) => {
                        tracing::info!("new TCP connection from {addr}");

                        let task_subduction = subduction.clone();
                        let task_handler = lp_handler.clone();
                        let task_discovery = discovery_audience;

                        conns.spawn(async move {
                            // Peek to determine transport:
                            //   GET  → WebSocket upgrade
                            //   POST → HTTP long-poll
                            //   OPTI → CORS preflight (OPTIONS), routed to HTTP handler
                            let mut peek_buf = [0u8; 4];
                            match tcp.peek(&mut peek_buf).await {
                                Ok(n) if n >= 3 => {}
                                Ok(_) | Err(_) => {
                                    tracing::warn!("failed to peek TCP stream from {addr}");
                                    return;
                                }
                            }

                            let is_http = peek_buf.starts_with(b"POST")
                                || peek_buf.starts_with(b"OPTI");

                            if peek_buf.starts_with(b"GET") && ws_enabled {
                                handle_websocket(
                                    tcp,
                                    addr,
                                    task_subduction,
                                    timeout,
                                    default_time_limit,
                                    handshake_max_drift,
                                    max_message_size,
                                    server_peer_id,
                                    task_discovery,
                                )
                                .await;
                            } else if is_http && lp_enabled {
                                handle_http_longpoll(
                                    tcp,
                                    addr,
                                    task_subduction,
                                    task_handler,
                                )
                                .await;
                            } else if peek_buf.starts_with(b"GET") {
                                tracing::warn!("WebSocket connection from {addr} rejected (transport disabled)");
                            } else if is_http {
                                tracing::warn!("HTTP long-poll connection from {addr} rejected (transport disabled)");
                            } else {
                                tracing::warn!(
                                    "unknown protocol from {addr}: {:02x?}",
                                    &peek_buf
                                );
                            }
                        });
                    }
                    Err(e) => tracing::error!("Accept error: {e}"),
                }
            }
        }
    }

    while (conns.join_next().await).is_some() {}
}

/// Handle a WebSocket connection: upgrade, handshake, register.
#[allow(clippy::too_many_arguments)]
async fn handle_websocket(
    tcp: tokio::net::TcpStream,
    addr: SocketAddr,
    subduction: CliSubduction,
    timeout: FuturesTimerTimeout,
    default_time_limit: Duration,
    handshake_max_drift: Duration,
    max_message_size: usize,
    server_peer_id: PeerId,
    discovery_audience: Option<Audience>,
) {
    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(max_message_size);

    let ws_stream = match async_tungstenite::tokio::accept_hdr_async_with_config(
        tcp,
        NoCallback,
        Some(ws_config),
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            tracing::error!("WebSocket upgrade error from {addr}: {e}");
            return;
        }
    };

    tracing::debug!("WebSocket upgrade complete for {addr}");

    let now = TimestampSeconds::now();
    let result = handshake::respond::<future_form::Sendable, _, _, _, _>(
        WebSocketHandshake::new(ws_stream),
        |ws_handshake, remote_peer_id| {
            let (ws, sender_fut) = WebSocket::new(
                ws_handshake.into_inner(),
                timeout,
                default_time_limit,
                remote_peer_id,
            );

            let listen_ws = ws.clone();
            tokio::spawn(async move {
                if let Err(e) = listen_ws.listen().await {
                    tracing::error!("WebSocket listen error: {e}");
                }
            });

            tokio::spawn(async move {
                if let Err(e) = sender_fut.await {
                    tracing::error!("WebSocket sender error: {e}");
                }
            });

            let unified_ws = UnifiedWebSocket::Accepted(ws);
            (UnifiedTransport::WebSocket(unified_ws), ())
        },
        subduction.signer(),
        subduction.nonce_cache(),
        server_peer_id,
        discovery_audience,
        now,
        handshake_max_drift,
    )
    .await;

    let authenticated = match result {
        Ok((auth, ())) => {
            tracing::info!(
                "WebSocket handshake complete: client {} from {addr}",
                auth.peer_id()
            );
            auth
        }
        Err(e) => {
            tracing::warn!("WebSocket handshake failed from {addr}: {e}");
            return;
        }
    };

    if let Err(e) = subduction.register(authenticated).await {
        tracing::error!("Failed to register WebSocket connection: {e}");
    }
}

/// Handle an HTTP long-poll connection via hyper.
async fn handle_http_longpoll(
    tcp: tokio::net::TcpStream,
    addr: SocketAddr,
    subduction: CliSubduction,
    handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
) {
    use http_body_util::Full;
    use hyper::{
        body::Bytes,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, HeaderValue,
        },
    };
    use hyper_util::rt::TokioIo;

    let io = TokioIo::new(tcp);

    let service = hyper::service::service_fn(move |req| {
        let handler = handler.clone();
        let subduction = subduction.clone();
        async move {
            // Handle CORS preflight
            if req.method() == hyper::Method::OPTIONS {
                let mut resp = hyper::Response::new(Full::new(Bytes::new()));
                *resp.status_mut() = hyper::StatusCode::NO_CONTENT;
                resp.headers_mut()
                    .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
                resp.headers_mut().insert(
                    ACCESS_CONTROL_ALLOW_METHODS,
                    HeaderValue::from_static("POST, OPTIONS"),
                );
                resp.headers_mut().insert(
                    ACCESS_CONTROL_ALLOW_HEADERS,
                    HeaderValue::from_static("Content-Type, X-Session-Id"),
                );
                resp.headers_mut().insert(
                    hyper::header::ACCESS_CONTROL_EXPOSE_HEADERS,
                    HeaderValue::from_static("X-Session-Id"),
                );
                resp.headers_mut()
                    .insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
                return Ok::<_, hyper::Error>(resp);
            }

            let resp = match handler.handle(req).await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::error!("fatal handler error: {e}");
                    hyper::Response::new(Full::new(Bytes::from(e.to_string())))
                }
            };

            // After a successful handshake, register with Subduction
            if resp.status() == hyper::StatusCode::OK
                && let Some(session_hdr) = resp
                    .headers()
                    .get(subduction_http_longpoll::SESSION_ID_HEADER)
                && let Ok(sid_str) = session_hdr.to_str()
                && let Some(sid) = subduction_http_longpoll::session::SessionId::from_hex(sid_str)
                && let Some(auth) = handler.take_authenticated(&sid).await
            {
                let unified_auth = auth.map(UnifiedTransport::HttpLongPoll);
                if let Err(e) = subduction.register(unified_auth).await {
                    tracing::error!("Failed to register HTTP long-poll connection: {e}");
                }
            }

            // Add CORS headers to every response
            let (mut parts, body) = resp.into_parts();
            parts
                .headers
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            parts.headers.insert(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("POST, OPTIONS"),
            );
            parts.headers.insert(
                ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("Content-Type, X-Session-Id"),
            );
            // Expose custom headers so the browser can read them
            parts.headers.insert(
                hyper::header::ACCESS_CONTROL_EXPOSE_HEADERS,
                HeaderValue::from_static("X-Session-Id"),
            );

            Ok::<_, hyper::Error>(hyper::Response::from_parts(parts, body))
        }
    });

    let builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    let conn = builder.serve_connection(io, service);

    if let Err(e) = conn.await {
        tracing::debug!("HTTP connection from {addr} ended: {e}");
    }
}

/// Connect to a peer via WebSocket (outbound).
#[allow(clippy::too_many_arguments)]
async fn try_connect_ws(
    uri: Uri,
    subduction: &CliSubduction,
    signer: &MemorySigner,
    service_name: &str,
    timeout: FuturesTimerTimeout,
    default_time_limit: Duration,
    cancel: CancellationToken,
) -> Result<PeerId, eyre::Error> {
    let uri_str = uri.to_string();
    tracing::info!("Connecting to peer at {uri_str} via discovery ({service_name})");

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(DEFAULT_MAX_MESSAGE_SIZE);
    let (ws_stream, _resp) =
        async_tungstenite::tokio::connect_async_with_config(uri.clone(), Some(ws_config)).await?;

    let audience = Audience::discover(service_name.as_bytes());
    let now = TimestampSeconds::now();
    let nonce = Nonce::random();

    let listen_uri = uri_str.clone();
    let sender_uri = uri_str.clone();
    let listen_cancel = cancel.clone();

    let (authenticated, ()) = handshake::initiate::<future_form::Sendable, _, _, _, _>(
        WebSocketHandshake::new(ws_stream),
        move |ws_handshake, remote_peer_id| {
            let (ws, sender_fut) = WebSocket::new(
                ws_handshake.into_inner(),
                timeout,
                default_time_limit,
                remote_peer_id,
            );

            let ws_conn = UnifiedWebSocket::Dialed(ws.clone());

            let listen_ws = ws.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = listen_cancel.cancelled() => {
                        tracing::debug!("Shutting down listener for peer {listen_uri}");
                    }
                    result = listen_ws.listen() => {
                        if let Err(e) = result {
                            tracing::error!("WebSocket listen error for {listen_uri}: {e}");
                        }
                    }
                }
            });

            let sender_cancel = cancel;
            tokio::spawn(async move {
                tokio::select! {
                    () = sender_cancel.cancelled() => {
                        tracing::debug!("Shutting down sender for peer {sender_uri}");
                    }
                    result = sender_fut => {
                        if let Err(e) = result {
                            tracing::error!("WebSocket sender error for {sender_uri}: {e}");
                        }
                    }
                }
            });

            (UnifiedTransport::WebSocket(ws_conn), ())
        },
        signer,
        audience,
        now,
        nonce,
    )
    .await?;

    let remote_id = authenticated.peer_id();
    tracing::info!("Handshake complete: connected to {remote_id}");

    subduction.register(authenticated).await?;
    tracing::info!("Connected to peer at {uri_str}");

    Ok(remote_id)
}

/// Connect to a peer via Iroh (QUIC) transport (outbound).
#[allow(clippy::too_many_arguments)]
async fn try_connect_iroh(
    endpoint: &iroh::Endpoint,
    addr: EndpointAddr,
    subduction: &CliSubduction,
    signer: &MemorySigner,
    service_name: &str,
    default_time_limit: Duration,
    cancel: CancellationToken,
) -> Result<PeerId, eyre::Error> {
    let node_id = addr.id;
    tracing::info!("iroh: connecting to {node_id} via discovery ({service_name})");

    let audience = Audience::discover(service_name.as_bytes());

    let connect_result = subduction_iroh::client::connect(
        endpoint,
        addr,
        default_time_limit,
        FuturesTimerTimeout,
        signer,
        audience,
    )
    .await?;

    let authenticated = connect_result.authenticated;
    let listener_task = connect_result.listener_task;
    let sender_task = connect_result.sender_task;

    let listener_cancel = cancel.clone();
    let sender_cancel = cancel;

    tokio::spawn(async move {
        tokio::select! {
            () = listener_cancel.cancelled() => {
                tracing::debug!("iroh: shutting down listener for peer {node_id}");
            }
            result = listener_task => {
                if let Err(e) = result {
                    tracing::error!("iroh: listener error for {node_id}: {e}");
                }
            }
        }
    });

    tokio::spawn(async move {
        tokio::select! {
            () = sender_cancel.cancelled() => {
                tracing::debug!("iroh: shutting down sender for peer {node_id}");
            }
            result = sender_task => {
                if let Err(e) = result {
                    tracing::error!("iroh: sender error for {node_id}: {e}");
                }
            }
        }
    });

    let remote_id = authenticated.peer_id();
    let auth = authenticated.map(UnifiedTransport::Iroh);
    subduction.attach(auth).await?;

    tracing::info!("iroh: attached peer {node_id} (subduction ID: {remote_id})");
    Ok(remote_id)
}
