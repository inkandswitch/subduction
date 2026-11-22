//! A Subduction peer that edits a document with [`automerge::next`].
//!
//! Connects to a Subduction WebSocket server, pulls the document, checks the
//! `--expect`ed root strings, writes the `--put` root strings, and pushes only
//! the new history back with `fragments_after`. Strings are text objects, as
//! Automerge 3's JS API writes them.
//!
//! ```sh
//! cargo run -p automerge_subduction_ingest --example next_peer -- \
//!   --server ws://localhost:3000 --service-name localhost:3000 \
//!   --doc-id automerge:... --expect fromJs=hello --put fromRust=hi
//! ```

use std::{sync::Arc, time::Duration};

use automerge::{
    ObjType, ROOT, Value,
    next::{AutoCommit, ReadDoc, transaction::Transactable},
};
use automerge_subduction_ingest::{ingest_fragments, parse_doc_id};
use clap::Parser;
use eyre::{Result, WrapErr, eyre};
use future_form::Sendable;
use sedimentree_core::blob::Blob;
use subduction_core::{
    handshake::audience::Audience, policy::open::OpenPolicy, storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder, timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn, client::TokioWebSocketClient};

#[derive(Debug, Parser)]
struct Args {
    /// WebSocket URL of the Subduction server.
    #[arg(long)]
    server: String,

    /// Service name the server accepts in the handshake.
    #[arg(long)]
    service_name: String,

    /// Document ID, as an `automerge:` URL or bare base58check.
    #[arg(long)]
    doc_id: String,

    /// `key=value` that must be a root string in the pulled document.
    #[arg(long, value_parser = parse_kv)]
    expect: Vec<(String, String)>,

    /// `key=value` to set as a root string and push.
    #[arg(long, value_parser = parse_kv)]
    put: Vec<(String, String)>,

    /// Overall deadline in seconds.
    #[arg(long, default_value = "30")]
    timeout: u64,
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .ok_or_else(|| format!("expected key=value, got {s}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let deadline = Duration::from_secs(args.timeout);
    tokio::time::timeout(deadline, run(args))
        .await
        .map_err(|_| eyre!("timed out after {}s", deadline.as_secs()))?
}

async fn run(args: Args) -> Result<()> {
    let sed_id = parse_doc_id(&args.doc_id)?;
    let signer = MemorySigner::generate();

    let (subduction, _handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(signer.clone())
            .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(TimeoutTokio)
            .build::<Sendable, MessageTransport<TokioWebSocketClient<MemorySigner>>>();
    tokio::spawn(listener_fut);
    tokio::spawn(manager_fut);

    let uri: tungstenite::http::Uri = args.server.parse().wrap_err("invalid server URL")?;
    let audience = Audience::discover(args.service_name.as_bytes());
    let (authenticated, listener_task, sender_task, keepalive_task) =
        TokioWebSocketClient::new(uri, signer, audience)
            .await
            .wrap_err("failed to connect to server")?;
    tokio::spawn(listener_task.into_future());
    tokio::spawn(sender_task.into_future());
    tokio::spawn(keepalive_task.into_future());

    let server = authenticated.peer_id();
    subduction
        .add_connection(authenticated.map(MessageTransport::new))
        .await
        .wrap_err("failed to register connection")?;

    let (found, _, _) = subduction
        .sync_with_peer(&server, sed_id, false, CallTimeout::Default)
        .await
        .map_err(|e| eyre!("sync failed: {e}"))?;
    if !found {
        eyre::bail!("server did not answer the sync");
    }

    let blobs = subduction
        .get_blobs(sed_id)
        .await
        .map_err(|e| eyre!("reading blobs failed: {e}"))?
        .ok_or_else(|| eyre!("server has no data for {sed_id}"))?;

    // One load: blobs arrive in no causal order, and loading them one by one
    // would drop each change that arrives before its dependencies.
    let all: Vec<u8> = blobs.iter().flat_map(Blob::as_slice).copied().collect();
    let mut doc = AutoCommit::load(&all).wrap_err("loading the pulled blobs failed")?;
    eprintln!(
        "pulled {} blobs, {} changes",
        blobs.len(),
        doc.document().num_changes()
    );

    for (key, want) in &args.expect {
        let got = root_string(&doc, key)?;
        if got.as_deref() != Some(want.as_str()) {
            eyre::bail!("expected {key}={want}, found {got:?}");
        }
        eprintln!("saw {key}={want}");
    }

    if args.put.is_empty() {
        return Ok(());
    }

    let before = doc.get_heads();
    for (key, value) in &args.put {
        let text = doc.put_object(ROOT, key.as_str(), ObjType::Text)?;
        doc.splice_text(&text, 0, 0, value)?;
    }
    doc.commit();

    let new_history = doc.fragments_after(&before, false)?;
    let ingested = ingest_fragments(new_history, sed_id)?;
    eprintln!(
        "pushing {} fragments + {} loose commits",
        ingested.fragment_count, ingested.loose_count
    );
    subduction
        .add_sedimentree(
            sed_id,
            ingested.sedimentree,
            ingested.blobs,
            CallTimeout::Default,
        )
        .await
        .map_err(|e| eyre!("push failed: {e}"))?;
    eprintln!("pushed");

    Ok(())
}

fn root_string(doc: &AutoCommit, key: &str) -> Result<Option<String>> {
    Ok(match doc.get(ROOT, key)? {
        Some((Value::Object(ObjType::Text), id)) => Some(doc.text(&id)?),
        Some((v, _)) => v.as_str().map(str::to_owned),
        None => None,
    })
}
