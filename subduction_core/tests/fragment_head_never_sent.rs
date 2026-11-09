//! A server never learns that a fragment covers loose commits it already
//! holds, because the fragment's head never reaches it as a loose commit.
//!
//! Clients upload new changes as loose commits while they are the uncovered
//! tail of a document. When a boundary change arrives (a commit id with a
//! leading zero byte), the changes before it become a fragment headed by it,
//! and the client uploads the fragment. A server counts a loose commit as
//! covered only when it can walk from it to the fragment's head among its
//! loose commits, but the head never arrives as one: a client holding the
//! fragment drops the head from its minimized tree (the fragment covers it)
//! and lists it as known in its fingerprint summary.
//!
//! So the server keeps the whole chain uncovered for good. It loads all of
//! it on every hydration and sends all of it to any peer that has compacted
//! the chain away (automerge-repo and pushwork both do), on every sync.
//!
//! This test fails today; the control alongside it, where the head reaches
//! the server before the fragment, passes.

#![allow(clippy::expect_used)]

use std::{collections::BTreeSet, time::Duration};

use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    connection::test_utils::InstantTimeout,
    peer::id::PeerId,
    test_utils::{ChannelConn, TestNode, dial, spawn_channel_node},
    timeout::call::CallTimeout,
};
use testresult::TestResult;

type Node = TestNode<ChannelConn, InstantTimeout>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(5_000);
const CHAIN: u16 = 150;

/// A commit id at depth 0 (first byte non-zero), or a boundary at depth 1.
fn commit_id(boundary: bool, n: u16) -> CommitId {
    let mut bytes = [0x11u8; 32];
    bytes[0] = if boundary { 0x00 } else { 0x55 };
    bytes[1..3].copy_from_slice(&n.to_be_bytes());
    CommitId::new(bytes)
}

/// The changes before the boundary, oldest first, and the boundary.
fn history() -> (Vec<CommitId>, CommitId) {
    (
        (0..CHAIN).map(|n| commit_id(false, n)).collect(),
        commit_id(true, 0),
    )
}

async fn connected(seed: u8, server: &Node, server_peer: PeerId) -> Node {
    let (node, peer) = spawn_channel_node(seed);
    dial(&node, peer, server, server_peer).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    node
}

async fn add_chain(node: &Node, id: SedimentreeId, chain: &[CommitId]) -> TestResult {
    for (i, commit) in chain.iter().enumerate() {
        let parents = i
            .checked_sub(1)
            .and_then(|p| chain.get(p).copied())
            .into_iter()
            .collect();
        node.add_commit(id, *commit, parents, Blob::new(vec![1; 8]))
            .await?;
    }
    Ok(())
}

async fn add_boundary(
    node: &Node,
    id: SedimentreeId,
    chain: &[CommitId],
    head: CommitId,
) -> TestResult {
    let parent = BTreeSet::from([*chain.last().expect("a chain")]);
    node.add_commit(id, head, parent, Blob::new(vec![2; 8]))
        .await?;
    Ok(())
}

/// The fragment holding the chain and its head (it starts at the root).
async fn add_fragment(node: &Node, id: SedimentreeId, head: CommitId) -> TestResult {
    node.add_fragment(id, head, BTreeSet::new(), &[], Blob::new(vec![3; 64]))
        .await?;
    Ok(())
}

/// What a fresh peer that has compacted the chain away (keeping the fragment
/// and its head) is sent by the server.
async fn sent_to_compacted_peer(
    seed: u8,
    server: &Node,
    server_peer: PeerId,
    id: SedimentreeId,
) -> TestResult<usize> {
    let (chain, head) = history();
    let compacted = connected(seed, server, server_peer).await;
    add_boundary(&compacted, id, &chain, head).await?;
    add_fragment(&compacted, id, head).await?;
    let (_, stats, _) = compacted
        .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
        .await?;
    Ok(stats.commits_received)
}

/// Live editing: the chain is uploaded while it is the uncovered tail, then
/// the boundary arrives and the client uploads the fragment. The client holds
/// the boundary as a loose commit too, and still the server never gets it.
#[tokio::test]
async fn chain_uploaded_before_its_fragment_is_never_covered() -> TestResult {
    let (server, server_peer) = spawn_channel_node(80);
    let author = connected(81, &server, server_peer).await;
    let id = SedimentreeId::new([0x8A; 32]);
    let (chain, head) = history();

    add_chain(&author, id, &chain).await?;
    author
        .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
        .await?;

    add_boundary(&author, id, &chain, head).await?;
    add_fragment(&author, id, head).await?;
    author
        .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
        .await?;

    assert_eq!(
        sent_to_compacted_peer(82, &server, server_peer, id).await?,
        0,
        "the fragment covers the chain, so the server must not send it again"
    );

    Ok(())
}

/// Control: when the boundary reaches the server before the fragment, the
/// server sees the chain is covered and sends nothing. This passes today.
#[tokio::test]
async fn chain_is_covered_when_its_head_arrives_first() -> TestResult {
    let (server, server_peer) = spawn_channel_node(90);
    let author = connected(91, &server, server_peer).await;
    let id = SedimentreeId::new([0x9A; 32]);
    let (chain, head) = history();

    add_chain(&author, id, &chain).await?;
    add_boundary(&author, id, &chain, head).await?;
    author
        .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
        .await?;

    add_fragment(&author, id, head).await?;
    author
        .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
        .await?;

    assert_eq!(
        sent_to_compacted_peer(92, &server, server_peer, id).await?,
        0
    );

    Ok(())
}
