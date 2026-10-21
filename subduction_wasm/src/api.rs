//! Public POJO boundary: admit owned core values synchronously, then convert
//! core results exactly once on output. Node state has no wasm-bindgen ABI.
use super::{JsSubductionOptions, Node, PeerSyncResult, PeerSyncResults};
use crate::{
    error::{WasmConnectError, WasmHandshakeError, WasmWriteError},
    handler::JsFrameHandler,
    transport::{JsTransport, authenticated},
};
use alloc::{
    format,
    rc::Rc,
    string::{String, ToString},
    vec::Vec,
};
use core::future::Future;
use js_sys::Uint8Array;
use nonempty::NonEmpty;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use sedimentree_core::{fragment::Fragment, loose_commit::LooseCommit};
use sedimentree_wasm::pojo as p;
use subduction_core::connection::stats::SyncStats;
use subduction_core::{peer::id::PeerId, timeout::call::CallTimeout};
use subduction_ephemeral::topic::Topic;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = include_str!("api.d.ts");

/// The only public library class. All data is copied/decoded before scheduling.
#[derive(Debug)]
#[wasm_bindgen(js_name = Subduction, skip_typescript)]
pub struct SubductionApi(Rc<Node>);

impl Drop for SubductionApi {
    fn drop(&mut self) {
        self.0.start_close();
    }
}

impl SubductionApi {
    fn admit<T, R, F>(
        &self,
        parse: impl FnOnce() -> Result<T, JsValue>,
        work: impl FnOnce(Rc<Node>, T) -> F + 'static,
    ) -> js_sys::Promise
    where
        T: 'static,
        R: Output + 'static,
        F: Future<Output = Result<R, JsValue>> + 'static,
    {
        // Admission and conversion are synchronous, even if the executor has not
        // polled the returned future. Never capture application data in `work`.
        let input = self.0.runtime.ensure_open().and_then(|()| parse());
        match input {
            Err(error) => js_sys::Promise::reject(&error),
            Ok(input) => {
                let node = self.0.clone();
                let runtime = node.runtime.clone();
                future_to_promise(async move {
                    runtime
                        .run::<_, JsValue>(work(node, input))
                        .await
                        .map(Output::output)
                })
            }
        }
    }

    fn commit(&self, id: &JsValue, input: &JsValue, push: bool) -> js_sys::Promise {
        self.admit(
            || {
                let id = p::tree(id, "id")?;
                Ok((id, p::commit_input(id, input, "commit")?))
            },
            move |node, (id, (commit, blob))| async move {
                let result = if push {
                    node.core
                        .add_commit(id, commit.head(), commit.parents().clone(), blob)
                        .await
                } else {
                    node.core
                        .store_commit(id, commit.head(), commit.parents().clone(), blob)
                        .await
                }
                .map_err(WasmWriteError::from)?;
                Ok(result.map(|request| {
                    p::object(&[
                        ("head", p::hex(request.head().as_bytes()).into()),
                        ("depth", request.depth().0.into()),
                    ])
                }))
            },
        )
    }

    fn fragment(&self, id: &JsValue, input: &JsValue, push: bool) -> js_sys::Promise {
        self.admit(
            || {
                let id = p::tree(id, "id")?;
                Ok((id, p::fragment_input(id, input, "fragment")?))
            },
            move |node, (id, (fragment, blob))| async move {
                // The core raw single-item API truncates these IDs to 12-byte
                // prefixes unconditionally. Padding preserves exactly the input
                // checkpoint set and the existing single-item push semantics.
                let checkpoints: Vec<_> = fragment
                    .checkpoints()
                    .iter()
                    .map(|cp| {
                        let mut bytes = [0; 32];
                        bytes[..12].copy_from_slice(cp.as_bytes());
                        CommitId::new(bytes)
                    })
                    .collect();
                if push {
                    node.core
                        .add_fragment(
                            id,
                            fragment.head(),
                            fragment.boundary().clone(),
                            &checkpoints,
                            blob,
                        )
                        .await
                } else {
                    node.core
                        .store_fragment(
                            id,
                            fragment.head(),
                            fragment.boundary().clone(),
                            &checkpoints,
                            blob,
                        )
                        .await
                }
                .map_err(WasmWriteError::from)?;
                Ok(())
            },
        )
    }

    fn batch(
        &self,
        id: &JsValue,
        input: &JsValue,
        options: &JsValue,
        broadcast: bool,
        built: bool,
    ) -> js_sys::Promise {
        self.admit(
            || {
                let id = p::tree(id, "id")?;
                p::record(input, "batch")?;
                let commits = p::field(input, "commits")?;
                let fragments = p::field(input, "fragments")?;
                let commits = optional_array(&commits, "commits")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        if built {
                            p::built_commit_input(id, v, &format!("commits[{i}]"))
                        } else {
                            p::commit_input(id, v, &format!("commits[{i}]"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fragments = optional_array(&fragments, "fragments")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        if built {
                            p::built_fragment_input(id, v, &format!("fragments[{i}]"))
                        } else {
                            p::fragment_input(id, v, &format!("fragments[{i}]"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((id, commits, fragments, timeout(options)?))
            },
            move |node, (id, commits, fragments, timeout)| async move {
                if broadcast {
                    let result = node
                        .core
                        .add_built_batch(
                            id,
                            commits,
                            fragments,
                            CallTimeout::from(timeout.map(u64::from)),
                        )
                        .await
                        .map_err(WasmWriteError::from)?;
                    Ok(result.output())
                } else {
                    node.core
                        .store_built_batch(id, commits, fragments)
                        .await
                        .map_err(WasmWriteError::from)?;
                    Ok(JsValue::UNDEFINED)
                }
            },
        )
    }

    fn sedimentree(
        &self,
        id: &JsValue,
        tree: &JsValue,
        blobs: &JsValue,
        options: &JsValue,
        broadcast: bool,
    ) -> js_sys::Promise {
        self.admit(
            || {
                let id = p::tree(id, "id")?;
                p::record(tree, "tree")?;
                let commits = p::array(&p::field(tree, "commits")?, "commits")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| p::read_commit(v, &format!("commits[{i}]")))
                    .collect::<Result<Vec<_>, _>>()?;
                let fragments = p::array(&p::field(tree, "fragments")?, "fragments")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| p::read_fragment(v, &format!("fragments[{i}]")))
                    .collect::<Result<Vec<_>, _>>()?;
                if commits.iter().any(|c| c.sedimentree_id() != id)
                    || fragments.iter().any(|f| f.sedimentree_id() != id)
                {
                    return Err(p::invalid(
                        "tree",
                        "metadata belongs to a different sedimentree",
                    ));
                }
                let blobs = p::array(blobs, "blobs")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| p::bytes(v, &format!("blobs[{i}]")).map(Blob::new))
                    .collect::<Result<Vec<_>, _>>()?;
                // The core whole-tree path signs while pairing items. Check all
                // pairs here first so a late missing/mismatched blob cannot cause
                // partial signer side effects during validation.
                let metas: alloc::collections::BTreeSet<_> = blobs
                    .iter()
                    .map(sedimentree_core::blob::BlobMeta::new)
                    .collect();
                for (i, commit) in commits.iter().enumerate() {
                    if !metas.contains(commit.blob_meta()) {
                        return Err(p::invalid(
                            &format!("commits[{i}].blobMeta"),
                            "no matching blob supplied",
                        ));
                    }
                }
                for (i, fragment) in fragments.iter().enumerate() {
                    if !metas.contains(&fragment.summary().blob_meta()) {
                        return Err(p::invalid(
                            &format!("fragments[{i}].blobMeta"),
                            "no matching blob supplied",
                        ));
                    }
                }
                let tree = sedimentree_core::sedimentree::Sedimentree::new(fragments, commits);
                Ok((id, tree, blobs, timeout(options)?))
            },
            move |node, (id, tree, blobs, timeout)| async move {
                if broadcast {
                    let result = node
                        .core
                        .add_sedimentree(id, tree, blobs, CallTimeout::from(timeout.map(u64::from)))
                        .await
                        .map_err(WasmWriteError::from)?;
                    Ok(result.output())
                } else {
                    node.core
                        .store_sedimentree(id, tree, blobs)
                        .await
                        .map_err(WasmWriteError::from)?;
                    Ok(JsValue::UNDEFINED)
                }
            },
        )
    }

    fn topics(&self, input: &JsValue, subscribe: bool) -> js_sys::Promise {
        self.admit(
            || {
                p::array(input, "topics")?
                    .iter()
                    .enumerate()
                    .map(|(i, v)| Ok(Topic::new(p::unhex(v, &format!("topics[{i}]"))?)))
                    .collect::<Result<Vec<_>, JsValue>>()
            },
            move |node, topics| async move {
                if let Some(topics) = NonEmpty::from_vec(topics) {
                    if subscribe {
                        node.ephemeral_handler.subscribe(topics).await;
                    } else {
                        node.ephemeral_handler.unsubscribe(topics).await;
                    }
                }
                Ok(())
            },
        )
    }
}

#[wasm_bindgen(js_class = Subduction)]
impl SubductionApi {
    /// Construct a node from structural services and snapshotted scalar options.
    ///
    /// # Errors
    /// Rejects missing/invalid options and propagates property getter exceptions.
    #[wasm_bindgen(constructor)]
    pub fn new(options: &JsValue) -> Result<Self, JsValue> {
        p::record(options, "options")?;
        let signer = p::field(options, "signer")?;
        let storage = p::field(options, "storage")?;
        require_methods(&signer, "signer", &["sign", "verifyingKey"])?;
        // Catch malformed identities at construction rather than reaching the
        // infallible core Signer trait with an invalid public key.
        let key_method = p::field(&signer, "verifyingKey")?
            .dyn_into::<js_sys::Function>()
            .map_err(|_| p::invalid("signer.verifyingKey", "expected a function"))?;
        let key = p::bytes(&key_method.call0(&signer)?, "signer.verifyingKey()")?;
        let key: [u8; 32] = key
            .try_into()
            .map_err(|_| p::invalid("signer.verifyingKey()", "expected 32 bytes"))?;
        ed25519_dalek::VerifyingKey::from_bytes(&key)
            .map_err(|_| p::invalid("signer.verifyingKey()", "invalid Ed25519 public key"))?;
        require_methods(
            &storage,
            "storage",
            &[
                "saveSedimentreeId",
                "deleteSedimentreeId",
                "loadAllSedimentreeIds",
                "saveCommit",
                "loadCommit",
                "listCommitIds",
                "loadAllCommits",
                "deleteCommit",
                "deleteAllCommits",
                "saveFragment",
                "loadFragment",
                "listFragmentIds",
                "loadAllFragments",
                "deleteFragment",
                "deleteAllFragments",
                "saveBatchAll",
            ],
        )?;
        let mut fields = alloc::vec![("signer", signer), ("storage", storage)];
        for key in [
            "serviceName",
            "hashMetricOverride",
            "maxResidentTrees",
            "policy",
            "ephemeralPolicy",
            "onRemoteHeads",
            "onEphemeral",
            "defaultTimeoutMilliseconds",
        ] {
            let value = p::field(options, key)?;
            if !value.is_undefined() {
                match key {
                    "serviceName" => {
                        string(&value, key)?;
                    }
                    "maxResidentTrees" | "defaultTimeoutMilliseconds" => {
                        p::u32_value(&value, key)?;
                    }
                    "hashMetricOverride" | "onRemoteHeads" | "onEphemeral" => {
                        if !value.is_function() {
                            return Err(p::invalid(key, "expected a function"));
                        }
                    }
                    "policy" => require_methods(
                        &value,
                        key,
                        &[
                            "authorizeConnect",
                            "authorizeFetch",
                            "authorizePut",
                            "filterAuthorizedFetch",
                        ],
                    )?,
                    "ephemeralPolicy" => require_methods(
                        &value,
                        key,
                        &[
                            "authorizeSubscribe",
                            "authorizePublish",
                            "filterAuthorizedSubscribers",
                        ],
                    )?,
                    _ => {}
                }
                fields.push((key, value));
            }
        }
        let options = p::object(&fields).unchecked_into::<JsSubductionOptions>();
        Ok(Self(Rc::new(Node::new(&options)?)))
    }

    /// Begin bounded shutdown immediately; repeated calls observe the same result.
    pub fn close(&self) -> js_sys::Promise {
        self.0.close()
    }

    /// The caller-owned storage object supplied at construction.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn storage(&self) -> JsValue {
        self.0.storage()
    }

    /// Persist a raw commit locally without network work.
    #[wasm_bindgen(js_name = storeCommit)]
    pub fn store_commit(&self, id: &JsValue, commit: &JsValue) -> js_sys::Promise {
        self.commit(id, commit, false)
    }
    /// Persist a raw commit and best-effort push it to authorized subscribers.
    #[wasm_bindgen(js_name = addCommit)]
    pub fn add_commit(&self, id: &JsValue, commit: &JsValue) -> js_sys::Promise {
        self.commit(id, commit, true)
    }
    /// Persist a raw fragment using checkpoint prefixes, without network work.
    #[wasm_bindgen(js_name = storeFragment)]
    pub fn store_fragment(&self, id: &JsValue, fragment: &JsValue) -> js_sys::Promise {
        self.fragment(id, fragment, false)
    }
    /// Persist a fragment and best-effort push it to authorized subscribers.
    #[wasm_bindgen(js_name = addFragment)]
    pub fn add_fragment(&self, id: &JsValue, fragment: &JsValue) -> js_sys::Promise {
        self.fragment(id, fragment, true)
    }
    /// Validate and persist a mixed raw batch with one minimization.
    #[wasm_bindgen(js_name = storeBatch)]
    pub fn store_batch(&self, id: &JsValue, batch: &JsValue) -> js_sys::Promise {
        self.batch(id, batch, &JsValue::UNDEFINED, false, false)
    }
    /// Persist a raw batch and await broadcast, returning results keyed by peer.
    #[wasm_bindgen(js_name = addBatch)]
    pub fn add_batch(&self, id: &JsValue, batch: &JsValue, options: &JsValue) -> js_sys::Promise {
        self.batch(id, batch, options, true, false)
    }
    /// Validate metadata/blob agreement and persist a built batch locally.
    #[wasm_bindgen(js_name = storeBuiltBatch)]
    pub fn store_built_batch(&self, id: &JsValue, batch: &JsValue) -> js_sys::Promise {
        self.batch(id, batch, &JsValue::UNDEFINED, false, true)
    }
    /// Persist a validated built batch and await broadcast.
    #[wasm_bindgen(js_name = addBuiltBatch)]
    pub fn add_built_batch(
        &self,
        id: &JsValue,
        batch: &JsValue,
        options: &JsValue,
    ) -> js_sys::Promise {
        self.batch(id, batch, options, true, true)
    }
    /// Commit-only convenience form of `storeBatch`.
    #[wasm_bindgen(js_name = storeCommitsBatch)]
    pub fn store_commits_batch(&self, id: &JsValue, commits: &JsValue) -> js_sys::Promise {
        self.store_batch(id, &p::object(&[("commits", commits.clone())]))
    }
    /// Commit-only convenience form of `addBatch`.
    #[wasm_bindgen(js_name = addCommitsBatch)]
    pub fn add_commits_batch(
        &self,
        id: &JsValue,
        commits: &JsValue,
        options: &JsValue,
    ) -> js_sys::Promise {
        self.add_batch(id, &p::object(&[("commits", commits.clone())]), options)
    }
    /// Fragment-only convenience form of `storeBatch`.
    #[wasm_bindgen(js_name = storeFragmentsBatch)]
    pub fn store_fragments_batch(&self, id: &JsValue, fragments: &JsValue) -> js_sys::Promise {
        self.store_batch(id, &p::object(&[("fragments", fragments.clone())]))
    }
    /// Fragment-only convenience form of `addBatch`.
    #[wasm_bindgen(js_name = addFragmentsBatch)]
    pub fn add_fragments_batch(
        &self,
        id: &JsValue,
        fragments: &JsValue,
        options: &JsValue,
    ) -> js_sys::Promise {
        self.add_batch(id, &p::object(&[("fragments", fragments.clone())]), options)
    }

    /// Persist a whole metadata tree and its blobs locally.
    #[wasm_bindgen(js_name = storeSedimentree)]
    pub fn store_sedimentree(
        &self,
        id: &JsValue,
        tree: &JsValue,
        blobs: &JsValue,
    ) -> js_sys::Promise {
        self.sedimentree(id, tree, blobs, &JsValue::UNDEFINED, false)
    }
    /// Persist a whole metadata tree and await broadcast.
    #[wasm_bindgen(js_name = addSedimentree)]
    pub fn add_sedimentree(
        &self,
        id: &JsValue,
        tree: &JsValue,
        blobs: &JsValue,
        options: &JsValue,
    ) -> js_sys::Promise {
        self.sedimentree(id, tree, blobs, options, true)
    }
    /// Remove a tree and its associated local data.
    #[wasm_bindgen(js_name = removeSedimentree)]
    pub fn remove_sedimentree(&self, id: &JsValue) -> js_sys::Promise {
        self.admit(
            || p::tree(id, "id"),
            |node, id| async move { node.remove_sedimentree(&id).await },
        )
    }
    /// Read a local blob by digest, returning independent bytes.
    #[wasm_bindgen(js_name = getBlob)]
    pub fn get_blob(&self, id: &JsValue, digest: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((p::tree(id, "id")?, p::digest::<Blob>(digest, "digest")?)),
            |node, (id, digest)| async move { node.get_blob(&id, &digest).await },
        )
    }
    /// Read all local blobs for a tree.
    #[wasm_bindgen(js_name = getBlobs)]
    pub fn get_blobs(&self, id: &JsValue) -> js_sys::Promise {
        self.admit(
            || p::tree(id, "id"),
            |node, id| async move { node.get_blobs(&id).await },
        )
    }
    /// Collect local-first contents without subscribing to further updates.
    #[wasm_bindgen(js_name = fetchBlobs)]
    pub fn fetch_blobs(&self, id: &JsValue, options: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((p::tree(id, "id")?, timeout(options)?)),
            |node, (id, timeout)| async move { node.fetch_blobs(&id, timeout).await },
        )
    }
    /// Read local commit metadata as independently owned plain objects.
    #[wasm_bindgen(js_name = getCommits)]
    pub fn get_commits(&self, id: &JsValue) -> js_sys::Promise {
        self.admit(
            || p::tree(id, "id"),
            |node, id| async move { node.get_commits(&id).await },
        )
    }
    /// Read local fragment metadata as independently owned plain objects.
    #[wasm_bindgen(js_name = getFragments)]
    pub fn get_fragments(&self, id: &JsValue) -> js_sys::Promise {
        self.admit(
            || p::tree(id, "id"),
            |node, id| async move { node.get_fragments(&id).await },
        )
    }
    /// Read a tree's heads; unknown trees yield undefined.
    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&self, id: &JsValue) -> js_sys::Promise {
        self.admit(
            || p::tree(id, "id"),
            |node, id| async move { node.get_heads(&id).await },
        )
    }
    /// Read heads for every locally known tree.
    #[wasm_bindgen(js_name = getAllHeads)]
    pub fn get_all_heads(&self) -> js_sys::Promise {
        self.admit(
            || Ok(()),
            |node, ()| async move { node.get_all_heads().await },
        )
    }
    /// List locally known tree identifiers in canonical hex.
    #[wasm_bindgen(js_name = sedimentreeIds)]
    pub fn sedimentree_ids(&self) -> js_sys::Promise {
        self.admit(
            || Ok(()),
            |node, ()| async move { node.sedimentree_ids().await },
        )
    }
    /// List authenticated, connected peer identifiers in canonical hex.
    #[wasm_bindgen(js_name = getConnectedPeerIds)]
    pub fn connected_peer_ids(&self) -> js_sys::Promise {
        self.admit(
            || Ok(()),
            |node, ()| async move { node.connected_peer_ids().await },
        )
    }
    /// Compute the depth of a commit ID using this node's metric.
    ///
    /// # Errors
    /// Rejects an invalid commit ID.
    #[wasm_bindgen(js_name = commitDepth)]
    pub fn commit_depth(&self, id: &JsValue) -> Result<u32, JsValue> {
        Ok(self.0.core.commit_depth(p::commit_id(id, "commitId")?).0)
    }
    /// Observe local contents and subsequent storage changes without networking.
    ///
    /// # Errors
    /// Rejects invalid identifiers or construction after node shutdown.
    #[wasm_bindgen(js_name = watchStored)]
    pub fn watch_stored(&self, id: &JsValue) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.0.runtime.ensure_open()?;
        self.0.watch_stored(&p::tree(id, "id")?)
    }
    /// Load locally, then sync/subscribe on current and future connections.
    ///
    /// # Errors
    /// Rejects invalid identifiers, aborted signals, or construction after shutdown.
    #[wasm_bindgen(js_name = loadAndWatch)]
    pub fn load_and_watch(
        &self,
        id: &JsValue,
        options: Option<crate::stored::JsLoadAndWatchOptions>,
    ) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.0.runtime.ensure_open()?;
        self.0.load_and_watch(&p::tree(id, "id")?, options)
    }
    /// Sync one tree with a peer, awaiting local response processing.
    #[wasm_bindgen(js_name = syncWithPeer)]
    pub fn sync_with_peer(
        &self,
        peer: &JsValue,
        id: &JsValue,
        options: &JsValue,
    ) -> js_sys::Promise {
        self.admit(
            || {
                Ok((
                    read_peer(peer)?,
                    p::tree(id, "id")?,
                    subscribe(options)?,
                    timeout(options)?,
                ))
            },
            |node, (peer, id, subscribe, timeout)| async move {
                node.sync_with_peer(&peer, &id, subscribe, timeout).await
            },
        )
    }
    /// Sync one tree with all peers, returning a plain peer-keyed result record.
    #[wasm_bindgen(js_name = syncWithAllPeers)]
    pub fn sync_with_all_peers(&self, id: &JsValue, options: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((p::tree(id, "id")?, subscribe(options)?, timeout(options)?)),
            |node, (id, subscribe, timeout)| async move {
                node.sync_with_all_peers(&id, subscribe, timeout).await
            },
        )
    }
    /// Sync all known trees with a peer.
    #[wasm_bindgen(js_name = fullSyncWithPeer)]
    pub fn full_sync_with_peer(&self, peer: &JsValue, options: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((read_peer(peer)?, subscribe(options)?, timeout(options)?)),
            |node, (peer, subscribe, timeout)| async move {
                node.full_sync_with_peer(&peer, Some(subscribe), timeout)
                    .await
            },
        )
    }
    /// Sync all known trees with all peers.
    #[wasm_bindgen(js_name = fullSyncWithAllPeers)]
    pub fn full_sync_with_all_peers(&self, options: &JsValue) -> js_sys::Promise {
        self.admit(
            || timeout(options),
            |node, timeout| async move { node.full_sync_with_all_peers(timeout).await },
        )
    }
    /// Connect by WebSocket to a known identity, authenticating before registration.
    pub fn connect(&self, address: &JsValue, expected_peer_id: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((url(address)?, read_peer(expected_peer_id)?)),
            |node, (url, peer)| async move { node.connect(&url, &peer).await },
        )
    }
    /// Discover and authenticate a WebSocket peer.
    #[wasm_bindgen(js_name = connectDiscover)]
    pub fn connect_discover(&self, address: &JsValue, service_name: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((url(address)?, optional_string(service_name, "serviceName")?)),
            |node, (url, service)| async move { node.connect_discover(&url, service).await },
        )
    }
    /// Connect by HTTP long-poll to a known identity.
    #[wasm_bindgen(js_name = connectLongPoll)]
    pub fn connect_long_poll(
        &self,
        address: &JsValue,
        expected_peer_id: &JsValue,
    ) -> js_sys::Promise {
        self.admit(
            || Ok((string(address, "address")?, read_peer(expected_peer_id)?)),
            |node, (url, peer)| async move { node.connect_long_poll(&url, &peer).await },
        )
    }
    /// Discover and authenticate an HTTP long-poll peer.
    #[wasm_bindgen(js_name = connectDiscoverLongPoll)]
    pub fn connect_discover_long_poll(
        &self,
        address: &JsValue,
        service_name: &JsValue,
    ) -> js_sys::Promise {
        self.admit(|| Ok((string(address, "address")?, optional_string(service_name, "serviceName")?)), |node, (url, service)| async move { node.connect_discover_long_poll(&url, service).await })
    }
    /// Take ownership of a structural transport and run the initiator handshake.
    #[wasm_bindgen(js_name = connectTransport)]
    pub fn connect_transport(&self, transport: JsTransport, audience: &JsValue) -> js_sys::Promise {
        self.transport(transport, audience, false)
    }
    /// Take ownership of a structural transport and run the responder handshake.
    #[wasm_bindgen(js_name = acceptTransport)]
    pub fn accept_transport(&self, transport: JsTransport, audience: &JsValue) -> js_sys::Promise {
        self.transport(transport, audience, true)
    }
    /// Link two local nodes through an owned, authenticated `MessageChannel`.
    pub fn link(a: &Self, b: &Self) -> js_sys::Promise {
        let b = b.0.clone();
        a.admit(
            || {
                b.runtime.ensure_open()?;
                Ok(b)
            },
            |a, b| async move { Node::link(&a, &b).await },
        )
    }
    /// Disconnect peers without shutting down the node.
    #[wasm_bindgen(js_name = disconnectAll)]
    pub fn disconnect_all(&self) -> js_sys::Promise {
        self.admit(
            || Ok(()),
            |node, ()| async move { node.disconnect_all().await },
        )
    }
    /// Disconnect every connection to the specified peer.
    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub fn disconnect_from_peer(&self, peer: &JsValue) -> js_sys::Promise {
        self.admit(
            || read_peer(peer),
            |node, peer| async move { node.disconnect_from_peer(&peer).await },
        )
    }
    /// Publish opaque ephemeral bytes to a hex topic.
    #[wasm_bindgen(js_name = publishEphemeral)]
    pub fn publish_ephemeral(&self, topic: &JsValue, payload: &JsValue) -> js_sys::Promise {
        self.admit(
            || {
                Ok((
                    Topic::new(p::unhex(topic, "topic")?),
                    p::bytes(payload, "payload")?,
                ))
            },
            |node, (topic, payload)| async move { node.publish_ephemeral(&topic, &payload).await },
        )
    }
    /// Subscribe to ephemeral topics on connected peers.
    #[wasm_bindgen(js_name = subscribeEphemeral)]
    pub fn subscribe_ephemeral(&self, topics: &JsValue) -> js_sys::Promise {
        self.topics(topics, true)
    }
    /// Stop subscribing to ephemeral topics.
    #[wasm_bindgen(js_name = unsubscribeEphemeral)]
    pub fn unsubscribe_ephemeral(&self, topics: &JsValue) -> js_sys::Promise {
        self.topics(topics, false)
    }
    /// Register a structural keyhive handler, or unregister with null/undefined.
    #[wasm_bindgen(js_name = registerFrameHandler)]
    pub fn register_frame_handler(&self, handler: Option<JsFrameHandler>) -> js_sys::Promise {
        self.admit(
            || {
                if let Some(ref handler) = handler {
                    require_methods(
                        handler.as_ref(),
                        "handler",
                        &["onMessage", "onPeerDisconnect"],
                    )?;
                }
                Ok(handler)
            },
            |node, handler| async move { node.register_frame_handler(handler).await },
        )
    }
    /// Send opaque keyhive frame contents to a connected peer.
    #[wasm_bindgen(js_name = sendKeyhiveMessage)]
    pub fn send_keyhive_message(&self, payload: &JsValue, peer: &JsValue) -> js_sys::Promise {
        self.admit(
            || Ok((p::bytes(payload, "payload")?, read_peer(peer)?)),
            |node, (payload, peer)| async move { node.send_keyhive_message(&payload, &peer).await },
        )
    }
}

impl SubductionApi {
    fn transport(
        &self,
        transport: JsTransport,
        audience: &JsValue,
        accept: bool,
    ) -> js_sys::Promise {
        self.admit(
            || {
                require_methods(
                    transport.as_ref(),
                    "transport",
                    &["sendBytes", "recvBytes", "disconnect", "onDisconnect"],
                )?;
                p::record(audience, "audience")?;
                let expected = p::field(audience, "expectedPeerId")?;
                let service = p::field(audience, "serviceName")?;
                let audience = if expected.is_undefined() {
                    (None, Some(string(&service, "serviceName")?))
                } else {
                    if !service.is_undefined() {
                        return Err(p::invalid(
                            "audience",
                            "choose expectedPeerId or serviceName, not both",
                        ));
                    }
                    (Some(read_peer(&expected)?), None)
                };
                // Getters may reenter close. Only take ownership after validation,
                // and before returning the promise, so immediate shutdown owns it.
                self.0.runtime.ensure_open()?;
                let lease = self.0.runtime.own(transport.clone());
                Ok((transport, audience, lease))
            },
            move |node, (transport, (peer, service), lease)| async move {
                let auth = match (accept, peer, service) {
                    (false, Some(peer), _) => {
                        authenticated::setup(transport, node.core.signer(), &peer).await
                    }
                    (true, Some(peer), _) => {
                        let auth =
                            authenticated::accept(transport, node.core.signer(), None).await?;
                        if auth.peer_id() != peer {
                            return Err(WasmHandshakeError::InvalidMessage(
                                "authenticated peer identity mismatch".into(),
                            )
                            .into());
                        }
                        Ok(auth)
                    }
                    (false, _, service) => {
                        authenticated::setup_discover(transport, node.core.signer(), service).await
                    }
                    (true, _, Some(service)) => {
                        authenticated::accept_discover(transport, node.core.signer(), service).await
                    }
                    _ => unreachable!(),
                }
                .map_err(WasmConnectError::from)?;
                let peer = auth.peer_id();
                node.register_transport(auth, &lease).await?;
                lease.disarm_cleanup();
                Ok(p::hex(peer.as_bytes()))
            },
        )
    }
}

fn require_methods(value: &JsValue, path: &str, names: &[&str]) -> Result<(), JsValue> {
    p::record(value, path)?;
    for name in names {
        if !p::field(value, name)?.is_function() {
            return Err(p::invalid(&format!("{path}.{name}"), "expected a function"));
        }
    }
    Ok(())
}
fn read_peer(id: &JsValue) -> Result<PeerId, JsValue> {
    Ok(PeerId::new(p::unhex(id, "peerId")?))
}
fn string(value: &JsValue, path: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| p::invalid(path, "expected a string"))
}
fn optional_string(value: &JsValue, path: &str) -> Result<Option<String>, JsValue> {
    if value.is_undefined() {
        Ok(None)
    } else {
        string(value, path).map(Some)
    }
}
fn optional_array(value: &JsValue, path: &str) -> Result<Vec<JsValue>, JsValue> {
    if value.is_undefined() {
        Ok(Vec::new())
    } else {
        p::array(value, path)
    }
}
fn option(options: &JsValue, name: &str) -> Result<JsValue, JsValue> {
    if options.is_undefined() {
        Ok(JsValue::UNDEFINED)
    } else {
        p::record(options, "options")?;
        p::field(options, name)
    }
}
fn timeout(options: &JsValue) -> Result<Option<u32>, JsValue> {
    let value = option(options, "timeoutMilliseconds")?;
    if value.is_undefined() {
        Ok(None)
    } else {
        p::u32_value(&value, "timeoutMilliseconds").map(Some)
    }
}
fn subscribe(options: &JsValue) -> Result<bool, JsValue> {
    let value = option(options, "subscribe")?;
    if value.is_undefined() {
        Ok(true)
    } else {
        value
            .as_bool()
            .ok_or_else(|| p::invalid("subscribe", "expected a boolean"))
    }
}
fn url(value: &JsValue) -> Result<web_sys::Url, JsValue> {
    if let Some(text) = value.as_string() {
        web_sys::Url::new(&text)
    } else if value.is_instance_of::<web_sys::Url>() {
        web_sys::Url::new(&value.unchecked_ref::<web_sys::Url>().href())
    } else {
        Err(p::invalid("address", "expected a URL or URL string"))
    }
}

trait Output {
    fn output(self) -> JsValue;
}
impl Output for () {
    fn output(self) -> JsValue {
        JsValue::UNDEFINED
    }
}
impl Output for bool {
    fn output(self) -> JsValue {
        self.into()
    }
}
impl Output for String {
    fn output(self) -> JsValue {
        self.into()
    }
}
impl Output for JsValue {
    fn output(self) -> JsValue {
        self
    }
}
impl Output for Uint8Array {
    fn output(self) -> JsValue {
        self.into()
    }
}
impl<T: Output> Output for Vec<T> {
    fn output(self) -> JsValue {
        p::values(self.into_iter().map(Output::output))
    }
}
impl<T: Output> Output for Option<T> {
    fn output(self) -> JsValue {
        self.map_or(JsValue::UNDEFINED, Output::output)
    }
}
impl Output for PeerId {
    fn output(self) -> JsValue {
        p::hex(self.as_bytes()).into()
    }
}
impl Output for CommitId {
    fn output(self) -> JsValue {
        p::hex(self.as_bytes()).into()
    }
}
impl Output for SedimentreeId {
    fn output(self) -> JsValue {
        p::hex(self.as_bytes()).into()
    }
}
impl Output for LooseCommit {
    fn output(self) -> JsValue {
        p::commit_metadata(&self)
    }
}
impl Output for Fragment {
    fn output(self) -> JsValue {
        p::fragment_metadata(&self)
    }
}
impl Output for Blob {
    fn output(self) -> JsValue {
        Uint8Array::from(self.as_slice()).into()
    }
}
impl Output for (SedimentreeId, Vec<CommitId>) {
    fn output(self) -> JsValue {
        p::object(&[("id", self.0.output()), ("heads", self.1.output())])
    }
}
impl Output for SyncStats {
    fn output(self) -> JsValue {
        p::object(&[
            ("commitsReceived", self.commits_received.into()),
            ("fragmentsReceived", self.fragments_received.into()),
            ("commitsSent", self.commits_sent.into()),
            ("fragmentsSent", self.fragments_sent.into()),
            ("totalReceived", self.total_received().into()),
            ("totalSent", self.total_sent().into()),
            ("isEmpty", self.is_empty().into()),
            (
                "remoteHeads",
                p::values(self.remote_heads.heads.iter().map(|id| (*id).output())),
            ),
        ])
    }
}
impl Output for PeerSyncResult {
    fn output(self) -> JsValue {
        let (success, stats, errors) = self;
        p::object(&[
            ("success", success.into()),
            ("stats", stats.output()),
            (
                "transportErrors",
                p::values(errors.into_iter().map(|(_, error)| {
                    p::object(&[
                        ("name", error.error_name().into()),
                        ("message", error.to_string().into()),
                    ])
                })),
            ),
        ])
    }
}
impl Output for PeerSyncResults {
    fn output(self) -> JsValue {
        let fields: Vec<_> = self
            .into_iter()
            .map(|(peer, result)| (p::hex(peer.as_bytes()), result.output()))
            .collect();
        p::object(
            &fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone()))
                .collect::<Vec<_>>(),
        )
    }
}
