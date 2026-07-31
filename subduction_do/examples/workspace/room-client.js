// RoomClient — resilient, multiplexed client for the Subduction Durable Object
// "room" routing seam.
//
// The service routes `/sync/<room>` to one Durable Object per `<room>`, where
// `<room>` is an *opaque grouping key*. Because the wire protocol tags every
// frame with its `SedimentreeId`, a single connection to a room can carry any
// number of documents. This class owns exactly **one WebSocket per room** and
// multiplexes every tracked document over it, so a workspace of N documents
// costs one connection + one handshake instead of N.
//
// It is deliberately framework-free and depends only on the public
// `subduction_wasm` surface:
//   * `SubductionWebSocket.tryDiscover(url, signer, serviceName, onDisconnect)`
//   * `subduction.addConnection(transport)`
//   * `subduction.syncWithPeer(peerId, id, subscribe, timeoutMs)`
//   * `subduction.disconnectFromPeer(peerId)`
//
// Resilience model:
//   * A dropped socket (`onDisconnect`) triggers reconnect with exponential
//     backoff + jitter (bounded), and on every (re)connect it re-runs the
//     discovery handshake and re-syncs+re-subscribes *all* tracked documents —
//     so no subscription is silently lost across a drop or a Durable Object
//     hibernation.
//   * There is no heartbeat/keepalive: we only reconnect in response to an
//     actual close, so an idle room is free to hibernate instead of being
//     polled awake.

/** Default reconnect backoff schedule. */
export const DEFAULT_BACKOFF = Object.freeze({
  baseMs: 500,
  maxMs: 15_000,
  factor: 2,
  jitter: 0.2, // ± fraction applied to each delay
});

/**
 * @typedef {"idle"|"connecting"|"connected"|"reconnecting"|"closed"} RoomState
 */

export class RoomClient {
  /**
   * @param {object} opts
   * @param {any}    opts.subduction   Shared Subduction instance (owns storage + identity).
   * @param {any}    opts.signer       Signer used for the discovery handshake.
   * @param {URL}    opts.url          Full ws URL for the room, e.g. `new URL("ws://host/sync/<room>")`.
   * @param {string} opts.serviceName  Discovery service name (must match the server's SERVICE_NAME).
   * @param {any}    opts.SubductionWebSocket  The wasm `SubductionWebSocket` class (injected).
   * @param {number} [opts.syncTimeoutMs=15000]
   * @param {object} [opts.backoff=DEFAULT_BACKOFF]
   * @param {(state: RoomState, info: object) => void} [opts.onStatus]
   * @param {(error: unknown) => void} [opts.onError]
   */
  constructor({
    subduction,
    signer,
    url,
    serviceName,
    SubductionWebSocket,
    syncTimeoutMs = 15_000,
    backoff = DEFAULT_BACKOFF,
    onStatus = () => {},
    onError = () => {},
  }) {
    if (!subduction || !signer || !url || !SubductionWebSocket) {
      throw new Error("RoomClient: subduction, signer, url and SubductionWebSocket are required");
    }
    this._subduction = subduction;
    this._signer = signer;
    this._url = url;
    this._serviceName = serviceName;
    this._SubductionWebSocket = SubductionWebSocket;
    this._syncTimeoutMs = syncTimeoutMs;
    this._backoff = { ...DEFAULT_BACKOFF, ...backoff };
    this._onStatus = onStatus;
    this._onError = onError;

    /** @type {Map<string, any>} doc id string -> SedimentreeId */
    this._docs = new Map();
    /** @type {RoomState} */
    this._state = "idle";
    this._serverPeerId = null;
    this._backoffMs = this._backoff.baseMs;
    this._reconnects = 0;
    this._timer = null;
    this._closed = false;
    // Set while we intentionally tear a connection down, so the resulting
    // `onDisconnect` isn't mistaken for a real drop and doesn't self-trigger a
    // reconnect.
    this._suppressDrop = false;
  }

  /** @returns {RoomState} */
  get state() {
    return this._state;
  }

  /** The verified server peer id for this room, or null when not connected. */
  get serverPeerId() {
    return this._serverPeerId;
  }

  /** Number of reconnect attempts made since construction. */
  get reconnects() {
    return this._reconnects;
  }

  /** The documents currently tracked (kept synced + subscribed) on this room. */
  docIds() {
    return [...this._docs.values()];
  }

  /**
   * Track a document on this room: keep it synced and subscribed for as long as
   * the room is open. Safe to call before or after {@link open}; if already
   * connected it syncs immediately, otherwise it is picked up on next connect.
   * Idempotent per document id.
   * @param {any} id SedimentreeId
   */
  async addDoc(id) {
    this._docs.set(id.toString(), id);
    if (this._state === "connected") {
      await this._syncOne(id);
    }
  }

  /**
   * Stop tracking a document. The client simply stops re-subscribing it on
   * reconnect (the wire protocol has no explicit unsubscribe; the server prunes
   * a peer's subscriptions when its socket closes).
   * @param {any} id SedimentreeId
   */
  removeDoc(id) {
    this._docs.delete(id.toString());
  }

  /**
   * Open the room connection (idempotent). Establishes one socket, handshakes,
   * and syncs every tracked document over it.
   */
  async open() {
    if (this._state === "connecting" || this._state === "connected" || this._state === "reconnecting") {
      return;
    }
    this._closed = false;
    this._setState("connecting");
    await this._connect();
  }

  /**
   * Manually reconcile tracked documents (e.g. after waking a hidden tab).
   * Reconciliation normally rides the server's push to subscribers, so this is
   * only a backstop. No-op unless connected.
   * @param {any} [id] Sync just this doc; omit to sync all tracked docs.
   */
  async sync(id) {
    if (this._state !== "connected") return;
    if (id) {
      await this._syncOne(id);
    } else {
      await Promise.all(this.docIds().map((d) => this._syncOne(d)));
    }
  }

  /**
   * Deliberately sever and rebuild the connection: tear the socket down, then
   * re-run the exact recovery path used for real drops (fresh handshake +
   * re-subscribe of every tracked doc). Useful to demonstrate/verify recovery.
   */
  async forceReconnect() {
    if (this._closed) return;
    this._reconnects += 1;
    this._setState("reconnecting", { attempt: this._reconnects, forced: true });
    await this._teardown();
    await this._connect();
  }

  /** Permanent shutdown: stop reconnecting and drop the connection. */
  async close() {
    this._closed = true;
    this._clearTimer();
    await this._teardown();
    this._setState("closed");
  }

  // --- internals ---------------------------------------------------------

  async _connect() {
    try {
      // Low-level discover (vs the high-level `connectDiscover`) so we can wire
      // an `onDisconnect` callback for reconnection.
      const authed = await this._SubductionWebSocket.tryDiscover(
        this._url,
        this._signer,
        this._serviceName,
        () => this._onDrop(),
      );
      // Read the verified peer id *before* `toTransport()`, which consumes the
      // socket wrapper (`to_transport(self)`) — touching `authed` afterwards
      // dereferences a freed wasm pointer. `addConnection` only borrows the
      // transport; the underlying browser WebSocket (and its onclose ->
      // onDisconnect wiring) lives on inside Subduction.
      const serverPeerId = authed.peerId;
      await this._subduction.addConnection(authed.toTransport());
      this._serverPeerId = serverPeerId;
      this._backoffMs = this._backoff.baseMs; // healthy connection resets backoff
      this._setState("connected", { serverPeerId: this._serverPeerId });

      // Re-sync + re-subscribe every tracked doc over the (new) socket. This is
      // what makes reconnection lossless: subscriptions are re-established and
      // anything missed during the outage is pulled.
      await Promise.all(this.docIds().map((d) => this._syncOne(d)));
    } catch (e) {
      this._onError(e);
      this._scheduleReconnect();
    }
  }

  async _syncOne(id) {
    if (!this._serverPeerId) return;
    try {
      await this._subduction.syncWithPeer(this._serverPeerId, id, true, this._syncTimeoutMs);
    } catch (e) {
      this._onError(e);
    }
  }

  _onDrop() {
    if (this._closed || this._suppressDrop) return;
    this._serverPeerId = null;
    this._scheduleReconnect();
  }

  _scheduleReconnect() {
    if (this._closed || this._timer) return;
    const spread = 1 + (Math.random() * 2 - 1) * this._backoff.jitter;
    const delay = Math.min(this._backoffMs, this._backoff.maxMs) * spread;
    this._reconnects += 1;
    this._setState("reconnecting", { attempt: this._reconnects, inMs: Math.round(delay) });
    this._timer = setTimeout(() => {
      this._timer = null;
      // Grow backoff for the *next* failure; a success in `_connect` resets it.
      this._backoffMs = Math.min(this._backoffMs * this._backoff.factor, this._backoff.maxMs);
      void this._connect();
    }, delay);
  }

  async _teardown() {
    this._clearTimer();
    if (this._serverPeerId) {
      this._suppressDrop = true;
      try {
        await this._subduction.disconnectFromPeer(this._serverPeerId);
      } catch (e) {
        // Already gone — nothing to do.
      }
      this._suppressDrop = false;
      this._serverPeerId = null;
    }
  }

  _clearTimer() {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
  }

  _setState(state, info = {}) {
    this._state = state;
    try {
      this._onStatus(state, info);
    } catch (e) {
      // A misbehaving status handler must not break connection management.
      this._onError(e);
    }
  }
}
