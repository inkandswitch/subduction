# Threat Categories

## T1: Impersonation

**Threat:** Attacker claims to be a different peer.

**Mitigation:**
- Handshake requires Ed25519 signature on challenge
- `PeerId` derived from verifying key
- Response binds to specific challenge via digest

**Residual risk:** Private key compromise allows impersonation.

## T2: Replay Attack (Handshake)

**Threat:** Attacker captures and replays a valid `Signed<Challenge>` to establish a session as the victim.

**Mitigation:**
- 128-bit random nonce in each challenge
- Timestamp freshness check (configurable, default 60s)
- Nonce tracking rejects seen `(PeerId, Nonce)` pairs within drift window
- Challenge digest in response prevents response replay

**Residual risk:** Within the timestamp window, an attacker with a privileged network position could attempt replay before nonce is recorded. Mitigated by TLS (can't intercept) and short window.

## T3: Person-in-the-Middle (PITM)

**Threat:** Attacker intercepts and relays/modifies messages between peers.

**Mitigation:**
- TLS provides transport encryption and integrity
- Ed25519 signatures on handshake messages
- `Audience` field binds challenge to intended recipient

**Residual risk:** TLS termination proxy could observe traffic. Mitigated by application-layer encryption of document content.

## T4: Unauthorized Data Access

**Threat:** Peer requests data they shouldn't have access to.

**Mitigation:**
- `StoragePolicy::authorize_fetch()` checked before returning data
- `StoragePolicy::authorize_put()` checked before accepting data
- Empty diff returned for unauthorized requests (no information leak)

**Residual risk:** Policy misconfiguration could grant unintended access.

## T5: Denial of Service

**Threat:** Attacker floods server with requests to exhaust resources.

**Attack vectors:**
- Many handshake attempts (signature verification is CPU-intensive)
- Large sync requests
- Connection exhaustion

**Mitigation:**
- Cheap checks first (timestamp, audience) before signature verification
- Connection limits at transport layer
- TLS provides some protection (attacker must complete TLS handshake)

**Residual risk:** Determined attacker with resources can still DoS. Recommend rate limiting at load balancer.

## T6: Clock Manipulation

**Threat:** Attacker manipulates timestamps to bypass freshness checks or corrupt drift correction.

**Mitigation:**
- `MAX_PLAUSIBLE_DRIFT` (±10 minutes) bounds acceptable drift
- Drift correction only accepted from signed responses
- Unsigned rejections can only shift clock within plausible bounds

**Residual risk:** Attacker could cause one extra round-trip by injecting fake rejection. Acceptable tradeoff.

## T7: Nonce Exhaustion

**Threat:** Attacker fills nonce cache to cause legitimate nonces to be rejected.

**Mitigation:**
- Only successful handshakes record nonces (attacker can't fill cache with failed attempts)
- Generational buckets with TTL naturally expire old entries
- Per-peer tracking limits blast radius

**Residual risk:** None if nonce tracking only records successful handshakes.

## T8: Content Inference

**Threat:** Attacker infers document contents from metadata.

**Attack vectors:**
- Document size
- Sync frequency
- Number of commits/fragments
- Access patterns

**Mitigation:**
- Application-layer encryption of blob contents
- Sedimentree works with opaque, encrypted payloads
- Content-addressed storage reveals nothing about plaintext

**Residual risk:** Metadata analysis (size, timing) may reveal information. Consider padding for high-security applications.

## T9: Coverage Poisoning

**Threat:** A peer allowed to write a document makes relays stop offering
some of its commits to readers.

**Attack vectors:**
- A fragment whose head or checkpoints name a range of loose commits its blob
  does not carry. Minimization trusts fragment metadata (see
  [Coverage](../sedimentree.md#coverage)), so the range is no longer offered.
- A fragment with the same head as an honest one but a lower digest, which
  replaces it under the equivocation tiebreak.
- Either kind of fragment held by a *reader*. Fragments are matched by head,
  so a relay holding any fragment with that head does not send the reader
  the honest one, and a relay holding the head's range loose does not offer
  the range (see [Batch Sync](../sync/batch.md#diffing-algorithm)). A reader
  cannot upload the fragment, so no relay replaces it; it gets one from a
  relay that accepted it, and relay-to-relay sync passes it on.

**Mitigation:**
- Nothing is deleted: dropped items stay in storage and held, so a relay that
  later holds a fragment carrying them offers them again.

**Residual risk:** Nothing checks that a fragment's blob carries what its
metadata claims. Any writer of the document can deny commits to readers of a
relay that accepts its fragment, and to readers that hold the fragment, from
every relay. Cleaning up the relay does not heal readers that already hold it.

An honest Automerge fragment held without the fragments it was formed on
(arrival out of formation order) also leaves commits covered that no kept
fragment carries, until those fragments arrive. That needs no attacker; see
UNCARRIED under [Coverage](../sedimentree.md#coverage).
