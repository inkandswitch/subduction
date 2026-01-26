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
