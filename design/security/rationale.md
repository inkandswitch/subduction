# Protocol Security Rationale

Security properties of each protocol layer.

## Handshake

```mermaid
sequenceDiagram
    participant I as Initiator
    participant R as Responder

    I->>R: Signed<Challenge> { audience, timestamp, nonce }
    Note right of R: ✓ Verify signature
    Note right of R: ✓ Check audience
    Note right of R: ✓ Check timestamp freshness
    Note right of R: ✓ Check nonce not seen

    R->>I: Signed<Response> { challenge_digest, timestamp }
    Note left of I: ✓ Verify signature
    Note left of I: ✓ Check challenge_digest
```

**Security properties:**
- Mutual authentication (both parties sign)
- Replay protection (nonce + timestamp + digest binding)
- Audience binding (prevents misdirection)

## Batch Sync

```mermaid
sequenceDiagram
    participant A as Requester
    participant B as Responder

    A->>B: BatchSyncRequest { summary }
    Note right of B: ✓ Check authorization
    B->>A: BatchSyncResponse { Signed<Commit>, Signed<Fragment> }
    Note left of A: ✓ Verify signatures
    Note left of A: ✓ Author from signature
    Note left of A: ✓ Authorize by author
```

**Security properties:**
- Authorization checked before returning data
- **Signed payloads** — Commits and fragments are `Signed<T>` with author signature
- **Author verification** — Author identity comes from cryptographic signature, not sender claim
- Content-addressed integrity (BLAKE3 hash verification)
- CAS storage keyed by digest — prevents tampering
- No information leak on unauthorized request (empty diff)

## Incremental Sync

```mermaid
sequenceDiagram
    participant A as Sender
    participant B as Receiver

    A->>B: LooseCommit { Signed<Commit>, blob }
    Note right of B: ✓ Verify Ed25519 signature
    Note right of B: ✓ Extract author from signature
    Note right of B: ✓ Authorize by author (not sender)
    Note right of B: ✓ Verify content hash
```

**Security properties:**
- **Signature verification** — Ed25519 signature verified before processing
- **Author from signature** — Author identity is cryptographic, not trusted from sender
- **Authorization by author** — Policy checks author identity, sender is just transport
- Content integrity via BLAKE3 digest
- CAS storage keyed by digest — deduplication and tamper detection
- Idempotent (duplicates harmless)

> [!NOTE]
> The sender (connection peer) may differ from the author (signature issuer). A peer can relay data signed by others. Authorization is always checked against the _author_, not the _sender_. This enables multi-hop forwarding while maintaining authorship accountability.
