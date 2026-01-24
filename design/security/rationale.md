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
    B->>A: BatchSyncResponse { diff }
```

**Security properties:**
- Authorization checked before returning data
- Content-addressed integrity (hash verification)
- No information leak on unauthorized request (empty diff)

## Incremental Sync

```mermaid
sequenceDiagram
    participant A as Sender
    participant B as Receiver

    A->>B: LooseCommit { commit, blob }
    Note right of B: ✓ Check authorization
    Note right of B: ✓ Verify content hash
```

**Security properties:**
- Authorization checked before accepting
- Content integrity via BLAKE3 digest
- Idempotent (duplicates harmless)
