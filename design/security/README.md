# Security

This directory contains security documentation for the Subduction protocol.

## Documents

| Document | Purpose |
|----------|---------|
| [threats.md](./threats.md) | Threat categories and mitigations |
| [rationale.md](./rationale.md) | Security rationale per protocol |

## Trust Assumptions

### What We Trust

| Component         | Assumption                                     |
|-------------------|------------------------------------------------|
| **Ed25519**       | Signatures are unforgeable                     |
| **BLAKE3**        | Collision-resistant, preimage-resistant        |
| **TLS**           | Transport encryption and integrity (when used) |
| **Local storage** | Not compromised by attacker                    |
| **Private keys**  | Kept secret by their owners                    |

### What We Don't Trust

| Component            | Assumption                                            |
|----------------------|-------------------------------------------------------|
| **Network**          | Attacker can observe, delay, drop, or inject messages |
| **Peers**            | May be malicious, compromised, or buggy               |
| **Clocks**           | May drift significantly (±10 minutes tolerated)       |
| **Server operators** | May attempt unauthorized access                       |

## Security Goals

1. **Authentication** — Know who you're talking to
2. **Integrity** — Detect tampering with messages
3. **Replay protection** — Reject replayed handshakes
4. **Authorization** — Enforce access control per document
5. **Confidentiality** — Data encrypted at rest and in transit (via TLS + application-layer encryption)

## Out of Scope

The following are explicitly not addressed by Subduction:

| Item                   | Reason                                  |
|------------------------|-----------------------------------------|
| **Key management**     | Delegated to application (e.g. Keyhive) |
| **Key revocation**     | Delegated to policy layer               |
| **Content encryption** | Application responsibility              |
| **Network anonymity**  | Use Tor/VPN if needed                   |
| **Forward secrecy**    | TLS provides this at transport layer    |
