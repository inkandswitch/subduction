# Guidance

## For Deployers

1. **Use TLS** — Always deploy behind TLS termination
2. **Rate limit** — Add rate limiting at load balancer
3. **Monitor** — Alert on unusual handshake failure rates
4. **Rotate keys** — Periodic key rotation limits compromise blast radius
5. **Audit policies** — Regularly review authorization rules

## For Application Developers

1. **Encrypt content** — Use application-layer encryption for sensitive data
2. **Validate policy** — Test authorization edge cases
3. **Handle errors** — Don't leak information in error messages
4. **Log security events** — Track failed auth attempts

## Further Reading

See [`design/`](./design/) for protocol documentation, security rationale, and threat model.
