#!/usr/bin/env node
//
// Mint an atproto **service-auth** JWT for testing Subduction's admission gate.
//
// It logs into your PDS with an app password, then calls
// `com.atproto.server.getServiceAuth` to mint a short-lived JWT whose `aud` is
// the Subduction service DID. Present that JWT to the service as `?auth=<jwt>`.
//
// This is a DEV/TEST helper only — do not paste your real account password;
// create an app password at https://bsky.app/settings/app-passwords.
//
// Usage:
//   node scripts/mint-service-jwt.mjs \
//     --identifier you.bsky.social \
//     --password xxxx-xxxx-xxxx-xxxx \
//     [--aud did:web:subduct.io] \
//     [--pds https://bsky.social] \
//     [--server wss://subduct.io] \
//     [--room <hex>] \
//     [--lxm <lexicon-method>] \
//     [--exp <unix-seconds>]
//
// Prints the JWT and a ready-to-open example URL:
//   https://subduct.io/examples/... isn't hosted; use the standalone examples:
//   open examples/workspace/index.html?server=<server>&auth=<jwt>

const args = Object.fromEntries(
  process.argv.slice(2).reduce((acc, cur, i, arr) => {
    if (cur.startsWith("--")) acc.push([cur.slice(2), arr[i + 1]]);
    return acc;
  }, []),
);

const identifier = args.identifier ?? process.env.ATP_IDENTIFIER;
const password = args.password ?? process.env.ATP_PASSWORD;
const aud = args.aud ?? "did:web:subduct.io";
const pds = (args.pds ?? "https://bsky.social").replace(/\/+$/, "");
const server = (args.server ?? "wss://subduct.io").replace(/\/+$/, "");
const room = args.room ?? [...crypto.getRandomValues(new Uint8Array(32))].map((b) => b.toString(16).padStart(2, "0")).join("");

if (!identifier || !password) {
  console.error("error: --identifier and --password (app password) are required");
  console.error("       create an app password at https://bsky.app/settings/app-passwords");
  process.exit(1);
}

function xrpcUrl(method, query = {}) {
  const url = new URL(`${pds}/xrpc/${method}`);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  }
  return url;
}

async function main() {
  // 1. Log in (app password) to obtain an access token + DID.
  const sessionRes = await fetch(`${pds}/xrpc/com.atproto.server.createSession`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ identifier, password }),
  });
  if (!sessionRes.ok) {
    console.error(`createSession failed: ${sessionRes.status} ${await sessionRes.text()}`);
    process.exit(1);
  }
  const session = await sessionRes.json();

  // 2. Mint a service-auth JWT (aud = the Subduction service DID).
  const authUrl = xrpcUrl("com.atproto.server.getServiceAuth", {
    aud,
    lxm: args.lxm,
    exp: args.exp,
  });
  const authRes = await fetch(authUrl, {
    headers: { authorization: `Bearer ${session.accessJwt}` },
  });
  if (!authRes.ok) {
    console.error(`getServiceAuth failed: ${authRes.status} ${await authRes.text()}`);
    console.error("(some PDSes require --lxm; try e.g. --lxm com.atproto.identity.resolveHandle)");
    process.exit(1);
  }
  const { token } = await authRes.json();

  console.log("");
  console.log(`identity : ${session.did} (${session.handle})`);
  console.log(`aud      : ${aud}`);
  console.log("");
  console.log("token (JWT):");
  console.log(token);
  console.log("");
  console.log("ws URL:");
  console.log(`${server}/sync/${room}?auth=${token}`);
  console.log("");
  console.log("open the workspace example against a gated service, e.g.:");
  console.log(`  examples/workspace/index.html?server=${server}&auth=${token}`);
  console.log("");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
