// Generate keyhive_wasm interop vectors for `tests/keyhive_wasm_interop.rs`.
//
// A browser client builds what it sends the server with keyhive_wasm, so the
// server must decode keyhive_wasm's bytes. These vectors are those bytes:
// signed bytes, a contact card, and the events Alice would sync to a server
// relaying her document, including a revocation that retains content heads
// (which carry `ChangeId`s).
//
// Build keyhive_wasm for Node at the release `Cargo.toml` uses, then run:
//
//   wasm-pack build --dev --target nodejs keyhive_wasm   # in the keyhive repo
//   node generate.cjs <keyhive repo>/keyhive_wasm/pkg > vectors.txt
//
// Keys, prekeys, and signatures are fresh each run, so regenerating changes
// every line. The checked-in vectors come from keyhive_wasm 0.6.0, the
// release alongside `keyhive_core` 0.10.0.
//
// If wasm-pack can't find a matching wasm-bindgen CLI, lower keyhive's
// `wasm-bindgen` pin to the CLI's version: that changes only the JS glue, not
// the bytes.

const path = require("node:path");

const kh = require(path.resolve(process.argv[2], "keyhive_wasm.js"));

const hex = (bytes) => Buffer.from(bytes).toString("hex");
const filled = (byte) => new Uint8Array(32).fill(byte);

const keyhive = (seed) =>
  kh.Keyhive.init(
    kh.Signer.memorySignerFromBytes(filled(seed)),
    kh.CiphertextStore.newInMemory(),
    () => {},
  );

async function main() {
  const alice = await keyhive(0xa1);
  const bob = await keyhive(0xb0);
  const server = await keyhive(0x5e);
  await alice.receiveContactCard(await bob.contactCard());
  await alice.receiveContactCard(await server.contactCard());

  const payload = new TextEncoder().encode("subduction keyhive_wasm interop");
  const signed = await alice.trySign(payload);
  const card = await alice.getExistingContactCard();

  const doc = await alice.generateDocument([], new kh.ChangeId(filled(1)), []);
  const membered = kh.MemberedId.document(doc);
  await alice.addMember(server.id.toIdentifier(), membered, kh.Power.relay());
  await alice.addMember(bob.id.toIdentifier(), membered, kh.Power.read());
  await alice.setContentHeads(doc, [new kh.ChangeId(filled(2))]);
  await alice.revokeMember(bob.id.toIdentifier(), true, membered);

  const relay = await alice.getPrincipal(server.id.toIdentifier());
  const events = await alice.eventsForAgent(relay);

  const lines = [
    "# keyhive_wasm interop vectors: see generate.cjs",
    `alice=${hex(alice.id.toBytes())}`,
    `bob=${hex(bob.id.toBytes())}`,
    `server=${hex(server.id.toBytes())}`,
    `document=${hex(doc.toBytes())}`,
    `payload=${hex(payload)}`,
    `signed=${hex(signed.toBytes())}`,
    `contact_card=${hex(card.toBytes())}`,
    ...[...events.values()].map((event) => `event=${hex(event)}`),
  ];
  process.stdout.write(`${lines.join("\n")}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
