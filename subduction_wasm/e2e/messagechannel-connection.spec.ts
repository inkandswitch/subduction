import { test, expect } from "@playwright/test";
import { URL } from "./config";

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

test.describe("MessageChannel Connection Tests", () => {
  test("should authenticate two peers via MessageChannel", async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { AuthenticatedTransport, MemorySigner, makeMessagePortConnection } = window.subduction;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const peerIdA = signerA.peerId();
        const peerIdB = signerB.peerId();

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        const [authA, authB] = await Promise.all([
          AuthenticatedTransport.setup(makeMessagePortConnection(channel.port1), signerA, peerIdB),
          AuthenticatedTransport.accept(makeMessagePortConnection(channel.port2), signerB),
        ]);

        return {
          authenticated: true,
          distinctPeers: peerIdA.toString() !== peerIdB.toString(),
          peerA_seesPeerB: authA.peerId.toString() === peerIdB.toString(),
          peerB_seesPeerA: authB.peerId.toString() === peerIdA.toString(),
          error: null,
        };
      } catch (error) {
        return {
          authenticated: false,
          distinctPeers: false,
          peerA_seesPeerB: false,
          peerB_seesPeerA: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });

    expect(result.error).toBeNull();
    expect(result.authenticated).toBe(true);
    expect(result.distinctPeers).toBe(true);
    expect(result.peerA_seesPeerB).toBe(true);
    expect(result.peerB_seesPeerA).toBe(true);
  });

  test("should onboard both peers via MessageChannel", async ({ page }) => {
    const result = await page.evaluate(async () => {
      const {
        AuthenticatedTransport, Subduction, MemoryStorage, MemorySigner,
        makeMessagePortConnection,
      } = window.subduction;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const peerIdA = signerA.peerId();
        const peerIdB = signerB.peerId();

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        const [authA, authB] = await Promise.all([
          AuthenticatedTransport.setup(makeMessagePortConnection(channel.port1), signerA, peerIdB),
          AuthenticatedTransport.accept(makeMessagePortConnection(channel.port2), signerB),
        ]);

        const syncerA = new Subduction(signerA, new MemoryStorage());
        const syncerB = new Subduction(signerB, new MemoryStorage());

        const [isNewA, isNewB] = await Promise.all([
          syncerA.addConnection(authA),
          syncerB.addConnection(authB),
        ]);

        const peersA = await syncerA.getConnectedPeerIds();
        const peersB = await syncerB.getConnectedPeerIds();

        return {
          onboarded: true,
          isNewA,
          isNewB,
          distinctPeers: peerIdA.toString() !== peerIdB.toString(),
          peerCountA: peersA.length,
          peerCountB: peersB.length,
          a_sees_b: peersA.length > 0 && peersA[0].toString() === peerIdB.toString(),
          b_sees_a: peersB.length > 0 && peersB[0].toString() === peerIdA.toString(),
          error: null,
        };
      } catch (error) {
        return {
          onboarded: false,
          isNewA: false,
          isNewB: false,
          distinctPeers: false,
          peerCountA: 0,
          peerCountB: 0,
          a_sees_b: false,
          b_sees_a: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });

    expect(result.error).toBeNull();
    expect(result.onboarded).toBe(true);
    expect(result.distinctPeers).toBe(true);
    expect(result.isNewA).toBe(true);
    expect(result.isNewB).toBe(true);
    expect(result.peerCountA).toBe(1);
    expect(result.peerCountB).toBe(1);
    expect(result.a_sees_b).toBe(true);
    expect(result.b_sees_a).toBe(true);
  });

  test("should reject handshake with wrong expected peer ID", async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { AuthenticatedTransport, MemorySigner, PeerId, makeMessagePortConnection } = window.subduction;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const wrongPeerId = new PeerId(new Uint8Array(32).fill(0xff));

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        await Promise.all([
          AuthenticatedTransport.setup(makeMessagePortConnection(channel.port1), signerA, wrongPeerId),
          AuthenticatedTransport.accept(makeMessagePortConnection(channel.port2), signerB),
        ]);

        return { rejected: false, error: null };
      } catch (error) {
        return {
          rejected: true,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });

    expect(result.rejected).toBe(true);
    expect(result.error).toBeTruthy();
  });
});
