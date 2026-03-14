import { test, expect } from "@playwright/test";
import { URL } from "./config";

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });

  // Inject shared helper into the page context
  await page.evaluate(() => {
    /**
     * Create a HandshakeConnection backed by a MessagePort.
     *
     * Implements both raw byte send/recv (for the handshake phase) and the
     * full Connection interface (for post-handshake communication) over the
     * same MessagePort.
     */
    (window as any).makeHandshakeConnection = (port: MessagePort) => {
      const incoming: any[] = [];
      const waiters: Array<(value: any) => void> = [];

      port.onmessage = (event: MessageEvent) => {
        if (waiters.length > 0) {
          waiters.shift()!(event.data);
        } else {
          incoming.push(event.data);
        }
      };

      function nextMessage<T>(): Promise<T> {
        if (incoming.length > 0) {
          return Promise.resolve(incoming.shift() as T);
        }
        return new Promise((resolve) => waiters.push(resolve));
      }

      return {
        async sendBytes(bytes: Uint8Array) {
          port.postMessage(bytes.buffer);
        },
        recvBytes() {
          return nextMessage<ArrayBuffer>().then((buf) => new Uint8Array(buf));
        },
        async disconnect() { port.close(); },
        async send(message: any) { port.postMessage(message); },
        recv: nextMessage,
        async nextRequestId() { throw new Error("not implemented"); },
        async call() { throw new Error("not implemented"); },
      };
    };
  });
});

test.describe("MessageChannel Connection Tests", () => {
  test("should authenticate two peers via MessageChannel", async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { AuthenticatedConnection, MemorySigner } = window.subduction;
      const makeConn = (window as any).makeHandshakeConnection;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const peerIdA = signerA.peerId();
        const peerIdB = signerB.peerId();

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        const [authA, authB] = await Promise.all([
          AuthenticatedConnection.setup(makeConn(channel.port1), signerA, peerIdB),
          AuthenticatedConnection.accept(makeConn(channel.port2), signerB),
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
        AuthenticatedConnection, Subduction, MemoryStorage, MemorySigner,
      } = window.subduction;
      const makeConn = (window as any).makeHandshakeConnection;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const peerIdA = signerA.peerId();
        const peerIdB = signerB.peerId();

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        const [authA, authB] = await Promise.all([
          AuthenticatedConnection.setup(makeConn(channel.port1), signerA, peerIdB),
          AuthenticatedConnection.accept(makeConn(channel.port2), signerB),
        ]);

        const syncerA = new Subduction(signerA, new MemoryStorage());
        const syncerB = new Subduction(signerB, new MemoryStorage());

        const [isNewA, isNewB] = await Promise.all([
          syncerA.onboard(authA),
          syncerB.onboard(authB),
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
      const { AuthenticatedConnection, MemorySigner, PeerId } = window.subduction;
      const makeConn = (window as any).makeHandshakeConnection;

      try {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();

        const wrongPeerId = new PeerId(new Uint8Array(32).fill(0xff));

        const channel = new MessageChannel();
        channel.port1.start();
        channel.port2.start();

        await Promise.all([
          AuthenticatedConnection.setup(makeConn(channel.port1), signerA, wrongPeerId),
          AuthenticatedConnection.accept(makeConn(channel.port2), signerB),
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
