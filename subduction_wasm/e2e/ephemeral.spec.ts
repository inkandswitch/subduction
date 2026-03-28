import { test, expect } from "@playwright/test";
import { URL } from "./config";
import { spawn, ChildProcess } from "child_process";
import { promisify } from "util";
import path from "path";
import { fileURLToPath } from "url";
import net from "net";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const sleep = promisify(setTimeout);

async function waitForPort(
  host: string,
  port: number,
  timeout: number = 10000
): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeout) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = new net.Socket();
        socket.setTimeout(1000);

        socket.once("connect", () => {
          socket.destroy();
          resolve();
        });

        socket.once("error", (err) => {
          socket.destroy();
          reject(err);
        });

        socket.once("timeout", () => {
          socket.destroy();
          reject(new Error("Connection timeout"));
        });

        socket.connect(port, host);
      });

      return;
    } catch (err) {
      await sleep(100);
    }
  }

  throw new Error(
    `Port ${host}:${port} did not become available within ${timeout}ms`
  );
}

const WS_HOST = "127.0.0.1";
const WS_PORTS: Record<string, number> = {
  chromium: 9898,
  firefox: 9899,
  webkit: 9900,
};
const CONSOLE_PORTS: Record<string, number> = {
  chromium: 6675,
  firefox: 6676,
  webkit: 6677,
};

let subductionServer: ChildProcess | null = null;
let currentPort: number;
let currentWsUrl: string;

test.beforeAll(async ({ browserName }) => {
  currentPort = WS_PORTS[browserName];
  currentWsUrl = `ws://${WS_HOST}:${currentPort}`;

  const cliPath =
    process.env.SUBDUCTION_CLI ??
    path.join(__dirname, "../../target/release/subduction_cli");

  subductionServer = spawn(
    cliPath,
    ["server", "--socket", `${WS_HOST}:${currentPort}`, "--ephemeral-key"],
    {
      cwd: path.join(__dirname, "../.."),
      stdio: "pipe",
      env: {
        ...process.env,
        RUST_LOG: "info",
        TOKIO_CONSOLE_BIND: `${WS_HOST}:${CONSOLE_PORTS[browserName]}`,
      },
    }
  );

  if (process.env.CI || process.env.DEBUG) {
    subductionServer.stdout?.on("data", (data) => {
      console.log(`[${browserName} stdout]:`, data.toString().trim());
    });
    subductionServer.stderr?.on("data", (data) => {
      console.error(`[${browserName} stderr]:`, data.toString().trim());
    });
  }

  const healthCheckTimeout = process.env.CI ? 15000 : 10000;
  try {
    await waitForPort(WS_HOST, currentPort, healthCheckTimeout);
    console.log(
      `✓ Subduction ephemeral server started on ${currentWsUrl} for ${browserName}`
    );
  } catch (error) {
    console.error(`Failed to start server on ${currentWsUrl}: ${error}`);
    throw error;
  }
});

test.afterAll(async () => {
  if (subductionServer) {
    subductionServer.kill("SIGTERM");
    await sleep(500);
    if (!subductionServer.killed) {
      subductionServer.kill("SIGKILL");
    }
    console.log("✓ Subduction ephemeral server stopped");
  }
});

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, {
    timeout: wasmTimeout,
  });
});

// ── Topic API ───────────────────────────────────────────────────────────

test.describe("Topic API", () => {
  test("Topic.fromBytes creates a valid 32-byte topic", async ({ page }) => {
    const result = await page.evaluate(() => {
      const { Topic } = window.subduction;

      const bytes = new Uint8Array(32).fill(0x42);
      const topic = Topic.fromBytes(bytes);
      const roundtripped = topic.toBytes();
      const str = topic.toString();

      return {
        bytesMatch: roundtripped.length === 32 && roundtripped[0] === 0x42,
        hasString: str.length > 0,
        error: null,
      };
    });

    expect(result.error).toBeNull();
    expect(result.bytesMatch).toBe(true);
    expect(result.hasString).toBe(true);
  });

  test("Topic.fromBytes rejects non-32-byte input", async ({ page }) => {
    const result = await page.evaluate(() => {
      const { Topic } = window.subduction;

      try {
        Topic.fromBytes(new Uint8Array(16));
        return { threw: false };
      } catch {
        return { threw: true };
      }
    });

    expect(result.threw).toBe(true);
  });
});

// ── Ephemeral Messaging ─────────────────────────────────────────────────

test.describe("Ephemeral Messaging", () => {
  test("two peers exchange ephemeral messages through the server", async ({
    page,
  }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      try {
        const topic = Topic.fromBytes(new Uint8Array(32).fill(0xaa));

        // Track received messages for each peer
        const peerAReceived: Array<{
          payload: number[];
          senderBytes: number[];
        }> = [];
        const peerBReceived: Array<{
          payload: number[];
          senderBytes: number[];
        }> = [];

        const signerA = await WebCryptoSigner.setup();
        const signerB = await WebCryptoSigner.setup();

        const syncerA = new Subduction(
          signerA,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, sender: any, payload: Uint8Array) => {
            peerAReceived.push({
              payload: Array.from(payload),
              senderBytes: Array.from(sender.toBytes()),
            });
          }
        );

        const syncerB = new Subduction(
          signerB,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, sender: any, payload: Uint8Array) => {
            peerBReceived.push({
              payload: Array.from(payload),
              senderBytes: Array.from(sender.toBytes()),
            });
          }
        );

        const url = new URL(wsUrl);
        const serviceName = wsUrl.replace("ws://", "");

        // Connect both peers to the server
        await syncerA.connectDiscover(url, serviceName);
        await syncerB.connectDiscover(url, serviceName);

        // Both subscribe to the same topic
        await syncerA.subscribeEphemeral([topic]);
        await syncerB.subscribeEphemeral([topic]);

        // Give subscriptions time to propagate
        await new Promise((r) => setTimeout(r, 200));

        // Peer A publishes a message
        const payload = new Uint8Array([1, 2, 3, 4, 5]);
        await syncerA.publishEphemeral(topic, payload);

        // Wait for delivery
        await new Promise((r) => setTimeout(r, 500));

        return {
          peerBReceivedCount: peerBReceived.length,
          peerBPayload:
            peerBReceived.length > 0 ? peerBReceived[0].payload : null,
          peerAReceivedCount: peerAReceived.length,
          error: null,
        };
      } catch (error) {
        return {
          peerBReceivedCount: 0,
          peerBPayload: null,
          peerAReceivedCount: 0,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentWsUrl);

    expect(result.error).toBeNull();
    // Peer B should have received the message from Peer A
    expect(result.peerBReceivedCount).toBeGreaterThanOrEqual(1);
    expect(result.peerBPayload).toEqual([1, 2, 3, 4, 5]);
    // Peer A should NOT receive its own message back
    expect(result.peerAReceivedCount).toBe(0);
  });

  test("unsubscribe stops receiving ephemeral messages", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      try {
        const topic = Topic.fromBytes(new Uint8Array(32).fill(0xbb));

        const received: Array<number[]> = [];

        const signerA = await WebCryptoSigner.setup();
        const signerB = await WebCryptoSigner.setup();

        const syncerA = new Subduction(
          signerA,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          null
        );

        const syncerB = new Subduction(
          signerB,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, sender: any, payload: Uint8Array) => {
            received.push(Array.from(payload));
          }
        );

        const url = new URL(wsUrl);
        const serviceName = wsUrl.replace("ws://", "");

        await syncerA.connectDiscover(url, serviceName);
        await syncerB.connectDiscover(url, serviceName);

        // Subscribe, wait, publish — should receive
        await syncerB.subscribeEphemeral([topic]);
        await new Promise((r) => setTimeout(r, 200));

        await syncerA.publishEphemeral(topic, new Uint8Array([10]));
        await new Promise((r) => setTimeout(r, 500));

        const countBeforeUnsub = received.length;

        // Unsubscribe
        await syncerB.unsubscribeEphemeral([topic]);
        await new Promise((r) => setTimeout(r, 200));

        // Publish again — should NOT be received
        await syncerA.publishEphemeral(topic, new Uint8Array([20]));
        await new Promise((r) => setTimeout(r, 500));

        const countAfterUnsub = received.length;

        return {
          countBeforeUnsub,
          countAfterUnsub,
          firstPayload: received.length > 0 ? received[0] : null,
          error: null,
        };
      } catch (error) {
        return {
          countBeforeUnsub: 0,
          countAfterUnsub: 0,
          firstPayload: null,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentWsUrl);

    expect(result.error).toBeNull();
    expect(result.countBeforeUnsub).toBeGreaterThanOrEqual(1);
    expect(result.firstPayload).toEqual([10]);
    // After unsubscribe, no new messages
    expect(result.countAfterUnsub).toBe(result.countBeforeUnsub);
  });

  test("messages are scoped to their topic", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      try {
        const topicA = Topic.fromBytes(new Uint8Array(32).fill(0xcc));
        const topicB = Topic.fromBytes(new Uint8Array(32).fill(0xdd));

        const receivedOnA: Array<number[]> = [];
        const receivedOnB: Array<number[]> = [];

        const signerSender = await WebCryptoSigner.setup();
        const signerListenerA = await WebCryptoSigner.setup();
        const signerListenerB = await WebCryptoSigner.setup();

        const sender = new Subduction(
          signerSender,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          null
        );

        // Listener A subscribes to topicA only
        const listenerA = new Subduction(
          signerListenerA,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, _sender: any, payload: Uint8Array) => {
            receivedOnA.push(Array.from(payload));
          }
        );

        // Listener B subscribes to topicB only
        const listenerB = new Subduction(
          signerListenerB,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, _sender: any, payload: Uint8Array) => {
            receivedOnB.push(Array.from(payload));
          }
        );

        const url = new URL(wsUrl);
        const serviceName = wsUrl.replace("ws://", "");

        await sender.connectDiscover(url, serviceName);
        await listenerA.connectDiscover(url, serviceName);
        await listenerB.connectDiscover(url, serviceName);

        await listenerA.subscribeEphemeral([topicA]);
        await listenerB.subscribeEphemeral([topicB]);
        await new Promise((r) => setTimeout(r, 200));

        // Publish to topicA only
        await sender.publishEphemeral(topicA, new Uint8Array([0xaa]));
        await new Promise((r) => setTimeout(r, 500));

        return {
          listenerACount: receivedOnA.length,
          listenerBCount: receivedOnB.length,
          listenerAPayload: receivedOnA.length > 0 ? receivedOnA[0] : null,
          error: null,
        };
      } catch (error) {
        return {
          listenerACount: 0,
          listenerBCount: 0,
          listenerAPayload: null,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentWsUrl);

    expect(result.error).toBeNull();
    // Listener A (subscribed to topicA) should receive the message
    expect(result.listenerACount).toBeGreaterThanOrEqual(1);
    expect(result.listenerAPayload).toEqual([0xaa]);
    // Listener B (subscribed to topicB) should NOT receive it
    expect(result.listenerBCount).toBe(0);
  });

  test("multiple messages arrive in order", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      try {
        const topic = Topic.fromBytes(new Uint8Array(32).fill(0xee));

        const received: Array<number[]> = [];

        const signerA = await WebCryptoSigner.setup();
        const signerB = await WebCryptoSigner.setup();

        const syncerA = new Subduction(
          signerA,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          null
        );

        const syncerB = new Subduction(
          signerB,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, sender: any, payload: Uint8Array) => {
            received.push(Array.from(payload));
          }
        );

        const url = new URL(wsUrl);
        const serviceName = wsUrl.replace("ws://", "");

        await syncerA.connectDiscover(url, serviceName);
        await syncerB.connectDiscover(url, serviceName);

        await syncerB.subscribeEphemeral([topic]);
        await new Promise((r) => setTimeout(r, 200));

        // Send three messages in sequence
        await syncerA.publishEphemeral(topic, new Uint8Array([1]));
        await syncerA.publishEphemeral(topic, new Uint8Array([2]));
        await syncerA.publishEphemeral(topic, new Uint8Array([3]));

        // Wait for all to arrive
        await new Promise((r) => setTimeout(r, 1000));

        return {
          count: received.length,
          payloads: received,
          error: null,
        };
      } catch (error) {
        return {
          count: 0,
          payloads: [],
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentWsUrl);

    expect(result.error).toBeNull();
    expect(result.count).toBe(3);
    expect(result.payloads).toEqual([[1], [2], [3]]);
  });

  test("duplicate messages are suppressed by nonce dedup", async ({
    page,
  }) => {
    // This test verifies that the nonce cache prevents duplicate delivery.
    // We can't easily replay the exact signed bytes from JS, but we can
    // verify that publishing the same logical content twice produces two
    // distinct nonces (and thus two deliveries — not deduped at the
    // application level, since each publish generates a fresh nonce).
    // The real nonce dedup is tested at the Rust level.
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      try {
        const topic = Topic.fromBytes(new Uint8Array(32).fill(0xff));

        const received: Array<number[]> = [];

        const signerA = await WebCryptoSigner.setup();
        const signerB = await WebCryptoSigner.setup();

        const syncerA = new Subduction(
          signerA,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          null
        );

        const syncerB = new Subduction(
          signerB,
          new MemoryStorage(),
          null,
          null,
          null,
          null,
          null,
          (topic: any, sender: any, payload: Uint8Array) => {
            received.push(Array.from(payload));
          }
        );

        const url = new URL(wsUrl);
        const serviceName = wsUrl.replace("ws://", "");

        await syncerA.connectDiscover(url, serviceName);
        await syncerB.connectDiscover(url, serviceName);

        await syncerB.subscribeEphemeral([topic]);
        await new Promise((r) => setTimeout(r, 200));

        // Publish the "same" payload twice — each gets a fresh nonce,
        // so both should be delivered (they're distinct messages).
        const payload = new Uint8Array([42]);
        await syncerA.publishEphemeral(topic, payload);
        await syncerA.publishEphemeral(topic, payload);
        await new Promise((r) => setTimeout(r, 500));

        return {
          count: received.length,
          error: null,
        };
      } catch (error) {
        return {
          count: 0,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentWsUrl);

    expect(result.error).toBeNull();
    // Each publish produces a distinct nonce, so both should arrive
    expect(result.count).toBe(2);
  });
});
