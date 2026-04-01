import { test, expect, type BrowserContext, type Page } from "@playwright/test";
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
      },
    }
  );

  if (process.env.CI || process.env.DEBUG) {
    subductionServer.stdout?.on("data", (data: Buffer) => {
      console.log(`[server stdout]:`, data.toString().trim());
    });
    subductionServer.stderr?.on("data", (data: Buffer) => {
      console.error(`[server stderr]:`, data.toString().trim());
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

// ── Helpers ─────────────────────────────────────────────────────────────

/**
 * Create a new browser context with an isolated page, Wasm loaded and ready.
 * Each context has its own IndexedDB, so WebCryptoSigner.setup() produces
 * a unique key per context.
 */
async function createPeerPage(
  browser: any
): Promise<{ ctx: BrowserContext; page: Page }> {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, {
    timeout: wasmTimeout,
  });
  return { ctx, page };
}

/**
 * Connect a peer, optionally subscribe and set up a receive callback.
 */
async function setupPeer(
  page: Page,
  wsUrl: string,
  opts: {
    topicHex?: number;
    onReceive?: boolean;
  } = {}
): Promise<void> {
  await page.evaluate(
    async ({ wsUrl, topicHex, onReceive }) => {
      const { Subduction, MemoryStorage, WebCryptoSigner, Topic } =
        window.subduction;

      const signer = await WebCryptoSigner.setup();
      window.ephReceived = [];

      const callback = onReceive
        ? (_topic: any, _sender: any, payload: Uint8Array) => {
            window.ephReceived.push(Array.from(payload));
          }
        : null;

      window.syncer = new Subduction(
        signer,
        new MemoryStorage(),
        null,
        null,
        null,
        null,
        null,
        null,
        callback
      );

      const url = new URL(wsUrl);
      const serviceName = wsUrl.replace("ws://", "");
      await window.syncer.connectDiscover(url, serviceName);

      if (topicHex !== undefined) {
        const topicBytes = new Uint8Array(32).fill(topicHex);
        await window.syncer.subscribeEphemeral([
          Topic.fromBytes(topicBytes),
        ]);
      }

      return "ok";
    },
    { wsUrl, topicHex: opts.topicHex, onReceive: opts.onReceive ?? false }
  );
}

/** Publish an ephemeral message from a peer's page. */
async function publishEphemeral(
  page: Page,
  topicHex: number,
  payload: number[]
): Promise<void> {
  await page.evaluate(
    async ({ topicHex, payload }) => {
      const { Topic } = window.subduction;
      const topicBytes = new Uint8Array(32).fill(topicHex);
      await window.syncer.publishEphemeral(
        Topic.fromBytes(topicBytes),
        new Uint8Array(payload)
      );
    },
    { topicHex, payload }
  );
}

/** Get the received ephemeral messages from a peer's page. */
async function getReceived(page: Page): Promise<number[][]> {
  return await page.evaluate(() => window.ephReceived);
}

/** Unsubscribe from a topic. */
async function unsubscribeEphemeral(
  page: Page,
  topicHex: number
): Promise<void> {
  await page.evaluate(async (topicHex) => {
    const { Topic } = window.subduction;
    const topicBytes = new Uint8Array(32).fill(topicHex);
    await window.syncer.unsubscribeEphemeral([
      Topic.fromBytes(topicBytes),
    ]);
  }, topicHex);
}

// ── Topic API ───────────────────────────────────────────────────────────

test.describe("Topic API", () => {
  test("Topic.fromBytes creates a valid 32-byte topic", async ({
    browser,
  }) => {
    const { ctx, page } = await createPeerPage(browser);

    const result = await page.evaluate(() => {
      const { Topic } = window.subduction;

      const bytes = new Uint8Array(32).fill(0x42);
      const topic = Topic.fromBytes(bytes);
      const roundtripped = topic.toBytes();
      const str = topic.toString();

      return {
        bytesMatch: roundtripped.length === 32 && roundtripped[0] === 0x42,
        hasString: str.length > 0,
      };
    });

    expect(result.bytesMatch).toBe(true);
    expect(result.hasString).toBe(true);
    await ctx.close();
  });

  test("Topic.fromBytes rejects non-32-byte input", async ({ browser }) => {
    const { ctx, page } = await createPeerPage(browser);

    const threw = await page.evaluate(() => {
      const { Topic } = window.subduction;
      try {
        Topic.fromBytes(new Uint8Array(16));
        return false;
      } catch {
        return true;
      }
    });

    expect(threw).toBe(true);
    await ctx.close();
  });
});

// ── Ephemeral Messaging ─────────────────────────────────────────────────

test.describe("Ephemeral Messaging", () => {
  test("two peers exchange ephemeral messages through the server", async ({
    browser,
  }) => {
    const { ctx: ctxA, page: pageA } = await createPeerPage(browser);
    const { ctx: ctxB, page: pageB } = await createPeerPage(browser);

    // B subscribes and listens
    await setupPeer(pageB, currentWsUrl, {
      topicHex: 0xaa,
      onReceive: true,
    });

    // A connects and subscribes (so publish sends to server)
    await setupPeer(pageA, currentWsUrl, { topicHex: 0xaa });

    // Give subscriptions time to propagate
    await sleep(300);

    // A publishes
    await publishEphemeral(pageA, 0xaa, [1, 2, 3, 4, 5]);

    // Wait for delivery
    await pageB.waitForFunction(() => window.ephReceived.length > 0, {
      timeout: 5000,
    });
    const received = await getReceived(pageB);

    expect(received.length).toBeGreaterThanOrEqual(1);
    expect(received[0]).toEqual([1, 2, 3, 4, 5]);

    // A should NOT have received its own message
    const aReceived = await getReceived(pageA);
    expect(aReceived.length).toBe(0);

    await ctxA.close();
    await ctxB.close();
  });

  test("unsubscribe stops receiving ephemeral messages", async ({
    browser,
  }) => {
    const { ctx: ctxA, page: pageA } = await createPeerPage(browser);
    const { ctx: ctxB, page: pageB } = await createPeerPage(browser);

    await setupPeer(pageB, currentWsUrl, {
      topicHex: 0xbb,
      onReceive: true,
    });
    await setupPeer(pageA, currentWsUrl, { topicHex: 0xbb });
    await sleep(300);

    // Publish — should be received
    await publishEphemeral(pageA, 0xbb, [10]);
    await pageB.waitForFunction(() => window.ephReceived.length > 0, {
      timeout: 5000,
    });
    const countBefore = (await getReceived(pageB)).length;
    expect(countBefore).toBeGreaterThanOrEqual(1);

    // Unsubscribe
    await unsubscribeEphemeral(pageB, 0xbb);
    await sleep(300);

    // Publish again — should NOT be received
    await publishEphemeral(pageA, 0xbb, [20]);
    await sleep(1000);

    const countAfter = (await getReceived(pageB)).length;
    expect(countAfter).toBe(countBefore);

    await ctxA.close();
    await ctxB.close();
  });

  test("messages are scoped to their topic", async ({ browser }) => {
    const { ctx: ctxSender, page: pageSender } = await createPeerPage(
      browser
    );
    const { ctx: ctxListenerA, page: pageListenerA } = await createPeerPage(
      browser
    );
    const { ctx: ctxListenerB, page: pageListenerB } = await createPeerPage(
      browser
    );

    // Listener A subscribes to topic 0xcc
    await setupPeer(pageListenerA, currentWsUrl, {
      topicHex: 0xcc,
      onReceive: true,
    });
    await sleep(100);

    // Listener B subscribes to topic 0xdd (different)
    await setupPeer(pageListenerB, currentWsUrl, {
      topicHex: 0xdd,
      onReceive: true,
    });
    await sleep(100);

    // Sender connects (subscribes to 0xcc so publish sends to server)
    await setupPeer(pageSender, currentWsUrl, { topicHex: 0xcc });
    await sleep(300);

    // Publish to topic 0xcc only
    await publishEphemeral(pageSender, 0xcc, [0xaa]);

    // Listener A should receive
    await pageListenerA.waitForFunction(
      () => window.ephReceived.length > 0,
      { timeout: 5000 }
    );
    const listenerAReceived = await getReceived(pageListenerA);
    expect(listenerAReceived.length).toBeGreaterThanOrEqual(1);
    expect(listenerAReceived[0]).toEqual([0xaa]);

    // Listener B should NOT receive (different topic)
    await sleep(500);
    const listenerBReceived = await getReceived(pageListenerB);
    expect(listenerBReceived.length).toBe(0);

    await ctxSender.close();
    await ctxListenerA.close();
    await ctxListenerB.close();
  });

  test("multiple messages arrive in order", async ({ browser }) => {
    const { ctx: ctxA, page: pageA } = await createPeerPage(browser);
    const { ctx: ctxB, page: pageB } = await createPeerPage(browser);

    await setupPeer(pageB, currentWsUrl, {
      topicHex: 0xee,
      onReceive: true,
    });
    await sleep(100);
    await setupPeer(pageA, currentWsUrl, { topicHex: 0xee });
    await sleep(300);

    // Send three messages
    await publishEphemeral(pageA, 0xee, [1]);
    await publishEphemeral(pageA, 0xee, [2]);
    await publishEphemeral(pageA, 0xee, [3]);

    // Wait for all three
    await pageB.waitForFunction(() => window.ephReceived.length >= 3, {
      timeout: 5000,
    });
    const received = await getReceived(pageB);

    expect(received.length).toBe(3);
    expect(received).toEqual([[1], [2], [3]]);

    await ctxA.close();
    await ctxB.close();
  });

  test("each publish generates a distinct nonce (both delivered)", async ({
    browser,
  }) => {
    const { ctx: ctxA, page: pageA } = await createPeerPage(browser);
    const { ctx: ctxB, page: pageB } = await createPeerPage(browser);

    await setupPeer(pageB, currentWsUrl, {
      topicHex: 0xff,
      onReceive: true,
    });
    await setupPeer(pageA, currentWsUrl, { topicHex: 0xff });
    await sleep(300);

    // Same payload, two publishes — each gets a fresh nonce
    await publishEphemeral(pageA, 0xff, [42]);
    await publishEphemeral(pageA, 0xff, [42]);

    await pageB.waitForFunction(() => window.ephReceived.length >= 2, {
      timeout: 5000,
    });
    const received = await getReceived(pageB);

    expect(received.length).toBe(2);

    await ctxA.close();
    await ctxB.close();
  });

  test("transitive gossip: Alice publishes, Bob and Carol both receive", async ({
    browser,
  }) => {
    const { ctx: ctxAlice, page: pageAlice } = await createPeerPage(browser);
    const { ctx: ctxBob, page: pageBob } = await createPeerPage(browser);
    const { ctx: ctxCarol, page: pageCarol } = await createPeerPage(browser);

    // Bob and Carol subscribe and listen
    await setupPeer(pageBob, currentWsUrl, {
      topicHex: 0x77,
      onReceive: true,
    });
    await sleep(100);
    await setupPeer(pageCarol, currentWsUrl, {
      topicHex: 0x77,
      onReceive: true,
    });
    await sleep(100);

    // Alice connects (subscribes so publish routes to server)
    await setupPeer(pageAlice, currentWsUrl, { topicHex: 0x77 });
    await sleep(300);

    // Alice publishes
    await publishEphemeral(pageAlice, 0x77, [0xca, 0xfe]);

    // Both Bob and Carol should receive
    await pageBob.waitForFunction(() => window.ephReceived.length > 0, {
      timeout: 5000,
    });
    await pageCarol.waitForFunction(() => window.ephReceived.length > 0, {
      timeout: 5000,
    });

    const bobReceived = await getReceived(pageBob);
    const carolReceived = await getReceived(pageCarol);

    expect(bobReceived.length).toBeGreaterThanOrEqual(1);
    expect(carolReceived.length).toBeGreaterThanOrEqual(1);
    expect(bobReceived[0]).toEqual([0xca, 0xfe]);
    expect(carolReceived[0]).toEqual([0xca, 0xfe]);

    // Alice should NOT receive her own message
    const aliceReceived = await getReceived(pageAlice);
    expect(aliceReceived.length).toBe(0);

    await ctxAlice.close();
    await ctxBob.close();
    await ctxCarol.close();
  });
});
