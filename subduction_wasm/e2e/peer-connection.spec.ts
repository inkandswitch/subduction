import { test, expect } from "@playwright/test";
import { URL } from "./config";
import { spawn, ChildProcess } from "child_process";
import { promisify } from "util";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const sleep = promisify(setTimeout);

// WebSocket server configuration
const WS_HOST = "127.0.0.1";
const WS_PORT = 9892; // Different from http-server port
const WS_URL = `ws://${WS_HOST}:${WS_PORT}`;

let subductionServer: ChildProcess | null = null;

test.beforeAll(async () => {
  const cliPath = path.join(__dirname, "../../target/release/subduction_cli");

  subductionServer = spawn(cliPath, ["start", "--socket", `${WS_HOST}:${WS_PORT}`], {
    cwd: path.join(__dirname, "../.."),
    stdio: "pipe",
    env: {
      ...process.env,
      RUST_LOG: "info",
      TOKIO_CONSOLE: "0",
    },
  });

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Server startup timeout"));
    }, 5000);

    let resolved = false;

    const checkOutput = (data: Buffer) => {
      const output = data.toString();
      console.log("[subduction_cli]", output);
      if ((output.includes("Starting WebSocket server") || output.includes("WebSocket server started")) && !resolved) {
        resolved = true;
        clearTimeout(timeout);
        resolve();
      }
    };

    subductionServer!.stdout?.on("data", checkOutput);
    subductionServer!.stderr?.on("data", checkOutput);

    subductionServer!.on("error", (err) => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        reject(err);
      }
    });
  });

  console.log(`✓ Subduction WebSocket server started on ${WS_URL}`);
});

test.afterAll(async () => {
  // Stop subduction_cli server
  if (subductionServer) {
    subductionServer.kill("SIGTERM");
    await sleep(500);
    if (!subductionServer.killed) {
      subductionServer.kill("SIGKILL");
    }
    console.log("✓ Subduction WebSocket server stopped");
  }
});

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  await page.waitForFunction(() => window.subductionReady === true, { timeout: 10000 });
});

test.describe.configure({ mode: 'serial' });

test.describe("Peer Connection Tests", { tag: "@peer" }, () => {
  test("should connect to WebSocket server", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { SubductionWebSocket, PeerId } = window.subduction;

      try {
        const ws = new WebSocket(wsUrl);

        await new Promise((resolve, reject) => {
          ws.onopen = resolve;
          ws.onerror = reject;
          setTimeout(() => reject(new Error("Connection timeout")), 5000);
        });

        const peerIdBytes = new Uint8Array(32);
        peerIdBytes[0] = 1; // Unique peer ID
        const peerId = new PeerId(peerIdBytes);

        const subductionWs = await SubductionWebSocket.setup(
          peerId,
          ws,
          5000
        );

        return {
          connected: true,
          hasWebSocket: !!subductionWs,
          error: null,
        };
      } catch (error) {
        return {
          connected: false,
          hasWebSocket: false,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.connected).toBe(true);
    expect(result.hasWebSocket).toBe(true);
    expect(result.error).toBeNull();
  });

  test("should register connection with Subduction instance", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, SubductionWebSocket, PeerId } = window.subduction;

      try {
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const ws = new WebSocket(wsUrl);
        await new Promise((resolve, reject) => {
          ws.onopen = resolve;
          ws.onerror = reject;
          setTimeout(() => reject(new Error("Connection timeout")), 5000);
        });

        const peerIdBytes = new Uint8Array(32);
        peerIdBytes[0] = 2;
        const peerId = new PeerId(peerIdBytes);

        const subductionWs = await SubductionWebSocket.setup(peerId, ws, 5000);

        const registered = await syncer.register(subductionWs);

        const peerIds = await syncer.getPeerIds();

        return {
          registered: !!registered,
          isNew: registered.is_new,
          peerCount: peerIds.length,
          error: null,
        };
      } catch (error) {
        return {
          registered: false,
          isNew: false,
          peerCount: 0,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.registered).toBe(true);
    expect(result.peerCount).toBeGreaterThan(0);
    expect(result.error).toBeNull();
  });

  test("should handle connection with SubductionWebSocket.connect", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, SubductionWebSocket, PeerId } = window.subduction;

      try {
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const peerIdBytes = new Uint8Array(32);
        peerIdBytes[0] = 3;
        const peerId = new PeerId(peerIdBytes);

        const url = new URL(wsUrl);
        const subductionWs = await SubductionWebSocket.connect(url, peerId, 5000);

        const registered = await syncer.register(subductionWs);
        const peerIds = await syncer.getPeerIds();

        return {
          connected: true,
          registered: !!registered,
          peerCount: peerIds.length,
          error: null,
        };
      } catch (error) {
        return {
          connected: false,
          registered: false,
          peerCount: 0,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.connected).toBe(true);
    expect(result.registered).toBe(true);
    expect(result.peerCount).toBeGreaterThan(0);
    expect(result.error).toBeNull();
  });

  test("should disconnect from peer", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, SubductionWebSocket, PeerId } = window.subduction;

      try {
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const peerIdBytes = new Uint8Array(32);
        peerIdBytes[0] = 4;
        const peerId = new PeerId(peerIdBytes);

        const url = new URL(wsUrl);
        const subductionWs = await SubductionWebSocket.connect(url, peerId, 5000);
        await syncer.register(subductionWs);

        const peerIdsBeforeDisconnect = await syncer.getPeerIds();
        await syncer.disconnectAll();
        const peerIdsAfterDisconnect = await syncer.getPeerIds();

        return {
          beforeCount: peerIdsBeforeDisconnect.length,
          afterCount: peerIdsAfterDisconnect.length,
          disconnected: peerIdsAfterDisconnect.length === 0,
          error: null,
        };
      } catch (error) {
        return {
          beforeCount: 0,
          afterCount: 0,
          disconnected: false,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.beforeCount).toBeGreaterThan(0);
    expect(result.afterCount).toBe(0);
    expect(result.disconnected).toBe(true);
    expect(result.error).toBeNull();
  });

  test("should request blobs from connected peer", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, SubductionWebSocket, PeerId, Digest } = window.subduction;

      try {
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const peerIdBytes = new Uint8Array(32);
        peerIdBytes[0] = 5;
        const peerId = new PeerId(peerIdBytes);

        const url = new URL(wsUrl);
        const subductionWs = await SubductionWebSocket.connect(url, peerId, 5000);
        await syncer.register(subductionWs);

        const digest1 = new Digest(new Uint8Array(32).fill(1));
        const digest2 = new Digest(new Uint8Array(32).fill(2));

        await syncer.requestBlobs([digest1, digest2]);

        return {
          requested: true,
          error: null,
        };
      } catch (error) {
        return {
          requested: false,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.requested).toBe(true);
    expect(result.error).toBeNull();
  });

  test("should handle multiple concurrent connections", async ({ page }) => {
    const result = await page.evaluate(async (wsUrl) => {
      const { Subduction, MemoryStorage, SubductionWebSocket, PeerId } = window.subduction;

      try {
        const storage1 = new MemoryStorage();
        const storage2 = new MemoryStorage();

        const syncer1 = new Subduction(storage1);
        const syncer2 = new Subduction(storage2);

        // Connect first syncer
        const peerId1 = new PeerId(new Uint8Array(32).fill(6));
        const url = new URL(wsUrl);
        const ws1 = await SubductionWebSocket.connect(url, peerId1, 5000);
        await syncer1.register(ws1);

        // Connect second syncer
        const peerId2 = new PeerId(new Uint8Array(32).fill(7));
        const ws2 = await SubductionWebSocket.connect(url, peerId2, 5000);
        await syncer2.register(ws2);

        const peers1 = await syncer1.getPeerIds();
        const peers2 = await syncer2.getPeerIds();

        return {
          syncer1Connected: peers1.length > 0,
          syncer2Connected: peers2.length > 0,
          bothConnected: peers1.length > 0 && peers2.length > 0,
          error: null,
        };
      } catch (error) {
        return {
          syncer1Connected: false,
          syncer2Connected: false,
          bothConnected: false,
          error: error.message || String(error),
        };
      }
    }, WS_URL);

    expect(result.syncer1Connected).toBe(true);
    expect(result.syncer2Connected).toBe(true);
    expect(result.bothConnected).toBe(true);
    expect(result.error).toBeNull();
  });
});
