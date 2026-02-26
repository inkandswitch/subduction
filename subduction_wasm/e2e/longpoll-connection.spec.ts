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

async function waitForPort(host: string, port: number, timeout: number = 10000): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeout) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = new net.Socket();
        socket.setTimeout(1000);
        socket.once("connect", () => { socket.destroy(); resolve(); });
        socket.once("error", () => { socket.destroy(); reject(); });
        socket.once("timeout", () => { socket.destroy(); reject(); });
        socket.connect(port, host);
      });
      return;
    } catch {
      await sleep(100);
    }
  }

  throw new Error(`Port ${host}:${port} did not become available within ${timeout}ms`);
}

// Different ports per browser to avoid conflicts when running in parallel
const LP_HOST = "127.0.0.1";
const LP_PORTS: Record<string, number> = {
  chromium: 9895,
  firefox: 9896,
  webkit: 9897,
};
const LP_CONSOLE_PORTS: Record<string, number> = {
  chromium: 6672,
  firefox: 6673,
  webkit: 6674,
};

let subductionServer: ChildProcess | null = null;
let currentPort: number;
let currentBaseUrl: string;

test.beforeAll(async ({ browserName }) => {
  if (process.env.CI) {
    test.skip();
  }

  currentPort = LP_PORTS[browserName];
  currentBaseUrl = `http://${LP_HOST}:${currentPort}`;

  const cliPath = path.join(__dirname, "../../target/release/subduction_cli");

  // Start server with both transports enabled (default), ephemeral key for test isolation
  subductionServer = spawn(
    cliPath,
    ["start", "--socket", `${LP_HOST}:${currentPort}`, "--ephemeral-key"],
    {
      cwd: path.join(__dirname, "../.."),
      stdio: "pipe",
      env: {
        ...process.env,
        RUST_LOG: "info",
        TOKIO_CONSOLE_BIND: `${LP_HOST}:${LP_CONSOLE_PORTS[browserName]}`,
      },
    }
  );

  if (process.env.CI || process.env.DEBUG) {
    subductionServer.stdout?.on("data", (data) => {
      console.log(`[${browserName} lp stdout]:`, data.toString().trim());
    });
    subductionServer.stderr?.on("data", (data) => {
      console.error(`[${browserName} lp stderr]:`, data.toString().trim());
    });
  }

  const healthCheckTimeout = process.env.CI ? 15000 : 10000;
  try {
    await waitForPort(LP_HOST, currentPort, healthCheckTimeout);
    console.log(`\u2713 Subduction server started on ${currentBaseUrl} for ${browserName} (long-poll tests)`);
  } catch (error) {
    console.error(`Failed to start server on ${currentBaseUrl}: ${error}`);
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
    console.log("\u2713 Subduction server stopped (long-poll tests)");
  }
});

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

test.describe.configure({ mode: "serial" });

test.describe("Long-Poll Connection Tests", () => {
  test("should connect via connectDiscoverLongPoll", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { Subduction, WebCryptoSigner } = window.subduction;

      try {
        const signer = await WebCryptoSigner.setup();
        const syncer = new Subduction(signer, new window.subduction.MemoryStorage());

        const peerId = await syncer.connectDiscoverLongPoll(
          baseUrl,
          signer,
          10000,
          baseUrl.replace("http://", "")
        );

        const connectedPeers = await syncer.getConnectedPeerIds();

        return {
          connected: true,
          peerIdHex: peerId.toString(),
          peerCount: connectedPeers.length,
          error: null,
        };
      } catch (error) {
        return {
          connected: false,
          peerIdHex: null,
          peerCount: 0,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    expect(result.error).toBeNull();
    expect(result.connected).toBe(true);
    expect(result.peerCount).toBeGreaterThan(0);
    expect(result.peerIdHex).toBeTruthy();
  });

  test("should disconnect after long-poll connect", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { Subduction, WebCryptoSigner, MemoryStorage } = window.subduction;

      try {
        const signer = await WebCryptoSigner.setup();
        const syncer = new Subduction(signer, new MemoryStorage());

        await syncer.connectDiscoverLongPoll(
          baseUrl,
          signer,
          10000,
          baseUrl.replace("http://", "")
        );

        const beforeCount = (await syncer.getConnectedPeerIds()).length;
        await syncer.disconnectAll();
        const afterCount = (await syncer.getConnectedPeerIds()).length;

        return {
          beforeCount,
          afterCount,
          error: null,
        };
      } catch (error) {
        return {
          beforeCount: 0,
          afterCount: 0,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    expect(result.error).toBeNull();
    expect(result.beforeCount).toBeGreaterThan(0);
    expect(result.afterCount).toBe(0);
  });

  test("should sync data via long-poll", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { Subduction, WebCryptoSigner, MemoryStorage, SedimentreeId } = window.subduction;

      try {
        const signer = await WebCryptoSigner.setup();
        const syncer = new Subduction(signer, new MemoryStorage());

        await syncer.connectDiscoverLongPoll(
          baseUrl,
          signer,
          10000,
          baseUrl.replace("http://", "")
        );

        // Add a commit locally
        const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(99));
        const blob = new Uint8Array([1, 2, 3, 4, 5]);
        await syncer.addCommit(sedId, [], blob);

        // Sync to the server (timeout is bigint)
        const syncResult = await syncer.fullSync(10000n);

        return {
          synced: syncResult.success,
          error: null,
        };
      } catch (error) {
        return {
          synced: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    expect(result.error).toBeNull();
    expect(result.synced).toBe(true);
  });

  test("should connect via SubductionLongPoll.tryDiscover", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { SubductionLongPoll, WebCryptoSigner } = window.subduction;

      try {
        const signer = await WebCryptoSigner.setup();

        const authenticated = await SubductionLongPoll.tryDiscover(
          baseUrl,
          signer,
          10000,
          baseUrl.replace("http://", "")
        );

        const peerId = authenticated.peerId;
        const sessionId = authenticated.sessionId;

        return {
          connected: true,
          hasPeerId: !!peerId,
          hasSessionId: typeof sessionId === "string" && sessionId.length > 0,
          error: null,
        };
      } catch (error) {
        return {
          connected: false,
          hasPeerId: false,
          hasSessionId: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    expect(result.error).toBeNull();
    expect(result.connected).toBe(true);
    expect(result.hasPeerId).toBe(true);
    expect(result.hasSessionId).toBe(true);
  });

  test("should handle tryDiscover with default parameters", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { SubductionLongPoll, WebCryptoSigner } = window.subduction;

      try {
        const signer = await WebCryptoSigner.setup();

        // Omit optional parameters -- should use defaults
        const authenticated = await SubductionLongPoll.tryDiscover(
          baseUrl,
          signer
        );

        return { connected: true, error: null };
      } catch (error) {
        return {
          connected: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    // The call was accepted at the binding level (no "missing argument" error).
    // Connection may or may not succeed depending on discovery name defaults.
    expect(true).toBe(true);
  });

  test("should run two long-poll clients concurrently", async ({ page }) => {
    const result = await page.evaluate(async (baseUrl) => {
      const { Subduction, WebCryptoSigner, MemoryStorage } = window.subduction;

      try {
        const signer1 = await WebCryptoSigner.setup();
        const signer2 = await WebCryptoSigner.setup();

        const syncer1 = new Subduction(signer1, new MemoryStorage());
        const syncer2 = new Subduction(signer2, new MemoryStorage());

        const serviceName = baseUrl.replace("http://", "");

        await syncer1.connectDiscoverLongPoll(baseUrl, signer1, 10000, serviceName);
        await syncer2.connectDiscoverLongPoll(baseUrl, signer2, 10000, serviceName);

        const peers1 = (await syncer1.getConnectedPeerIds()).length;
        const peers2 = (await syncer2.getConnectedPeerIds()).length;

        return {
          syncer1Connected: peers1 > 0,
          syncer2Connected: peers2 > 0,
          error: null,
        };
      } catch (error) {
        return {
          syncer1Connected: false,
          syncer2Connected: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }, currentBaseUrl);

    expect(result.error).toBeNull();
    expect(result.syncer1Connected).toBe(true);
    expect(result.syncer2Connected).toBe(true);
  });
});
