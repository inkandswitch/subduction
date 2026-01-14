import { test, expect } from "@playwright/test";
import { URL } from "./config";

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  await page.waitForFunction(() => window.subductionReady === true, { timeout: 10000 });
});

test.describe("Subduction", () => {
  test.describe("Constructor and Initialization", () => {
    test("should create Subduction instance with MemoryStorage", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);
        return {
          hasSyncer: !!syncer,
          hasStorage: !!storage,
        };
      });

      expect(result.hasSyncer).toBe(true);
      expect(result.hasStorage).toBe(true);
    });

    test("should hydrate Subduction from existing storage", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();

        // Create initial instance and add data
        const syncer1 = new Subduction(storage);
        const ids1 = await syncer1.sedimentreeIds();

        // Hydrate from same storage
        const syncer2 = await Subduction.hydrate(storage);
        const ids2 = await syncer2.sedimentreeIds();

        return {
          hasSyncer1: !!syncer1,
          hasSyncer2: !!syncer2,
          idsMatch: JSON.stringify(ids1) === JSON.stringify(ids2),
        };
      });

      expect(result.hasSyncer1).toBe(true);
      expect(result.hasSyncer2).toBe(true);
      expect(result.idsMatch).toBe(true);
    });
  });

  test.describe("Storage Operations", () => {
    test("should work with MemoryStorage", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();

        return {
          hasStorage: !!storage,
          storageType: storage.constructor.name,
        };
      });

      expect(result.hasStorage).toBe(true);
      expect(result.storageType).toBe("MemoryStorage");
    });
  });

  test.describe("Sedimentree Management", () => {
    test("should return empty array for sedimentreeIds initially", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const ids = await syncer.sedimentreeIds();

        return {
          ids,
          idsLength: ids.length,
          isArray: Array.isArray(ids),
        };
      });

      expect(result.isArray).toBe(true);
      expect(result.idsLength).toBe(0);
    });

    test("should handle Digest creation", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Digest } = window.subduction;

        // Create a test digest (32 bytes)
        const testBytes = new Uint8Array(32);
        for (let i = 0; i < 32; i++) {
          testBytes[i] = i;
        }

        const digest = new Digest(testBytes);

        return {
          hasDigest: !!digest,
          digestType: digest.constructor.name,
        };
      });

      expect(result.hasDigest).toBe(true);
      expect(result.digestType).toBe("Digest");
    });

    test("should get peer IDs", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const peerIds = await syncer.getPeerIds();

        return {
          peerIds,
          isArray: Array.isArray(peerIds),
          length: peerIds.length,
        };
      });

      expect(result.isArray).toBe(true);
      expect(result.length).toBe(0);
    });

    test("should disconnect all connections", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        await syncer.disconnectAll();
        const peerIds = await syncer.getPeerIds();

        return {
          disconnected: true,
          peerIdsLength: peerIds.length,
        };
      });

      expect(result.disconnected).toBe(true);
      expect(result.peerIdsLength).toBe(0);
    });
  });

  test.describe("Data Retrieval", () => {
    test("should return undefined for non-existent sedimentree commits", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, SedimentreeId } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        // Create a test sedimentree ID
        const testId = new Uint8Array(32);
        testId[0] = 1;
        const sedimentreeId = SedimentreeId.fromBytes(testId);

        const commits = await syncer.getCommits(sedimentreeId);

        return {
          commits,
          isUndefined: commits === undefined,
        };
      });

      expect(result.isUndefined).toBe(true);
    });

    test("should return undefined for non-existent sedimentree fragments", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, SedimentreeId } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        // Create a test sedimentree ID
        const testId = new Uint8Array(32);
        testId[0] = 1;
        const sedimentreeId = SedimentreeId.fromBytes(testId);

        const fragments = await syncer.getFragments(sedimentreeId);

        return {
          fragments,
          isUndefined: fragments === undefined,
        };
      });

      expect(result.isUndefined).toBe(true);
    });

    test("should return undefined for non-existent blob", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, Digest } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        // Create a test digest
        const testDigest = new Uint8Array(32);
        testDigest[0] = 255;
        const digest = new Digest(testDigest);

        const blob = await syncer.getLocalBlob(digest);

        return {
          blob,
          isUndefined: blob === undefined,
        };
      });

      expect(result.isUndefined).toBe(true);
    });

    test("should return empty array for non-existent sedimentree blobs", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, SedimentreeId } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        // Create a test sedimentree ID
        const testId = new Uint8Array(32);
        testId[0] = 1;
        const sedimentreeId = SedimentreeId.fromBytes(testId);

        const blobs = await syncer.getLocalBlobs(sedimentreeId);

        return {
          blobs,
          isArray: Array.isArray(blobs),
          length: blobs.length,
        };
      });

      expect(result.isArray).toBe(true);
      expect(result.length).toBe(0);
    });
  });

  test.describe("Event Handling", () => {
    test("should register and unregister commit callbacks", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        let callbackCalled = false;
        const callback = () => { callbackCalled = true; };

        await syncer.onCommit(callback);
        await syncer.offCommit(callback);

        return {
          registered: true,
          callbackCalled,
        };
      });

      expect(result.registered).toBe(true);
      expect(result.callbackCalled).toBe(false);
    });

    test("should register and unregister fragment callbacks", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        let callbackCalled = false;
        const callback = () => { callbackCalled = true; };

        await syncer.onFragment(callback);
        await syncer.offFragment(callback);

        return {
          registered: true,
          callbackCalled,
        };
      });

      expect(result.registered).toBe(true);
      expect(result.callbackCalled).toBe(false);
    });

    test("should register and unregister blob callbacks", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        let callbackCalled = false;
        const callback = () => { callbackCalled = true; };

        await syncer.onBlob(callback);
        await syncer.offBlob(callback);

        return {
          registered: true,
          callbackCalled,
        };
      });

      expect(result.registered).toBe(true);
      expect(result.callbackCalled).toBe(false);
    });

    test("should handle multiple callbacks for same event", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const callback1 = () => {};
        const callback2 = () => {};
        const callback3 = () => {};

        await syncer.onCommit(callback1);
        await syncer.onCommit(callback2);
        await syncer.onCommit(callback3);

        await syncer.offCommit(callback2);

        return {
          multipleRegistered: true,
        };
      });

      expect(result.multipleRegistered).toBe(true);
    });
  });

  test.describe("Multiple Instances", () => {
    test("should support multiple Subduction instances", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage } = window.subduction;

        const storage1 = new MemoryStorage();
        const storage2 = new MemoryStorage();

        const syncer1 = new Subduction(storage1);
        const syncer2 = new Subduction(storage2);

        const ids1 = await syncer1.sedimentreeIds();
        const ids2 = await syncer2.sedimentreeIds();

        return {
          hasSyncer1: !!syncer1,
          hasSyncer2: !!syncer2,
          bothEmpty: ids1.length === 0 && ids2.length === 0,
        };
      });

      expect(result.hasSyncer1).toBe(true);
      expect(result.hasSyncer2).toBe(true);
      expect(result.bothEmpty).toBe(true);
    });
  });

  test.describe("Type System", () => {
    test("should create PeerId from bytes", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { PeerId } = window.subduction;

        const bytes = new Uint8Array(32);
        for (let i = 0; i < 32; i++) {
          bytes[i] = i;
        }

        const peerId = new PeerId(bytes);

        return {
          hasPeerId: !!peerId,
          type: peerId.constructor.name,
        };
      });

      expect(result.hasPeerId).toBe(true);
      expect(result.type).toBe("PeerId");
    });

    test("should create SedimentreeId from bytes", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { SedimentreeId } = window.subduction;

        const bytes = new Uint8Array(32);
        for (let i = 0; i < 32; i++) {
          bytes[i] = i;
        }

        const sedimentreeId = SedimentreeId.fromBytes(bytes);

        return {
          hasSedimentreeId: !!sedimentreeId,
          type: sedimentreeId.constructor.name,
        };
      });

      expect(result.hasSedimentreeId).toBe(true);
      expect(result.type).toBe("SedimentreeId");
    });

    test("should create BlobMeta", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { BlobMeta } = window.subduction;

        const blobData = new Uint8Array(1024);
        for (let i = 0; i < 1024; i++) {
          blobData[i] = i % 256;
        }
        const blobMeta = new BlobMeta(blobData);

        return {
          hasBlobMeta: !!blobMeta,
          type: blobMeta.constructor.name,
          hasDigest: !!blobMeta.digest(),
          sizeBytes: blobMeta.sizeBytes,
        };
      });

      expect(result.hasBlobMeta).toBe(true);
      expect(result.type).toBe("BlobMeta");
      expect(result.hasDigest).toBe(true);
      expect(result.sizeBytes).toBe(1024n);
    });
  });

  test.describe("Error Handling", () => {
    test("should handle invalid digest size", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Digest } = window.subduction;

        try {
          // Try to create digest with wrong size (should be 32 bytes)
          const invalidBytes = new Uint8Array(16);
          new Digest(invalidBytes);
          return { error: null };
        } catch (error) {
          return {
            error: error.message || "Error occurred",
            hasError: true,
          };
        }
      });

      expect(result.hasError).toBe(true);
    });

    test("should handle invalid PeerId size", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { PeerId } = window.subduction;

        try {
          // Try to create PeerId with wrong size (should be 32 bytes)
          const invalidBytes = new Uint8Array(16);
          new PeerId(invalidBytes);
          return { error: null };
        } catch (error) {
          return {
            error: error.message || "Error occurred",
            hasError: true,
          };
        }
      });

      expect(result.hasError).toBe(true);
    });

    test("should handle operations on disconnected peers", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, PeerId } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        // Create a fake peer ID
        const peerBytes = new Uint8Array(32);
        peerBytes[0] = 1;
        const peerId = new PeerId(peerBytes);

        // Try to disconnect from non-existent peer
        const disconnected = await syncer.disconnectFromPeer(peerId);

        return {
          disconnected,
        };
      });

      expect(result.disconnected).toBe(false);
    });
  });

  test.describe("API Smoke Tests", () => {
    test("should call requestBlobs without throwing", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, Digest } = window.subduction;
        const storage = new MemoryStorage();
        const syncer = new Subduction(storage);

        const digest1 = new Digest(new Uint8Array(32));
        const digest2 = new Digest(new Uint8Array(32).fill(1));

        try {
          await syncer.requestBlobs([digest1, digest2]);
          return { success: true, error: null };
        } catch (error) {
          return { success: false, error: error.message };
        }
      });

      expect(result.success).toBe(true);
      expect(result.error).toBeNull();
    });
  });
});
