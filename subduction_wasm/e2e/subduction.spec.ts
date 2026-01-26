import { test, expect } from "@playwright/test";
import { URL } from "./config";

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

test.describe("Subduction", () => {
  test.describe("Constructor and Initialization", () => {
    test("should create Subduction instance with MemoryStorage", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);
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
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();

        const syncer1 = new Subduction(signer, storage);
        const ids1 = await syncer1.sedimentreeIds();

        const syncer2 = await Subduction.hydrate(signer, storage);
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
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

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
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        const peerIds = await syncer.connectedPeerIds();

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
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        await syncer.disconnectAll();
        const peerIds = await syncer.connectedPeerIds();

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
        const { Subduction, MemoryStorage, SedimentreeId, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

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
        const { Subduction, MemoryStorage, SedimentreeId, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

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
        const { Subduction, MemoryStorage, Digest, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        const testDigest = new Uint8Array(32);
        testDigest[0] = 255;
        const digest = new Digest(testDigest);

        const blob = await syncer.getBlob(digest);

        return {
          blob,
          isUndefined: blob === undefined,
        };
      });

      expect(result.isUndefined).toBe(true);
    });

    test("should return empty map for non-existent blob digests", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, Digest, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        const testDigest = new Uint8Array(32);
        testDigest[0] = 1;
        const digest = new Digest(testDigest);

        const blobs = await syncer.getBlobs([digest]);

        return {
          blobs,
          isMap: blobs instanceof Map,
          size: blobs.size,
        };
      });

      expect(result.isMap).toBe(true);
      expect(result.size).toBe(0);
    });
  });

  test.describe("Multiple Instances", () => {
    test("should support multiple Subduction instances", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, WebCryptoSigner } = window.subduction;

        const signer1 = await WebCryptoSigner.setup();
        const signer2 = await WebCryptoSigner.setup();
        const storage1 = new MemoryStorage();
        const storage2 = new MemoryStorage();

        const syncer1 = new Subduction(signer1, storage1);
        const syncer2 = new Subduction(signer2, storage2);

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

    test("should convert PeerId to bytes matching input", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { PeerId } = window.subduction;

        const inputBytes = new Uint8Array(32);
        for (let i = 0; i < 32; i++) {
          inputBytes[i] = i;
        }

        const peerId = new PeerId(inputBytes);
        const outputBytes = peerId.toBytes();

        return {
          isUint8Array: outputBytes instanceof Uint8Array,
          length: outputBytes.length,
          matchesInput: inputBytes.every((b, i) => b === outputBytes[i]),
        };
      });

      expect(result.isUint8Array).toBe(true);
      expect(result.length).toBe(32);
      expect(result.matchesInput).toBe(true);
    });

    test("should convert PeerId to 64-char hex string", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { PeerId } = window.subduction;

        const bytes = new Uint8Array(32);
        for (let i = 0; i < 32; i++) {
          bytes[i] = i;
        }

        const peerId = new PeerId(bytes);
        const hexString = peerId.toString();

        const expectedHex = Array.from(bytes)
          .map((b) => b.toString(16).padStart(2, "0"))
          .join("");

        return {
          isString: typeof hexString === "string",
          length: hexString.length,
          isLowercase: hexString === hexString.toLowerCase(),
          matchesExpected: hexString === expectedHex,
        };
      });

      expect(result.isString).toBe(true);
      expect(result.length).toBe(64);
      expect(result.isLowercase).toBe(true);
      expect(result.matchesExpected).toBe(true);
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

  test.describe("Message Serialization", () => {
    test("should round-trip Message through CBOR", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Message, SedimentreeId, Digest } = window.subduction;

        const digestBytes = new Uint8Array(32);
        digestBytes[0] = 42;
        const digest = new Digest(digestBytes);
        const original = Message.blobsRequest([digest]);

        const cborBytes = original.toCborBytes();

        const restored = Message.fromCborBytes(cborBytes);

        return {
          hasCborBytes: cborBytes instanceof Uint8Array,
          cborBytesLength: cborBytes.length,
          hasRestored: !!restored,
          typeMatches: original.type === restored.type,
          originalType: original.type,
          restoredType: restored.type,
        };
      });

      expect(result.hasCborBytes).toBe(true);
      expect(result.cborBytesLength).toBeGreaterThan(0);
      expect(result.hasRestored).toBe(true);
      expect(result.typeMatches).toBe(true);
    });

    test("should throw MessageDeserializationError for invalid CBOR bytes", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Message } = window.subduction;

        try {
          const invalidBytes = new Uint8Array([0xff, 0xfe, 0x00, 0x01]);
          Message.fromCborBytes(invalidBytes);
          return { threw: false, errorName: null };
        } catch (error) {
          return {
            threw: true,
            errorName: error.name,
            hasMessage: !!error.message,
          };
        }
      });

      expect(result.threw).toBe(true);
      expect(result.errorName).toBe("MessageDeserializationError");
    });
  });

  test.describe("Error Handling", () => {
    test("should handle invalid digest size", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Digest } = window.subduction;

        try {
          const invalidBytes = new Uint8Array(17);
          new Digest(invalidBytes);
          return { error: null };
        } catch (error) {
          return {
            error: error.message ,
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
          const invalidBytes = new Uint8Array(17);
          new PeerId(invalidBytes);
          return { error: null };
        } catch (error) {
          return {
            error: error.message ,
            hasError: true,
          };
        }
      });

      expect(result.hasError).toBe(true);
    });

    test("should handle operations on disconnected peers", async ({ page }) => {
      const result = await page.evaluate(async () => {
        const { Subduction, MemoryStorage, PeerId, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        const peerBytes = new Uint8Array(32);
        peerBytes[0] = 1;
        const peerId = new PeerId(peerBytes);

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
        const { Subduction, MemoryStorage, Digest, WebCryptoSigner } = window.subduction;
        const signer = await WebCryptoSigner.setup();
        const storage = new MemoryStorage();
        const syncer = new Subduction(signer, storage);

        const digest1 = new Digest(new Uint8Array(32));
        const digest2 = new Digest(new Uint8Array(32).fill(1));

        try {
          await syncer.requestBlobs([digest1, digest2]);
          return { error: null };
        } catch (error) {
          return { error: error.message };
        }
      });

      expect(result.error).toBeNull();
    });
  });
});
