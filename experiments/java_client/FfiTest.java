import java.lang.foreign.*;
import java.lang.invoke.*;
import java.nio.file.Path;
import java.util.Arrays;

import static java.lang.foreign.ValueLayout.*;

/**
 * Panama FFM test harness for all three FFI approaches.
 *
 * Requires JDK 22+ with --enable-native-access=ALL-UNNAMED.
 *
 * Usage:
 *   javac FfiTest.java
 *   java --enable-native-access=ALL-UNNAMED \
 *        -Djava.library.path=../../target/debug \
 *        FfiTest
 */
public class FfiTest {

    // ==================== Shared FFI type layouts ====================

    static final StructLayout FFI_BYTES = MemoryLayout.structLayout(
        ADDRESS.withName("ptr"),
        JAVA_LONG.withName("len")   // size_t on 64-bit
    );

    static final StructLayout FFI_RESULT = MemoryLayout.structLayout(
        JAVA_INT.withName("status"),
        MemoryLayout.paddingLayout(4),  // alignment padding
        FFI_BYTES.withName("data"),
        ADDRESS.withName("error_msg")
    );

    static final StructLayout FFI_EFFECT = MemoryLayout.structLayout(
        JAVA_INT.withName("tag"),
        MemoryLayout.paddingLayout(4),
        FFI_BYTES.withName("data")
    );

    // VarHandles for field access
    static final VarHandle RESULT_STATUS = FFI_RESULT.varHandle(
        MemoryLayout.PathElement.groupElement("status"));
    static final VarHandle RESULT_ERROR_MSG = FFI_RESULT.varHandle(
        MemoryLayout.PathElement.groupElement("error_msg"));

    static final VarHandle BYTES_PTR = FFI_BYTES.varHandle(
        MemoryLayout.PathElement.groupElement("ptr"));
    static final VarHandle BYTES_LEN = FFI_BYTES.varHandle(
        MemoryLayout.PathElement.groupElement("len"));

    static final VarHandle EFFECT_TAG = FFI_EFFECT.varHandle(
        MemoryLayout.PathElement.groupElement("tag"));

    // ==================== Library loading ====================

    static final Linker LINKER = Linker.nativeLinker();
    static Arena LIB_ARENA = Arena.ofAuto();

    static final String LIB_DIR = System.getProperty("lib.dir", "../../target/debug");

    static SymbolLookup loadLib(String name) {
        Path libPath = Path.of(LIB_DIR, System.mapLibraryName(name));
        return SymbolLookup.libraryLookup(libPath, LIB_ARENA);
    }

    static MethodHandle downcall(SymbolLookup lib, String name, FunctionDescriptor desc) {
        return LINKER.downcallHandle(
            lib.find(name).orElseThrow(() ->
                new RuntimeException("Symbol not found: " + name)),
            desc);
    }

    // ==================== Helper: extract FfiResult ====================

    static byte[] extractResult(MemorySegment resultSeg) throws Throwable {
        int status = (int) RESULT_STATUS.get(resultSeg, 0L);
        if (status != 0) {
            MemorySegment errorPtr = (MemorySegment) RESULT_ERROR_MSG.get(resultSeg, 0L);
            String msg = "FFI error (status " + status + ")";
            if (!errorPtr.equals(MemorySegment.NULL)) {
                msg = errorPtr.reinterpret(256).getString(0);
                // Free error msg via ffi_common (symbols are in the loaded libs)
            }
            throw new RuntimeException(msg);
        }

        // Extract data bytes
        MemorySegment dataSeg = resultSeg.asSlice(
            FFI_RESULT.byteOffset(MemoryLayout.PathElement.groupElement("data")),
            FFI_BYTES.byteSize());
        MemorySegment dataPtr = (MemorySegment) BYTES_PTR.get(dataSeg, 0L);
        long dataLen = (long) BYTES_LEN.get(dataSeg, 0L);

        if (dataPtr.equals(MemorySegment.NULL) || dataLen == 0) {
            return new byte[0];
        }

        byte[] result = dataPtr.reinterpret(dataLen).toArray(JAVA_BYTE);
        // Note: should free via ffi_bytes_free, but for test purposes we skip
        return result;
    }

    // ==================== Tokio approach ====================

    static void testTokio() throws Throwable {
        System.out.println("=== Tokio Approach ===");
        var lib = loadLib("ffi_tokio");

        var storageNew = downcall(lib, "storage_new",
            FunctionDescriptor.of(ADDRESS));
        var storageFree = downcall(lib, "storage_free",
            FunctionDescriptor.ofVoid(ADDRESS));
        var saveSedimentreeId = downcall(lib, "storage_save_sedimentree_id",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS, ADDRESS, JAVA_LONG));
        var loadAllIds = downcall(lib, "storage_load_all_sedimentree_ids",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS));
        var saveBlob = downcall(lib, "storage_save_blob",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS, ADDRESS, JAVA_LONG));
        var loadBlob = downcall(lib, "storage_load_blob",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS, ADDRESS, JAVA_LONG));

        MemorySegment handle = (MemorySegment) storageNew.invokeExact();
        System.out.println("  Created storage handle: " + handle);

        try (Arena arena = Arena.ofConfined()) {
            // Build a CBOR-encoded SedimentreeId (32-byte bytestring)
            byte[] testId = new byte[34];
            testId[0] = 0x58;
            testId[1] = 0x20;
            for (int i = 0; i < 32; i++) testId[i + 2] = (byte) i;

            MemorySegment idBuf = arena.allocateFrom(JAVA_BYTE, testId);

            // Save
            MemorySegment saveResult = (MemorySegment) saveSedimentreeId.invokeExact(
                (SegmentAllocator) arena, handle, idBuf, (long) testId.length);
            extractResult(saveResult);
            System.out.println("  Saved sedimentree ID");

            // Load all
            MemorySegment loadResult = (MemorySegment) loadAllIds.invokeExact(
                (SegmentAllocator) arena, handle);
            byte[] ids = extractResult(loadResult);
            System.out.println("  Loaded all IDs: " + ids.length + " bytes");

            // Save blob
            byte[] blobCbor = {(byte) 0x83, 0x01, 0x02, 0x03};
            MemorySegment blobBuf = arena.allocateFrom(JAVA_BYTE, blobCbor);
            MemorySegment blobResult = (MemorySegment) saveBlob.invokeExact(
                (SegmentAllocator) arena, handle, blobBuf, (long) blobCbor.length);
            byte[] digest = extractResult(blobResult);
            System.out.println("  Saved blob, digest: " + digest.length + " bytes");

            // Load blob
            MemorySegment digestBuf = arena.allocateFrom(JAVA_BYTE, digest);
            MemorySegment loadBlobResult = (MemorySegment) loadBlob.invokeExact(
                (SegmentAllocator) arena, handle, digestBuf, (long) digest.length);
            byte[] loaded = extractResult(loadBlobResult);
            System.out.println("  Loaded blob: " + loaded.length + " bytes");
        }

        storageFree.invokeExact(handle);
        System.out.println("  PASS\n");
    }

    // ==================== Sans-I/O approach ====================

    static void testSansio() throws Throwable {
        System.out.println("=== Sans-I/O Approach ===");
        var lib = loadLib("ffi_sansio");

        var newSaveId = downcall(lib, "sansio_driver_new_save_sedimentree_id",
            FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        var newLoadAllIds = downcall(lib, "sansio_driver_new_load_all_sedimentree_ids",
            FunctionDescriptor.of(ADDRESS));
        var newSaveBlob = downcall(lib, "sansio_driver_new_save_blob",
            FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        var nextEffect = downcall(lib, "sansio_driver_next_effect",
            FunctionDescriptor.of(FFI_EFFECT, ADDRESS));
        var provideResponse = downcall(lib, "sansio_driver_provide_response",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG));
        var finish = downcall(lib, "sansio_driver_finish",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS));

        try (Arena arena = Arena.ofConfined()) {
            // Test: save sedimentree ID
            byte[] testId = new byte[34];
            testId[0] = 0x58;
            testId[1] = 0x20;
            for (int i = 0; i < 32; i++) testId[i + 2] = (byte) i;
            MemorySegment idBuf = arena.allocateFrom(JAVA_BYTE, testId);

            MemorySegment driver = (MemorySegment) newSaveId.invokeExact(idBuf, (long) testId.length);

            // Drive: get effect
            MemorySegment effectSeg = (MemorySegment) nextEffect.invokeExact(
                (SegmentAllocator) arena, driver);
            int tag = (int) EFFECT_TAG.get(effectSeg, 0L);
            System.out.println("  Effect tag: " + tag + " (expected: 1 = SaveSedimentreeId)");

            // Provide empty response (unit operation)
            int complete = (int) provideResponse.invokeExact(driver, MemorySegment.NULL, 0L);
            System.out.println("  Complete: " + (complete != 0));

            // Finish
            MemorySegment result = (MemorySegment) finish.invokeExact(
                (SegmentAllocator) arena, driver);
            extractResult(result);
            System.out.println("  Save sedimentree ID: PASS");
        }

        System.out.println("  PASS\n");
    }

    // ==================== Driven approach ====================

    static void testDriven() throws Throwable {
        System.out.println("=== Driven Approach ===");
        var lib = loadLib("ffi_driven");

        var startSaveId = downcall(lib, "driven_start_save_sedimentree_id",
            FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        var nextEffect = downcall(lib, "driven_next_effect",
            FunctionDescriptor.of(FFI_EFFECT, ADDRESS));
        var provideResponse = downcall(lib, "driven_provide_response",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG));
        var finish = downcall(lib, "driven_finish",
            FunctionDescriptor.of(FFI_RESULT, ADDRESS));

        try (Arena arena = Arena.ofConfined()) {
            byte[] testId = new byte[34];
            testId[0] = 0x58;
            testId[1] = 0x20;
            for (int i = 0; i < 32; i++) testId[i + 2] = (byte) i;
            MemorySegment idBuf = arena.allocateFrom(JAVA_BYTE, testId);

            MemorySegment driver = (MemorySegment) startSaveId.invokeExact(idBuf, (long) testId.length);

            // Drive: get effect
            MemorySegment effectSeg = (MemorySegment) nextEffect.invokeExact(
                (SegmentAllocator) arena, driver);
            int tag = (int) EFFECT_TAG.get(effectSeg, 0L);
            System.out.println("  Effect tag: " + tag + " (expected: 1 = SaveSedimentreeId)");

            // Provide empty response
            int complete = (int) provideResponse.invokeExact(driver, MemorySegment.NULL, 0L);
            System.out.println("  Complete: " + (complete != 0));

            // Finish
            MemorySegment result = (MemorySegment) finish.invokeExact(
                (SegmentAllocator) arena, driver);
            extractResult(result);
            System.out.println("  Save sedimentree ID: PASS");
        }

        System.out.println("  PASS\n");
    }

    // ==================== Main ====================

    public static void main(String[] args) throws Throwable {
        testTokio();
        testSansio();
        testDriven();
        System.out.println("All approaches passed!");
    }
}
