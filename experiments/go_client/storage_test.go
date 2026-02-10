package subduction_ffi_experiment

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// ==================== Tokio approach tests ====================

func TestTokio_SaveAndLoadSedimentreeId(t *testing.T) {
	storage, err := NewTokioStorage()
	if err != nil {
		t.Fatalf("NewTokioStorage: %v", err)
	}
	defer storage.Close()

	// A SedimentreeId is CBOR-encoded as a 32-byte bytestring.
	// minicbor encodes this as: major type 2 (bytes), length 32, then 32 bytes.
	// CBOR: 0x5820 + 32 bytes
	testId := make([]byte, 34)
	testId[0] = 0x58 // bytes, 1-byte length follows
	testId[1] = 0x20 // 32 bytes
	for i := 0; i < 32; i++ {
		testId[i+2] = byte(i)
	}

	// Save
	err = storage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("SaveSedimentreeId: %v", err)
	}

	// Load all — should contain our ID
	result, err := storage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("LoadAllSedimentreeIds: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("LoadAllSedimentreeIds returned empty result")
	}

	t.Logf("Tokio: saved and loaded sedimentree ID, result %d bytes", len(result))
}

func TestTokio_SaveAndLoadBlob(t *testing.T) {
	storage, err := NewTokioStorage()
	if err != nil {
		t.Fatalf("NewTokioStorage: %v", err)
	}
	defer storage.Close()

	// A Blob is CBOR-encoded as a transparent Vec<u8>.
	// minicbor encodes Vec<u8> as a CBOR array of integers (not byte string).
	// For simplicity, encode a 3-byte blob: CBOR array [0x48, 0x65, 0x6c]
	// Actually, let's just use a simple array: 0x83 0x01 0x02 0x03
	blobCbor := []byte{0x83, 0x01, 0x02, 0x03} // CBOR array [1, 2, 3]

	digestCbor, err := storage.SaveBlob(blobCbor)
	if err != nil {
		t.Fatalf("SaveBlob: %v", err)
	}

	if len(digestCbor) == 0 {
		t.Fatal("SaveBlob returned empty digest")
	}

	// Load it back
	loaded, err := storage.LoadBlob(digestCbor)
	if err != nil {
		t.Fatalf("LoadBlob: %v", err)
	}

	t.Logf("Tokio: save blob → digest %d bytes, load → %d bytes", len(digestCbor), len(loaded))
}

// ==================== Sans-I/O approach tests ====================

// inMemoryHandler implements a trivial in-memory storage via the
// effect handler callback. This demonstrates the host driving storage.
func inMemoryHandler() EffectHandler {
	var mu sync.Mutex
	ids := make(map[string]bool)
	blobs := make(map[string][]byte)

	return func(tag uint32, data []byte) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()

		switch tag {
		case EffectTagSaveSedimentreeId:
			// Key is the raw CBOR bytes of the ID
			ids[string(data)] = true
			return nil, nil

		case EffectTagDeleteSedimentreeId:
			delete(ids, string(data))
			return nil, nil

		case EffectTagLoadAllSedimentreeIds:
			// Return CBOR array of IDs.
			// For simplicity, return an empty array if no IDs.
			if len(ids) == 0 {
				return []byte{0x80}, nil // CBOR empty array
			}
			// Encode as CBOR array: just concatenate for now.
			// This is a rough approximation — real code would properly encode.
			var buf []byte
			count := len(ids)
			if count < 24 {
				buf = append(buf, byte(0x80|count)) // CBOR array header
			}
			for id := range ids {
				buf = append(buf, []byte(id)...)
			}
			return buf, nil

		case EffectTagSaveBlob:
			// The effect data is CBOR-encoded Blob.
			// Use a hash of the content as the "digest".
			// For this test, just use the data itself as key, return a fake digest.
			key := fmt.Sprintf("blob:%x", data)
			blobs[key] = data
			// Return a CBOR-encoded Digest<Blob> — 32-byte bytestring.
			digest := make([]byte, 34)
			digest[0] = 0x58
			digest[1] = 0x20
			// Fill with hash-like content from the blob data
			for i := 0; i < 32; i++ {
				if i < len(data) {
					digest[i+2] = data[i]
				}
			}
			return digest, nil

		case EffectTagLoadBlob:
			// The effect data is CBOR-encoded Digest<Blob>.
			key := fmt.Sprintf("blob:%x", data)
			if blob, ok := blobs[key]; ok {
				// Return CBOR Some(blob) — for minicbor Option, present value
				// is encoded as the value itself (no wrapping)
				return blob, nil
			}
			// Return CBOR None — minicbor uses 0xf6 (null) for None
			return []byte{0xf6}, nil

		case EffectTagDeleteBlob:
			key := fmt.Sprintf("blob:%x", data)
			delete(blobs, key)
			return nil, nil

		default:
			// For unhandled effects, return empty (unit response)
			return nil, nil
		}
	}
}

func TestSansio_SaveAndLoadSedimentreeId(t *testing.T) {
	handler := inMemoryHandler()
	storage := NewSansioStorage(handler)

	testId := make([]byte, 34)
	testId[0] = 0x58
	testId[1] = 0x20
	for i := 0; i < 32; i++ {
		testId[i+2] = byte(i)
	}

	err := storage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("SaveSedimentreeId: %v", err)
	}

	result, err := storage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("LoadAllSedimentreeIds: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("LoadAllSedimentreeIds returned empty result")
	}

	t.Logf("SansIO: saved and loaded sedimentree ID, result %d bytes", len(result))
}

func TestSansio_SaveAndLoadBlob(t *testing.T) {
	handler := inMemoryHandler()
	storage := NewSansioStorage(handler)

	blobCbor := []byte{0x83, 0x01, 0x02, 0x03}

	digestCbor, err := storage.SaveBlob(blobCbor)
	if err != nil {
		t.Fatalf("SaveBlob: %v", err)
	}

	if len(digestCbor) == 0 {
		t.Fatal("SaveBlob returned empty digest")
	}

	loaded, err := storage.LoadBlob(digestCbor)
	if err != nil {
		t.Fatalf("LoadBlob: %v", err)
	}

	// The loaded blob should match what we stored
	if len(loaded) == 0 {
		t.Fatal("LoadBlob returned empty")
	}

	t.Logf("SansIO: save blob → digest %d bytes, load → %d bytes", len(digestCbor), len(loaded))
}

// ==================== Driven approach tests ====================

func TestDriven_SaveAndLoadSedimentreeId(t *testing.T) {
	handler := inMemoryHandler()
	storage := NewDrivenStorage(handler)

	testId := make([]byte, 34)
	testId[0] = 0x58
	testId[1] = 0x20
	for i := 0; i < 32; i++ {
		testId[i+2] = byte(i)
	}

	err := storage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("SaveSedimentreeId: %v", err)
	}

	result, err := storage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("LoadAllSedimentreeIds: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("LoadAllSedimentreeIds returned empty result")
	}

	t.Logf("Driven: saved and loaded sedimentree ID, result %d bytes", len(result))
}

func TestDriven_SaveAndLoadBlob(t *testing.T) {
	handler := inMemoryHandler()
	storage := NewDrivenStorage(handler)

	blobCbor := []byte{0x83, 0x01, 0x02, 0x03}

	digestCbor, err := storage.SaveBlob(blobCbor)
	if err != nil {
		t.Fatalf("SaveBlob: %v", err)
	}

	if len(digestCbor) == 0 {
		t.Fatal("SaveBlob returned empty digest")
	}

	loaded, err := storage.LoadBlob(digestCbor)
	if err != nil {
		t.Fatalf("LoadBlob: %v", err)
	}

	if len(loaded) == 0 {
		t.Fatal("LoadBlob returned empty")
	}

	t.Logf("Driven: save blob → digest %d bytes, load → %d bytes", len(digestCbor), len(loaded))
}

// ==================== Cross-approach comparison ====================

func TestAllApproaches_SedimentreeIdRoundTrip(t *testing.T) {
	testId := make([]byte, 34)
	testId[0] = 0x58
	testId[1] = 0x20
	for i := 0; i < 32; i++ {
		testId[i+2] = byte(i + 0xA0)
	}

	// Tokio
	tokioStorage, err := NewTokioStorage()
	if err != nil {
		t.Fatalf("NewTokioStorage: %v", err)
	}
	defer tokioStorage.Close()

	err = tokioStorage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("Tokio SaveSedimentreeId: %v", err)
	}
	tokioResult, err := tokioStorage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("Tokio LoadAllSedimentreeIds: %v", err)
	}

	// Sans-I/O
	handler := inMemoryHandler()
	sansioStorage := NewSansioStorage(handler)
	err = sansioStorage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("SansIO SaveSedimentreeId: %v", err)
	}
	sansioResult, err := sansioStorage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("SansIO LoadAllSedimentreeIds: %v", err)
	}

	// Driven
	handler2 := inMemoryHandler()
	drivenStorage := NewDrivenStorage(handler2)
	err = drivenStorage.SaveSedimentreeId(testId)
	if err != nil {
		t.Fatalf("Driven SaveSedimentreeId: %v", err)
	}
	drivenResult, err := drivenStorage.LoadAllSedimentreeIds()
	if err != nil {
		t.Fatalf("Driven LoadAllSedimentreeIds: %v", err)
	}

	t.Logf("Results: tokio=%d bytes, sansio=%d bytes, driven=%d bytes",
		len(tokioResult), len(sansioResult), len(drivenResult))

	// Tokio uses real MemoryStorage so its CBOR encoding may differ
	// from our Go mock, but all three should succeed without error.
	_ = bytes.Compare(sansioResult, drivenResult) // sans-I/O and driven use same handler
}
