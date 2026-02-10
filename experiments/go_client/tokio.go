package subduction_ffi_experiment

/*
#include "ffi_types.h"

typedef void* StorageHandle;

extern StorageHandle storage_new(void);
extern void          storage_free(StorageHandle h);

extern FfiResult storage_save_sedimentree_id(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_sedimentree_id(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_all_sedimentree_ids(StorageHandle h);
extern FfiResult storage_save_loose_commit(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_loose_commit(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_list_commit_digests(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_loose_commits(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_loose_commit(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_loose_commits(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_save_fragment(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_fragment(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_list_fragment_digests(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_fragments(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_fragment(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_fragments(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_save_blob(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_blob(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_load_blobs(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_delete_blob(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_save_commit_with_blob(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_save_fragment_with_blob(StorageHandle h, const uint8_t *ptr, size_t len);
extern FfiResult storage_save_batch(StorageHandle h, const uint8_t *ptr, size_t len);
*/
import "C"
import "fmt"

// TokioStorage wraps the Rust tokio-backed storage via FFI.
type TokioStorage struct {
	handle C.StorageHandle
}

func NewTokioStorage() (*TokioStorage, error) {
	h := C.storage_new()
	if h == nil {
		return nil, fmt.Errorf("failed to create tokio storage")
	}
	return &TokioStorage{handle: h}, nil
}

func (s *TokioStorage) Close() {
	if s.handle != nil {
		C.storage_free(s.handle)
		s.handle = nil
	}
}

func (s *TokioStorage) SaveSedimentreeId(idCbor []byte) error {
	ptr, ln := cBytes(idCbor)
	_, err := extractResult(C.storage_save_sedimentree_id(s.handle, ptr, ln))
	return err
}

func (s *TokioStorage) DeleteSedimentreeId(idCbor []byte) error {
	ptr, ln := cBytes(idCbor)
	_, err := extractResult(C.storage_delete_sedimentree_id(s.handle, ptr, ln))
	return err
}

func (s *TokioStorage) LoadAllSedimentreeIds() ([]byte, error) {
	return extractResult(C.storage_load_all_sedimentree_ids(s.handle))
}

func (s *TokioStorage) SaveBlob(blobCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(blobCbor)
	return extractResult(C.storage_save_blob(s.handle, ptr, ln))
}

func (s *TokioStorage) LoadBlob(digestCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(digestCbor)
	return extractResult(C.storage_load_blob(s.handle, ptr, ln))
}

func (s *TokioStorage) DeleteBlob(digestCbor []byte) error {
	ptr, ln := cBytes(digestCbor)
	_, err := extractResult(C.storage_delete_blob(s.handle, ptr, ln))
	return err
}
