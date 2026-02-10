package subduction_ffi_experiment

/*
#include "ffi_types.h"

typedef void* DrivenDriverHandle;

extern DrivenDriverHandle driven_start_save_sedimentree_id(const uint8_t *ptr, size_t len);
extern DrivenDriverHandle driven_start_delete_sedimentree_id(const uint8_t *ptr, size_t len);
extern DrivenDriverHandle driven_start_load_all_sedimentree_ids(const uint8_t *ptr, size_t len);
extern DrivenDriverHandle driven_start_save_blob(const uint8_t *ptr, size_t len);
extern DrivenDriverHandle driven_start_load_blob(const uint8_t *ptr, size_t len);
extern DrivenDriverHandle driven_start_delete_blob(const uint8_t *ptr, size_t len);

extern FfiEffect driven_next_effect(DrivenDriverHandle h);
extern int32_t   driven_provide_response(DrivenDriverHandle h, const uint8_t *ptr, size_t len);
extern int32_t   driven_is_complete(DrivenDriverHandle h);
extern FfiResult driven_finish(DrivenDriverHandle h);
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// driveDriven runs the Driven driver loop to completion.
// Same protocol as sans-I/O — the only difference is the Rust internals.
func driveDriven(handle C.DrivenDriverHandle, handler EffectHandler) ([]byte, error) {
	for {
		effect := C.driven_next_effect(handle)

		if effect.tag == C.uint32_t(EffectTagComplete) {
			if effect.data.ptr != nil {
				C.ffi_bytes_free(effect.data)
			}
			return extractResult(C.driven_finish(handle))
		}

		var effectData []byte
		if effect.data.ptr != nil && effect.data.len > 0 {
			effectData = C.GoBytes(unsafe.Pointer(effect.data.ptr), C.int(effect.data.len))
			C.ffi_bytes_free(effect.data)
		}

		responseData, err := handler(uint32(effect.tag), effectData)
		if err != nil {
			C.driven_finish(handle) // always consumes the handle
			return nil, fmt.Errorf("effect handler error (tag %d): %w", effect.tag, err)
		}

		rPtr, rLen := cBytes(responseData)
		complete := C.driven_provide_response(handle, rPtr, rLen)
		if complete != 0 {
			return extractResult(C.driven_finish(handle))
		}
	}
}

// DrivenStorage wraps the Driven approach.
type DrivenStorage struct {
	handler EffectHandler
}

func NewDrivenStorage(handler EffectHandler) *DrivenStorage {
	return &DrivenStorage{handler: handler}
}

func (s *DrivenStorage) SaveSedimentreeId(idCbor []byte) error {
	ptr, ln := cBytes(idCbor)
	h := C.driven_start_save_sedimentree_id(ptr, ln)
	_, err := driveDriven(h, s.handler)
	return err
}

func (s *DrivenStorage) LoadAllSedimentreeIds() ([]byte, error) {
	h := C.driven_start_load_all_sedimentree_ids(nil, 0)
	return driveDriven(h, s.handler)
}

func (s *DrivenStorage) SaveBlob(blobCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(blobCbor)
	h := C.driven_start_save_blob(ptr, ln)
	return driveDriven(h, s.handler)
}

func (s *DrivenStorage) LoadBlob(digestCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(digestCbor)
	h := C.driven_start_load_blob(ptr, ln)
	return driveDriven(h, s.handler)
}
