package subduction_ffi_experiment

/*
#include "ffi_types.h"

typedef void* SansioDriverHandle;

extern SansioDriverHandle sansio_driver_new_save_sedimentree_id(const uint8_t *ptr, size_t len);
extern SansioDriverHandle sansio_driver_new_delete_sedimentree_id(const uint8_t *ptr, size_t len);
extern SansioDriverHandle sansio_driver_new_load_all_sedimentree_ids(void);
extern SansioDriverHandle sansio_driver_new_save_blob(const uint8_t *ptr, size_t len);
extern SansioDriverHandle sansio_driver_new_load_blob(const uint8_t *ptr, size_t len);
extern SansioDriverHandle sansio_driver_new_delete_blob(const uint8_t *ptr, size_t len);

extern FfiEffect sansio_driver_next_effect(SansioDriverHandle d);
extern int32_t   sansio_driver_provide_response(SansioDriverHandle d, const uint8_t *ptr, size_t len);
extern int32_t   sansio_driver_is_complete(SansioDriverHandle d);
extern FfiResult sansio_driver_finish(SansioDriverHandle d);
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// driveSansio runs the sans-I/O driver loop to completion.
func driveSansio(handle C.SansioDriverHandle, handler EffectHandler) ([]byte, error) {
	for {
		effect := C.sansio_driver_next_effect(handle)

		if effect.tag == C.uint32_t(EffectTagComplete) {
			if effect.data.ptr != nil {
				C.ffi_bytes_free(effect.data)
			}
			return extractResult(C.sansio_driver_finish(handle))
		}

		var effectData []byte
		if effect.data.ptr != nil && effect.data.len > 0 {
			effectData = C.GoBytes(unsafe.Pointer(effect.data.ptr), C.int(effect.data.len))
			C.ffi_bytes_free(effect.data)
		}

		responseData, err := handler(uint32(effect.tag), effectData)
		if err != nil {
			C.sansio_driver_finish(handle) // always consumes the handle
			return nil, fmt.Errorf("effect handler error (tag %d): %w", effect.tag, err)
		}

		rPtr, rLen := cBytes(responseData)
		complete := C.sansio_driver_provide_response(handle, rPtr, rLen)
		if complete != 0 {
			return extractResult(C.sansio_driver_finish(handle))
		}
	}
}

// SansioStorage wraps the sans-I/O approach.
type SansioStorage struct {
	handler EffectHandler
}

func NewSansioStorage(handler EffectHandler) *SansioStorage {
	return &SansioStorage{handler: handler}
}

func (s *SansioStorage) SaveSedimentreeId(idCbor []byte) error {
	ptr, ln := cBytes(idCbor)
	h := C.sansio_driver_new_save_sedimentree_id(ptr, ln)
	_, err := driveSansio(h, s.handler)
	return err
}

func (s *SansioStorage) LoadAllSedimentreeIds() ([]byte, error) {
	h := C.sansio_driver_new_load_all_sedimentree_ids()
	return driveSansio(h, s.handler)
}

func (s *SansioStorage) SaveBlob(blobCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(blobCbor)
	h := C.sansio_driver_new_save_blob(ptr, ln)
	return driveSansio(h, s.handler)
}

func (s *SansioStorage) LoadBlob(digestCbor []byte) ([]byte, error) {
	ptr, ln := cBytes(digestCbor)
	h := C.sansio_driver_new_load_blob(ptr, ln)
	return driveSansio(h, s.handler)
}
