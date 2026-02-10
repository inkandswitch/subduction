package subduction_ffi_experiment

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/debug -lffi_tokio -lffi_sansio -lffi_driven
#include "ffi_types.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Effect tag constants — must match ffi_common/src/abi.rs
const (
	EffectTagComplete              = 0
	EffectTagSaveSedimentreeId     = 1
	EffectTagDeleteSedimentreeId   = 2
	EffectTagLoadAllSedimentreeIds = 3
	EffectTagSaveLooseCommit       = 10
	EffectTagLoadLooseCommit       = 11
	EffectTagListCommitDigests     = 12
	EffectTagLoadLooseCommits      = 13
	EffectTagDeleteLooseCommit     = 14
	EffectTagDeleteLooseCommits    = 15
	EffectTagSaveFragment          = 20
	EffectTagLoadFragment          = 21
	EffectTagListFragmentDigests   = 22
	EffectTagLoadFragments         = 23
	EffectTagDeleteFragment        = 24
	EffectTagDeleteFragments       = 25
	EffectTagSaveBlob              = 30
	EffectTagLoadBlob              = 31
	EffectTagLoadBlobs             = 32
	EffectTagDeleteBlob            = 33
	EffectTagSaveCommitWithBlob    = 40
	EffectTagSaveFragmentWithBlob  = 41
	EffectTagSaveBatch             = 42
)

// EffectHandler is called by driver loops for each effect.
type EffectHandler func(tag uint32, data []byte) ([]byte, error)

// extractResult converts an FfiResult to Go bytes + error.
func extractResult(r C.FfiResult) ([]byte, error) {
	if r.status != 0 {
		var msg string
		if r.error_msg != nil {
			msg = C.GoString(r.error_msg)
			C.ffi_error_free(r.error_msg)
		} else {
			msg = fmt.Sprintf("FFI error (status %d)", r.status)
		}
		return nil, fmt.Errorf("%s", msg)
	}

	var data []byte
	if r.data.ptr != nil && r.data.len > 0 {
		data = C.GoBytes(unsafe.Pointer(r.data.ptr), C.int(r.data.len))
		C.ffi_bytes_free(r.data)
	}
	return data, nil
}

// cBytes converts a Go byte slice to C pointers.
func cBytes(b []byte) (*C.uint8_t, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}
