#ifndef FFI_TYPES_H
#define FFI_TYPES_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    uint8_t *ptr;
    size_t   len;
} FfiBytes;

typedef struct {
    int32_t  status;
    FfiBytes data;
    char    *error_msg;
} FfiResult;

typedef struct {
    uint32_t tag;
    FfiBytes data;
} FfiEffect;

// From ffi_common
extern void ffi_bytes_free(FfiBytes bytes);
extern void ffi_error_free(char *msg);

#endif
