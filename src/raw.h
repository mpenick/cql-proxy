#ifndef RAW_H
#define RAW_H

#include <cassandra.h>

typedef struct CassRawResult_ CassRawResult;

extern CassFuture* cass_session_execute_raw(CassSession* session,
                                            cass_uint8_t opcode, cass_uint8_t flags,
                                            const char* frame, size_t frame_size);

extern const CassRawResult* cass_future_get_raw_result(CassFuture* future);

extern void cass_raw_result_free(const CassRawResult* result);

extern cass_uint8_t cass_raw_result_opcode(const CassRawResult* result);

extern const char* cass_raw_result_frame(const CassRawResult* result);

extern size_t cass_raw_result_frame_length(const CassRawResult* result);

#endif // RAW_H
