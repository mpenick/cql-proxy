/*
  Copyright (c) Michael Penick

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
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
