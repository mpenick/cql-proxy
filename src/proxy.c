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
#include <uv.h>

#include <assert.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "uthash.h"
#include "picohash.h"

#include "parse.h"
#include "raw.h"
#include "serde.h"

#define CQL_OPCODE_ERROR 0x00
#define CQL_OPCODE_STARTUP 0x01
#define CQL_OPCODE_READY 0x02
#define CQL_OPCODE_AUTHENTICATE 0x03
#define CQL_OPCODE_CREDENTIALS 0x04
#define CQL_OPCODE_OPTIONS 0x05
#define CQL_OPCODE_SUPPORTED 0x06
#define CQL_OPCODE_QUERY 0x07
#define CQL_OPCODE_RESULT 0x08
#define CQL_OPCODE_PREPARE 0x09
#define CQL_OPCODE_EXECUTE 0x0A
#define CQL_OPCODE_REGISTER 0x0B
#define CQL_OPCODE_EVENT 0x0C
#define CQL_OPCODE_BATCH 0x0D
#define CQL_OPCODE_AUTH_CHALLENGE 0x0E
#define CQL_OPCODE_AUTH_RESPONSE 0x0F
#define CQL_OPCODE_AUTH_SUCCESS 0x10
#define CQL_OPCODE_CANCEL 0xFF

#define CQL_ERROR_SERVER_ERROR 0x0000
#define CQL_ERROR_PROTOCOL_ERROR 0x000A
#define CQL_ERROR_BAD_CREDENTIALS 0x0100
#define CQL_ERROR_UNAVAILABLE 0x1000
#define CQL_ERROR_OVERLOADED 0x1001
#define CQL_ERROR_IS_BOOTSTRAPPING 0x1002
#define CQL_ERROR_TRUNCATE_ERROR 0x1003
#define CQL_ERROR_WRITE_TIMEOUT 0x1100
#define CQL_ERROR_READ_TIMEOUT 0x1200
#define CQL_ERROR_READ_FAILURE 0x1300
#define CQL_ERROR_FUNCTION_FAILURE 0x1400
#define CQL_ERROR_WRITE_FAILURE 0x1500
#define CQL_ERROR_SYNTAX_ERROR 0x2000
#define CQL_ERROR_UNAUTHORIZED 0x2100
#define CQL_ERROR_INVALID_QUERY 0x2200
#define CQL_ERROR_CONFIG_ERROR 0x2300
#define CQL_ERROR_ALREADY_EXISTS 0x2400
#define CQL_ERROR_UNPREPARED 0x2500
#define CQL_ERROR_CLIENT_WRITE_FAILURE 0x8000

#define CQL_QUERY_FLAG_VALUES 0x00000001
#define CQL_QUERY_FLAG_SKIP_METADATA 0x00000002
#define CQL_QUERY_FLAG_PAGE_SIZE 0x00000004
#define CQL_QUERY_FLAG_PAGING_STATE 0x00000008
#define CQL_QUERY_FLAG_SERIAL_CONSISTENCY 0x00000010
#define CQL_QUERY_FLAG_DEFAULT_TIMESTAMP 0x00000020
#define CQL_QUERY_FLAG_NAMES_FOR_VALUES 0x00000040
#define CQL_QUERY_FLAG_WITH_KEYSPACE 0x00000080
#define CQL_QUERY_FLAG_PAGE_SIZE_BYTES 0x40000000
#define CQL_QUERY_FLAG_CONTINUOUS_PAGING 0x80000000

#define CQL_RESULT_FLAG_GLOBAL_TABLESPEC 0x00000001
#define CQL_RESULT_FLAG_HAS_MORE_PAGES 0x00000002
#define CQL_RESULT_FLAG_NO_METADATA 0x00000004
#define CQL_RESULT_FLAG_METADATA_CHANGED 0x00000008
#define CQL_RESULT_FLAG_CONTINUOUS_PAGING 0x40000000
#define CQL_RESULT_FLAG_LAST_CONTINUOUS_PAGE 0x80000000

#define CQL_RESULT_KIND_VOID 1
#define CQL_RESULT_KIND_ROWS 2
#define CQL_RESULT_KIND_SET_KEYSPACE 3
#define CQL_RESULT_KIND_PREPARED 4
#define CQL_RESULT_KIND_SCHEMA_CHANGE 5

#define CQL_TYPE_CUSTOM 0x0000
#define CQL_TYPE_ASCII 0x0001
#define CQL_TYPE_BIGINT 0x0002
#define CQL_TYPE_BLOG 0x0003
#define CQL_TYPE_BOOLEAN 0x0004
#define CQL_TYPE_COUNTER 0x0005
#define CQL_TYPE_DECIMAL 0x0006
#define CQL_TYPE_DOUBLE 0x0007
#define CQL_TYPE_FLOAT 0x0008
#define CQL_TYPE_INT 0x0009
#define CQL_TYPE_TIMESTAMP 0x000B
#define CQL_TYPE_UUID 0x000C
#define CQL_TYPE_VARCHAR 0x000D
#define CQL_TYPE_VARINT 0x000E
#define CQL_TYPE_TIMEUUD 0x000F
#define CQL_TYPE_INET 0x0010
#define CQL_TYPE_DATE 0x0011
#define CQL_TYPE_TIME 0x0012
#define CQL_TYPE_SMALLINT 0x0013
#define CQL_TYPE_TINYINT 0x0014
#define CQL_TYPE_LIST 0x0020
#define CQL_TYPE_MAP 0x0021
#define CQL_TYPE_SET 0x0022
#define CQL_TYPE_UDT 0x0030
#define CQL_TYPE_TUPLE 0x0031

#define VALUE_TYPE_NULL 0
#define VALUE_TYPE_SIMPLE 1
#define VALUE_TYPE_COLL 2

#define MAX_CLIENTS 128

#define MAX_BATCH 64
#define MAX_BATCH_SIZE 1024

#define MAX_QUEUED 64

#define MAX_FRAME_SIZE 8388608 // 8MB
#define MAX_BODY_INLINED 8192 // 8KB

typedef struct column_type_s column_type_t;
typedef struct value_s value_t;

typedef struct client_s client_t;
typedef struct client_queue_s client_queue_t;
typedef struct request_s request_t;
typedef struct response_s response_t;
typedef struct batch_s batch_t;
typedef struct prepared_s prepared_t;
typedef struct session_s session_t;
typedef struct queued_s queued_t;

static CassSession *session;
static CassCluster *cluster;

static uv_async_t async;
static uv_prepare_t prepare;
static const char *cassandra_version = NULL;
static const char *cassandra_parititioner = NULL;

typedef void (*client_queue_cb)(client_t *client);

struct client_queue_s {
  client_t *clients[MAX_CLIENTS];
  size_t count;
  uv_mutex_t mutex;
};

static client_queue_t to_flush;
static client_queue_t use_keyspace_success;
static client_queue_t use_keyspace_failed;

struct column_type_s {
  int16_t basic;
  int16_t sub_types[2]; // Only support primitive types
};

typedef struct {
  const char *name;
  column_type_t type;
} column_t;

typedef struct {
  int32_t count;
  column_t *columns;
} columns_t;

typedef struct {
  int32_t len;
  char data[128];
} bytes_t;

typedef struct {
  int32_t count;
  bytes_t *values;
} collection_t;

struct value_s {
  int type;
  union {
    bytes_t value;
    collection_t coll;
  };
};

typedef struct {
  value_t *columns;
} row_t;

typedef struct {
  int32_t count;
  row_t *rows;
} rows_t;

bytes_t integer_value(int32_t value) {
  bytes_t bytes;
  char *pos = bytes.data;
  pos = encode_int32(pos, value);
  bytes.len = 4;
  return bytes;
}

bytes_t varchar_value(const char *value) {
  bytes_t bytes;
  char *pos = bytes.data;
  int32_t len = (int32_t)strlen(value);
  memcpy(pos, value, (size_t)len);
  bytes.len = len;
  return bytes;
}

bytes_t uuid_value(const char *value) {
  size_t len = strlen(value);
  if (value == NULL || len != 36) {
    abort();
  }

  // clang-format off
  static const signed char hex_to_half_byte[256] = {
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
     0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  10,  11,  12,  13,  14,  15,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  10,  11,  12,  13,  14,  15,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
    -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
  };
  // clang-format on

  bytes_t bytes;
  const char *end = value + 36;
  const char *pos = value;
  for (size_t i = 0; i < 16; ++i) {
    if (pos < end && *pos == '-')
      pos++;
    if (pos + 2 > end) {
      abort();
    }
    uint8_t p0 = (uint8_t)pos[0];
    uint8_t p1 = (uint8_t)(pos[1]);
    if (hex_to_half_byte[p0] == -1 || hex_to_half_byte[p1] == -1) {
      abort();
    }
    bytes.data[i] = (char)(hex_to_half_byte[p0] << 4) + hex_to_half_byte[p1];
    pos += 2;
  }
  bytes.len = 16;
  return bytes;
}

bytes_t inet_value(const char *value) {
  bytes_t bytes;
  char addr[16];
  int32_t len;
  char *pos = bytes.data;
  if (uv_inet_pton(AF_INET, value, addr) == 0) {
    len = 4;
  } else if (uv_inet_pton(AF_INET6, value, addr) == 0) {
    len = 16;
  } else {
    abort();
  }
  memcpy(pos, addr, (size_t)len);
  bytes.len = len;
  return bytes;
}

char *encode_header(char *pos, int16_t stream, int8_t opcode) {
  pos = encode_int8(pos, (int8_t)0x84); // Version
  pos = encode_int8(pos, 0);            // TODO: Flags
  pos = encode_int16(pos, stream);      // Stream
  pos = encode_int8(pos, opcode);       // Opcode
  return pos;
}

char *encode_column_type(char *pos, const column_type_t *type) {
  switch (type->basic) {
  case CQL_TYPE_VARCHAR:
  case CQL_TYPE_INET:
  case CQL_TYPE_INT:
  case CQL_TYPE_UUID:
  case CQL_TYPE_TIMEUUD:
    pos = encode_int16(pos, type->basic);
    break;
  case CQL_TYPE_SET:
    pos = encode_int16(pos, type->basic);
    pos = encode_int16(pos, type->sub_types[0]);
    break;
  default:
    abort();
  }
  return pos;
}

char *encode_collection(char *pos, const collection_t *coll) {
  int32_t len = 4;
  for (int32_t i = 0; i < coll->count; ++i) {
    len += 4 + coll->values[i].len;
  }
  pos = encode_int32(pos, len);         // Size
  pos = encode_int32(pos, coll->count); // Count
  // Elements
  for (int32_t i = 0; i < coll->count; ++i) {
    const bytes_t *value = &coll->values[i];
    pos = encode_long_string(pos, value->data, (size_t)value->len);
  }
  return pos;
}

char *encode_row(char *pos, int32_t column_count, const row_t *row) {
  for (int32_t i = 0; i < column_count; ++i) {
    const value_t *column = &row->columns[i];
    switch (column->type) {
    case VALUE_TYPE_NULL:
      pos = encode_int32(pos, -1);
      break;
    case VALUE_TYPE_SIMPLE:
      pos = encode_long_string(pos, column->value.data, (size_t)column->value.len);
      break;
    case VALUE_TYPE_COLL:
      pos = encode_collection(pos, &column->coll);
      break;
    default:
      abort();
    }
  }
  return pos;
}

char *encode_empty_rows(char *pos) {
  pos = encode_int32(pos, CQL_RESULT_KIND_ROWS); // Kind
  pos = encode_int32(pos, 0);                    // Flags
  pos = encode_int32(pos, 0);                    // Column count
  pos = encode_int32(pos, 0);                    // Row count
  return pos;
}

char *encode_rows(char *pos, bool no_metadata, const char *keyspace,
                  const char *table, const columns_t *columns,
                  const rows_t *rows) {
  pos = encode_int32(pos, CQL_RESULT_KIND_ROWS); // Kind

  if (no_metadata) {
    pos = encode_int32(pos, CQL_RESULT_FLAG_NO_METADATA); // Flags
    pos = encode_int32(pos, columns->count);              // Column count
  } else {
    pos = encode_int32(pos, CQL_RESULT_FLAG_GLOBAL_TABLESPEC); // Flags
    pos = encode_int32(pos, columns->count);                   // Column count

    pos = encode_string(pos, keyspace,
                        strlen(keyspace)); // Keyspace (global table spec)
    pos = encode_string(pos, table, strlen(table)); // Table (global table spec)

    // Columns
    for (int32_t i = 0; i < columns->count; ++i) {
      const column_t *column = &columns->columns[i];
      pos = encode_string(pos, column->name, strlen(column->name));
      pos = encode_column_type(pos, &column->type);
    }
  }

  pos = encode_int32(pos, rows->count); // Row count

  // Rows
  for (int32_t i = 0; i < rows->count; ++i) {
    pos = encode_row(pos, columns->count, &rows->rows[i]);
  }
  return pos;
}

#if defined(__GNUC__) || defined(__clang__)
#define ATTRIBUTE_FORMAT(string, first) __attribute__((__format__(__printf__, string, first)))
#else
#define ATTRIBUTE_FORMAT(string, first)
#endif

ATTRIBUTE_FORMAT(1, 2)
static void print(const char *format, ...) {
  va_list args;
  va_start(args, format);
  vfprintf(stdout, format, args); fflush(stdout);
  va_end(args);
}

typedef struct {
  int32_t count;
  uint16_t *pk_indexes;
} primary_keys_t;

struct prepared_s {
  char hash[PICOHASH_MD5_DIGEST_LENGTH];
  char query[512];
  statement_t stmt;
  UT_hash_handle hh;
};

static prepared_t *prepared_cache = NULL;

prepared_t *find_prepared(const char *hash) {
  prepared_t *prepared;
  HASH_FIND(hh, prepared_cache, hash, PICOHASH_MD5_DIGEST_LENGTH, prepared);
  return prepared;
}

prepared_t *add_prepared(const char *query) {
  prepared_t *prepared = malloc(sizeof(prepared_t));
  strncpy(prepared->query, query, sizeof(prepared->query) - 1);

  picohash_ctx_t ctx;
  picohash_init_md5(&ctx);
  picohash_update(&ctx, query, strlen(query));
  picohash_final(&ctx, prepared->hash);

  // Check for collisions?

  prepared_t *existing = find_prepared(prepared->hash);
  if (existing) {
    HASH_DEL(prepared_cache, existing);
    free(existing);
  }

  HASH_ADD(hh, prepared_cache, hash[0], PICOHASH_MD5_DIGEST_LENGTH, prepared);

  return prepared;
}


struct session_s {
  char keyspace [64];
  bool is_connected;
  CassSession *session;
  UT_hash_handle hh;
};

static session_t *session_cache = NULL;

session_t *find_session(const char* keyspace) {
  session_t *session = NULL;
  HASH_FIND_STR(session_cache, keyspace, session);
  return session;
}

session_t *get_session(const char* keyspace) {
  session_t *existing = find_session(keyspace);
  if (existing) {
    return existing;
  }

  session_t *session = malloc(sizeof(session_t));
  strncpy(session->keyspace, keyspace, sizeof(session->keyspace) - 1);
  session->is_connected = false;
  session->session = cass_session_new();

  HASH_ADD_STR(session_cache, keyspace, session);

  return session;
}

void init_client_queue(client_queue_t* queue) {
  queue->count = 0;
  uv_mutex_init(&queue->mutex);
}

// Thread-safe
void add_to_client_queue(client_queue_t *queue, client_t *client) {
  uv_mutex_lock(&queue->mutex);
  for (size_t i = 0; i < queue->count; ++i) {
    if (queue->clients[i] == client) {
      uv_mutex_unlock(&queue->mutex);
      return;
    }
  }
  queue->clients[queue->count++] = client;
  uv_mutex_unlock(&queue->mutex);
  uv_async_send(&async);
}

// Thread-safe
void process_client_queue(client_queue_t* queue, client_queue_cb cb) {
  client_t *copy[MAX_CLIENTS];
  size_t count = 0;

  uv_mutex_lock(&queue->mutex);
  count = queue->count;
  for (size_t i = 0;  i < count; ++i) {
    copy[i] = queue->clients[i];
  }
  queue->count = 0;
  uv_mutex_unlock(&queue->mutex);


  for (size_t i = 0; i < count; ++i) {
    cb(copy[i]);
  }
}

char *encode_prepared(char *pos, prepared_t* prepared, const char *keyspace,
                      const char *table, const columns_t *bind_markers,
                      const primary_keys_t *pks, const columns_t *columns) {
  pos = encode_int32(pos, CQL_RESULT_KIND_PREPARED);                // Kind
  pos = encode_string(pos, prepared->hash, sizeof(prepared->hash)); // Id
  pos = encode_int32(pos, CQL_RESULT_FLAG_GLOBAL_TABLESPEC);        // Flags
  pos = encode_int32(pos, bind_markers->count); // Bind marker column count
  pos = encode_int32(pos, pks->count);          // Primary key count

  // Primary key indexes
  for (int32_t i = 0; i < pks->count; ++i) {
    pos = encode_uint16(pos, pks->pk_indexes[i]);
  }

  pos = encode_string(pos, keyspace,
                      strlen(keyspace)); // Keyspace (global table spec)
  pos = encode_string(pos, table, strlen(table)); // Table (global table spec)

  // Bind marker bind_markers
  for (int32_t i = 0; i < bind_markers->count; ++i) {
    const column_t *column = &bind_markers->columns[i];
    pos = encode_string(pos, column->name, strlen(column->name));
    pos = encode_column_type(pos, &column->type);
  }

  // Result metadata
  pos = encode_int32(pos, CQL_RESULT_FLAG_GLOBAL_TABLESPEC); // Flags
  pos = encode_int32(pos, columns->count);                   // Column count
  pos = encode_string(pos, keyspace,
                      strlen(keyspace)); // Keyspace (global table spec)
  pos = encode_string(pos, table, strlen(table)); // Table (global table spec)

  // Columns
  for (int32_t i = 0; i < columns->count; ++i) {
    const column_t *column = &columns->columns[i];
    pos = encode_string(pos, column->name, strlen(column->name));
    pos = encode_column_type(pos, &column->type);
  }

  return pos;
}

static columns_t local_columns = {
    12,
    (column_t[]){{.name = "key", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rpc_address", {.basic = CQL_TYPE_INET}},
                 {.name = "data_center", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rack", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "tokens", {.basic = CQL_TYPE_SET, .sub_types = {CQL_TYPE_VARCHAR}}},
                 {.name = "release_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "partitioner", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "cluster_name", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "cql_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "schema_version", {.basic = CQL_TYPE_UUID}},
                 {.name = "native_protocol_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "host_id", {.basic = CQL_TYPE_UUID}}}};

static columns_t peers_columns = {
    8,
    (column_t[]){{.name = "peer", {.basic = CQL_TYPE_INET}},
                 {.name = "data_center", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rack", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "release_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rpc_address", {.basic = CQL_TYPE_INET}},
                 {.name = "schema_version", {.basic = CQL_TYPE_UUID}},
                 {.name = "host_id", {.basic = CQL_TYPE_UUID}},
                 {.name = "tokens",
                  {.basic = CQL_TYPE_SET, .sub_types = {CQL_TYPE_VARCHAR}}}}};

static rows_t empty_rows = {0};

struct queued_s {
  frame_t frame;
  char* body;
  char keyspace[64];
};

struct client_s {
  uv_tcp_t tcp;
  char data[64 * 1024];
  char *body;
  char body_inline[MAX_BODY_INLINED];
  char *body_pos;
  char keyspace[64];
  frame_t frame;
  batch_t *batch;
  batch_t *batches[MAX_BATCH];
  size_t batch_count;
  uv_mutex_t mutex;
  bool is_closing;
  queued_t queued[MAX_QUEUED];
  size_t queued_count;
  int16_t use_keyspace_stream;
};

struct request_s {
  client_t *client;
  int16_t stream;
};

typedef void (*response_free_cb)(response_t *response);

struct response_s {
  char header[9];
  char *body;
  size_t len;
  response_free_cb free_cb;
  void *data;
};

struct batch_s {
  response_t reqs[MAX_BATCH_SIZE];
  uv_buf_t bufs[MAX_BATCH_SIZE];
  uv_write_t req;
  unsigned int count;
  client_t *client;
  batch_t *next;
};

batch_t *alloc_batch(client_t *client) {
  batch_t *batch;
  batch = (batch_t *)malloc(sizeof(batch_t));
  batch->req.data = batch;
  batch->client = client;
  batch->count = 0;
  return batch;
}


void close_client(client_t *client);
void write_response_body(client_t *client, int8_t opcode, int16_t stream, const char *body, size_t body_size);
void write_response_result(client_t *client, int16_t stream, const CassRawResult *result);

void write_error(client_t *client, int16_t stream, int32_t code, const char *message, size_t len) {
  char body[128];
  if (len > sizeof(body) - 4 - 2) {
    len = sizeof(body) - 4 - 2;
  }
  char *pos = encode_int32(body, code);
  encode_string(pos, message, len);
  write_response_body(client, CQL_OPCODE_ERROR, stream, body, 4 + 2 + len);
}

void on_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  client_t *client = (client_t *)handle->data;
  buf->base = client->data;
  buf->len = sizeof(client->data);
}

void do_error(client_t *client, int32_t code, const char *message) {
  write_error(client, client->frame.stream, code, message, strlen(message));
}

void do_options(client_t *client) {
  char body[128];
  char *pos = body;
  pos = encode_uint16(pos, 2);
  pos = encode_const_string(pos, "CQL_VERSION");
  pos = encode_uint16(pos, 1);
  pos = encode_const_string(pos, "3.0.0");
  pos = encode_const_string(pos, "COMPRESSION");
  pos = encode_uint16(pos, 0);
  write_response_body(client, CQL_OPCODE_SUPPORTED, client->frame.stream, body, (size_t)(pos - body));
}

void do_startup_or_register(client_t *client) {
  char body[1];
  write_response_body(client, CQL_OPCODE_READY, client->frame.stream, body, 0);
}

void on_result(CassFuture *future, void *data) {
  request_t *request = (request_t*)data;
  const CassRawResult *result = cass_future_get_raw_result(future);
  if (result == NULL) {
    const char *message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    write_error(request->client, request->stream, CQL_ERROR_SERVER_ERROR, message, message_length);
  } else {
    write_response_result(request->client, request->stream, result);
  }
  cass_future_free(future);
  free(request);
}

void queue_request(client_t* client) {
  uv_mutex_lock(&client->mutex);
  if (client->queued_count == MAX_QUEUED) {
    do_error(client, CQL_ERROR_OVERLOADED, "Unable to handle request");
    uv_mutex_unlock(&client->mutex);
    return;
  }
  queued_t* entry = &client->queued[client->queued_count++];

  entry->frame = client->frame;
  entry->body = malloc((size_t)client->frame.length);
  memcpy(entry->body, client->body, (size_t)client->frame.length);
  uv_mutex_unlock(&client->mutex);
}

void do_request(client_t *client, frame_t* frame, char* body) {
  CassSession* connected_session = session;

  if (strlen(client->keyspace) > 0) {
    session_t* s = find_session(client->keyspace);
    if (s) {
      if (s->is_connected) {
        connected_session = s->session;
      } else {
        uv_read_stop((uv_stream_t *)&client->tcp);
        queue_request(client);
        return;
      }
    } else {
      do_error(client, CQL_ERROR_SERVER_ERROR, "Unable to find session for keyspace");
      return;
    }
  }

  CassFuture *future =  cass_session_execute_raw(connected_session,
                                                 (cass_uint8_t)frame->opcode, (cass_uint8_t)frame->flags,
                                                 body, (size_t)frame->length);
  request_t *request = malloc(sizeof(request_t));
  request->client = client;
  request->stream = client->frame.stream;
  cass_future_set_callback(future, on_result, request);
}

int32_t find_column(const char* name, const columns_t *columns) {
  for (int32_t i = 0; i < columns->count; ++i) {
    if (strcmp(name, columns->columns[i].name) == 0) {
      return i;
    }
  }
  return -1;
}

void write_prepared(client_t *client, statement_t *stmt,
                    const char *query, const char *keyspace,
                    const char *table, const columns_t *bind_markers,
                    const primary_keys_t *pks, const columns_t *columns) {
  char body[1024];
  prepared_t *prepared = add_prepared(query);
  prepared->stmt = *stmt;

  char *pos = NULL;
  if (!stmt || stmt->type == STMT_USE || (stmt && stmt->select.exprs_count > 0 && stmt->select.exprs[0].type == STMT_EXPR_STAR)) {
    pos = encode_prepared(body, prepared, keyspace, table, bind_markers, pks, columns);
  } else if (stmt && stmt->select.exprs_count > 0 && stmt->select.exprs[0].type == STMT_EXPR_COUNT) {
    columns_t columns_meta = {
      1,
      (column_t[]){
        {.name = "count", {.basic = CQL_TYPE_INT}}}};
    pos = encode_prepared(body, prepared, keyspace, table, bind_markers, pks, &columns_meta);
  } else {
    int exprs_count = stmt->select.exprs_count;
    if (exprs_count == 0) {
      do_error(client, CQL_ERROR_INVALID_QUERY, "Invalid select expressions"); // TODO: Figure out C* error
      return;
    }
    column_t columns_values[20];
    for (int32_t i = 0; i < exprs_count; ++i) {
      const statement_expr_t *expr = &stmt->select.exprs[i];
      int32_t c = find_column(expr->id, columns);
      if (c < 0) {
        do_error(client, CQL_ERROR_INVALID_QUERY, "Invalid column name"); // TODO: Figure out C* error
        return;
      }
      columns_values[i] = local_columns.columns[c];
      if (expr->type == STMT_EXPR_ALIAS) {
        columns_values[i].name = expr->alias;
      }
    }
    columns_t columns_meta = (columns_t){ exprs_count, columns_values };
    pos = encode_prepared(body, prepared, keyspace, table, bind_markers, pks, &columns_meta);
  }

  write_response_body(client, CQL_OPCODE_RESULT, client->frame.stream, body, (size_t)(pos - body));
}

void do_prepare(client_t *client) {
  char query[512] = {0};
  int32_t len = sizeof(query) - 1;

  { // Decode
    decode_long_string(client->body, query, &len);
    query[len] = '\0';
  }

  columns_t bind_markers = {0}; // TODO?
  primary_keys_t pks = {0};
  columns_t empty_columns = {0};

  statement_t stmt = {0};
  if (parse(&stmt, query, (size_t)len)) {

    switch (stmt.type) {
      case STMT_SELECT:
        if (stmt.select.is_table && strcmp(client->keyspace, "system") != 0) {
          do_request(client, &client->frame, client->body);
        } else {
          switch (stmt.select.table_type) {
            case TK_LOCAL: {
              write_prepared(client, &stmt, query, "system", "local",
                             &bind_markers, &pks, &local_columns);
            }
              break;
            case TK_PEERS: {
              write_prepared(client, &stmt, query, "system", "peers",
                             &bind_markers, &pks, &peers_columns);
            }
              break;
            case TK_PEERS_V2:
              do_error(client, CQL_ERROR_INVALID_QUERY, "Doesn't exist");
              break;
            default:
              do_request(client, &client->frame, client->body);
              break;
          }
        }
        break;
      case STMT_USE:
        write_prepared(client, &stmt, query, "", "",
                       &bind_markers, &pks, &empty_columns);
        break;
    }
  } else {
    do_request(client, &client->frame, client->body);
  }
}


void write_set_keyspace(client_t *client, int16_t stream) {
  char body[128];
  char* pos = encode_int32(body, CQL_RESULT_KIND_SET_KEYSPACE); // Kind
  pos = encode_string(pos, client->keyspace, strlen(client->keyspace)); // Keyspace
  write_response_body(client, CQL_OPCODE_RESULT, stream, body, (size_t)(pos - body));
}

void write_rows(client_t *client,
                const statement_t *stmt,
                const char *keyspace, const char *table,
                const columns_t *columns, const rows_t *rows) {
  char body[1024];

  char* pos = NULL;
  if (!stmt || (stmt && stmt->select.exprs_count > 0 && stmt->select.exprs[0].type == STMT_EXPR_STAR)) {
    pos = encode_rows(body, client->frame.flags & CQL_QUERY_FLAG_SKIP_METADATA,
                            keyspace, table, columns, rows);
  } else if (stmt && stmt->select.exprs_count > 0 && stmt->select.exprs[0].type == STMT_EXPR_COUNT) {
    columns_t count_columns = {
      1,
      (column_t[]){
        {.name = "count", {.basic = CQL_TYPE_INT}}}};
    rows_t count_rows = (rows_t){
                  1,
                  (row_t[]){{
                  (value_t[]){
    {.type = VALUE_TYPE_SIMPLE, .value = integer_value(stmt->select.table_type == TK_LOCAL ? 1 : 0)}}}}};
    pos = encode_rows(body, client->frame.flags & CQL_QUERY_FLAG_SKIP_METADATA,
                            keyspace, table, &count_columns, &count_rows);
  } else {
    int exprs_count = stmt->select.exprs_count;
    if (exprs_count == 0) {
      do_error(client, CQL_ERROR_INVALID_QUERY, "Invalid select expressions"); // TODO: Figure out C* error
      return;
    }

    column_t columns_values[20];
    value_t values[20];
    for (int32_t i = 0; i < exprs_count; ++i) {
      const statement_expr_t *expr = &stmt->select.exprs[i];
      int32_t c = find_column(expr->id, columns);
      if (c < 0) {
        do_error(client, CQL_ERROR_INVALID_QUERY, "Invalid column name"); // TODO: Figure out C* error
        return;
      }
      columns_values[i] = columns->columns[c];
      if (rows->count > 0) {
        values[i] = rows->rows->columns[c];
      }
      if (expr->type == STMT_EXPR_ALIAS) {
        columns_values[i].name = expr->alias;
      }
    }

    columns_t selected_columns = (columns_t){ exprs_count, columns_values };
    rows_t selected_rows = (rows_t){
                  1,
                  (row_t[]){{ values }}};
    pos = encode_rows(body, client->frame.flags & CQL_QUERY_FLAG_SKIP_METADATA,
                            keyspace, table, &selected_columns, rows->count > 0 ? &selected_rows : rows);
  }
  write_response_body(client, CQL_OPCODE_RESULT, client->frame.stream, body, (size_t)(pos - body));
}

void write_system_local(client_t *client, const statement_t *stmt) {
  rows_t local_rows = (rows_t){
                      1,
                      (row_t[]){{
                      (value_t[]){
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("local")},
    {.type = VALUE_TYPE_SIMPLE, .value = inet_value("127.0.0.1")}, // TODO: Get the correct local IP
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("dc1")},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("rack1")},
    {.type = VALUE_TYPE_COLL, .coll = {1, (bytes_t[]){varchar_value("0")}}},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value(cassandra_version)},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value(cassandra_parititioner)},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("cql-proxy")},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("3.0.0")},
    {.type = VALUE_TYPE_SIMPLE, .value = uuid_value("4f2b29e6-59b5-4e2d-8fd6-01e32e67f0d7")},
    {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("4")},
    {.type = VALUE_TYPE_SIMPLE, .value = uuid_value("19e26944-ffb1-40a9-a184-a9b065e5e06b")}},
  }}};

  write_rows(client, stmt, "system", "local", &local_columns, &local_rows);
}

void write_system_peers(client_t *client, const statement_t *stmt) {
  write_rows(client, stmt, "system", "peers", &peers_columns, &empty_rows);
}

void on_session_connected(CassFuture *future, void *data) {
  client_t *client = (client_t*)data;
  CassError rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    const char *message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    int32_t code = rc == CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE ? CQL_ERROR_INVALID_QUERY : CQL_ERROR_SERVER_ERROR;
    write_error(client, client->use_keyspace_stream, code, message, message_length);
    add_to_client_queue(&use_keyspace_failed, client);
  } else {
    write_set_keyspace(client, client->use_keyspace_stream);
    add_to_client_queue(&use_keyspace_success, client);
  }
  cass_future_free(future);
}

void do_use_keyspace(client_t *client, const statement_t *stmt) {
  session_t* s = get_session(stmt->use.keyspace);
  if (client->use_keyspace_stream >= 0) {
    do_error(client, CQL_ERROR_OVERLOADED, "Use keyspace already in progress");
    return;
  }
  strncpy(client->keyspace, stmt->use.keyspace, sizeof(client->keyspace) - 1);
  if (s->is_connected) {
    write_set_keyspace(client, client->frame.stream);
  } else {
    CassFuture *future = cass_session_connect_keyspace(s->session, cluster, stmt->use.keyspace);
    client->use_keyspace_stream = client->frame.stream;
    cass_future_set_callback(future, on_session_connected, client);
  }
}

void do_execute(client_t *client) {
  char id[32] = {0};
  uint16_t len = sizeof(id) - 1;

  { // Decode
    decode_string(client->body, id, &len);
    id[len] = '\0';
  }

  prepared_t * prepared = NULL;
  if (len == PICOHASH_MD5_DIGEST_LENGTH) {
    prepared = find_prepared(id);
  }

  if (prepared != NULL) {
    const statement_t *stmt = &prepared->stmt;
    if (stmt->type == STMT_SELECT) {
        switch (stmt->select.table_type) {
          case TK_LOCAL:
            write_system_local(client, stmt);
            break;
          case TK_PEERS:
            write_system_peers(client, stmt);
            break;
          default:
            assert("Invalid system table" && false);
        }
    } else if (stmt->type == STMT_USE) {
      do_use_keyspace(client, stmt);
    } else {
      assert("Unsupported prepared statement type" && false);
    }
  } else {
    do_request(client, &client->frame, client->body);
  }
}

void do_query(client_t *client) {
  int32_t len = 0;
  const char *query = decode_int32(client->body, &len);

  statement_t stmt = {0};
  if (parse(&stmt, query, (size_t)len)) {
    switch (stmt.type) {
      case STMT_SELECT:
        if (stmt.select.is_table && strcmp(client->keyspace, "system") != 0) {
          do_request(client, &client->frame, client->body);
        } else {
          switch (stmt.select.table_type) {
            case TK_LOCAL:
              write_system_local(client, &stmt);
              break;
            case TK_PEERS:
              write_system_peers(client, &stmt);
              break;
            case TK_PEERS_V2:
              do_error(client, CQL_ERROR_INVALID_QUERY, "Doesn't exist");
              break;
            default:
              do_request(client, &client->frame, client->body);
              break;
          }
        }
        break;
      case STMT_USE:
        do_use_keyspace(client, &stmt);
        break;
    }
  } else {
    do_request(client, &client->frame, client->body);
  }
}

void on_write(uv_write_t *req, int status) {
  batch_t *batch = (batch_t *)req->data;
  if (batch->client->is_closing &&
      uv_stream_get_write_queue_size((uv_stream_t *)&batch->client->tcp) == 0) {
    close_client(batch->client);
  }
  free(batch);
}

void on_frame_header_done(frame_t *frame) {
  client_t *client = (client_t *)frame->user_data;

  client->body = client->body_inline;

  if (frame->version < 3 || frame->version > 4) {
    do_error(client, CQL_ERROR_PROTOCOL_ERROR, "Invalid or unsupported protocol version");
  } else if (frame->length < 0) {
    do_error(client, CQL_ERROR_PROTOCOL_ERROR, "Frame length is invalid");
    close_client(client);
  } else if (frame->length > MAX_FRAME_SIZE) {
    do_error(client, CQL_ERROR_PROTOCOL_ERROR, "Frame body is too big");
    close_client(client);
  } else if (frame->length > (int32_t)sizeof(client->body_inline)) {
    client->body = malloc((size_t)frame->length);
  }

  client->body_pos = client->body;
}

void on_frame_body(frame_t *frame, const char *data, size_t len) {
  client_t *client = (client_t *)frame->user_data;
  memcpy(client->body_pos, data, len);
  client->body_pos += len;
}

void on_frame_done(frame_t *frame) {
  client_t *client = (client_t *)frame->user_data;

  switch (frame->opcode) {
  case CQL_OPCODE_OPTIONS:
    do_options(client);
    break;
  case CQL_OPCODE_STARTUP: // fall through
  case CQL_OPCODE_REGISTER:
    do_startup_or_register(client);
    break;
  case CQL_OPCODE_PREPARE:
    do_prepare(client);
    break;
  case CQL_OPCODE_EXECUTE:
    do_execute(client);
    break;
  case CQL_OPCODE_QUERY:
    do_query(client);
    break;
  default:
    do_error(client, CQL_ERROR_PROTOCOL_ERROR, "Unsupported operation");
    break;
  }

  if (client->body != client->body_inline) {
    free(client->body);
  }
}


void on_response_body_free(response_t *response) {
  free(response->body);
}

void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

void process_queued(client_t *client) {
  uv_mutex_lock(&client->mutex);
  client->use_keyspace_stream = -1;
  for (size_t i = 0; i < client->queued_count; ++i) {
    queued_t *queued = &client->queued[i];
    do_request(client, &queued->frame, queued->body);
    free(queued->body);
  }
  uv_read_start((uv_stream_t *)&client->tcp, on_alloc, on_read);
  client->queued_count = 0;
  uv_mutex_unlock(&client->mutex);
}

void set_keyspace(client_t *client) {
  session_t *session = find_session(client->keyspace);
  assert(session != NULL && "No session for keyspace");
  session->is_connected = true;
  process_queued(client);
}

void set_keyspace_failed(client_t *client) {
  client->keyspace[0] = '\0';
  process_queued(client);
}

// Thread-safe
void flush_client(client_t *client) {
  batch_t *copy[MAX_BATCH];
  size_t count = 0;

  uv_mutex_lock(&client->mutex);
  count = client->batch_count;
  for (size_t i = 0;  i < count; ++i) {
    copy[i] = client->batches[i];
  }
  client->batch = NULL;
  client->batch_count = 0;
  uv_mutex_unlock(&client->mutex);


  for (size_t i = 0; i < count; ++i) {
    batch_t *batch = copy[i];
    uv_write(&batch->req, (uv_stream_t *)&client->tcp,
             batch->bufs, batch->count, on_write);
  }
}

response_t *get_batch_reponse(client_t *client) {
  if (client->batch == NULL || client->batch->count + 2 >= MAX_BATCH_SIZE) {
    if (client->batch_count == MAX_BATCH - 2) {
      abort(); // TODO
    }
    client->batch = alloc_batch(client);
    client->batches[client->batch_count++] = client->batch;
  }
  return &client->batch->reqs[client->batch->count];
}


void add_response_to_batch(client_t *client, response_t *response) {
  client->batch->bufs[client->batch->count++] =
      uv_buf_init(response->header, sizeof(response->header));
  client->batch->bufs[client->batch->count++] =
      uv_buf_init(response->body, (unsigned int)response->len);
}

// Thread-safe
void write_response_body(client_t *client, int8_t opcode, int16_t stream, const char *body, size_t body_size) {
  uv_mutex_lock(&client->mutex);
  if (client->is_closing) {
    uv_mutex_unlock(&client->mutex);
    return;
  }
  response_t *response = get_batch_reponse(client);

  char *pos = encode_header(response->header, stream, opcode);
  encode_int32(pos, (int32_t)body_size); // Length
  response->body = malloc(body_size);
  response->len = body_size;
  memcpy(response->body, body, body_size);
  response->free_cb = on_response_body_free;

  add_response_to_batch(client, response);
  uv_mutex_unlock(&client->mutex);

  add_to_client_queue(&to_flush, client);
}

void on_response_result_free(response_t *response) {
  cass_raw_result_free((CassRawResult*)response->data);
}

// Thread-safe
void write_response_result(client_t *client, int16_t stream, const CassRawResult *result) {
  uv_mutex_lock(&client->mutex);
  if (client->is_closing) {
    uv_mutex_unlock(&client->mutex);
    return;
  }
  response_t *response = get_batch_reponse(client);

  char *pos = encode_header(response->header, stream, (int8_t)cass_raw_result_opcode(result));
  // TODO: Handle custom payloads, warnings, tracing
  size_t length = cass_raw_result_frame_length(result);
  encode_int32(pos, (int32_t)length); // Length
  response->body = (char*)cass_raw_result_frame(result);
  response->len = length;
  response->data = (char *)result;

  add_response_to_batch(client, response);
  uv_mutex_unlock(&client->mutex);

  add_to_client_queue(&to_flush, client);
}

void on_close(uv_handle_t *handle) {
  client_t *client = (client_t *)handle->data;
  // TODO: Clean up in-flight?
  free(client);
}

void close_client(client_t *client) {
  if (client->batch_count > 0) {
    client->is_closing = true;
  } else {
    uv_close((uv_handle_t *)&client->tcp, on_close);
  }
}

void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  client_t *client = (client_t *)stream->data;

  if (nread > 0) {
    decode_frames(&client->frame, buf->base, (size_t)nread);
  } else {
    close_client(client);
  }
}

void on_connection(uv_stream_t *server, int status) {
  int rc;
  assert(status == 0 && "Unable to handle client connection");

  client_t *client = (client_t *)malloc(sizeof(client_t));

  rc = uv_tcp_init(server->loop, &client->tcp);
  assert(rc == 0 && "Unable to initialize client tcp");

  rc = uv_accept(server, (uv_stream_t *)&client->tcp);
  assert(rc == 0 && "Unable to accept client connection");

  client->batch = NULL;
  client->batch_count = 0;
  client->tcp.data = client;
  client->frame.user_data = client;
  client->is_closing = false;
  client->keyspace[0] = '\0';
  client->queued_count = 0;
  client->use_keyspace_stream = -1;
  uv_mutex_init(&client->mutex);
  frame_init_ex(&client->frame, on_frame_header_done, on_frame_body,
                on_frame_done);
  uv_read_start((uv_stream_t *)&client->tcp, on_alloc, on_read);
}

const char *copy_str_value(const CassValue *value) {
  if (!value) {
    return NULL;
  }

  const char *str = NULL;
  size_t str_length = 0;
  if (cass_value_get_string(value, &str, &str_length) != CASS_OK) {
    return NULL;
  }

  char *copy = malloc(str_length + 1);
  memcpy(copy, str, str_length);
  copy[str_length] = '\0';
  return copy;
}

void print_error(const char *context, CassFuture *future) {
  const char *message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "%s: '%.*s'\n", context, (int)message_length, message);
}

bool connect_session(const char *bundle, const char *username, const char *password) {
  CassFuture *connect_future = NULL;
  cluster = cass_cluster_new();
  session = cass_session_new();

  cass_cluster_set_load_balance_round_robin(cluster);
  cass_cluster_set_token_aware_routing(cluster, cass_false);

  /* Setup driver to connect to the cloud using the secure connection bundle */
  if (cass_cluster_set_cloud_secure_connection_bundle(cluster, bundle) != CASS_OK) {
    fprintf(stderr, "Unable to configure the secure connection bundle: %s\n",
            bundle);
    goto error;
  }

  cass_cluster_set_credentials(cluster, username, password);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  CassError rc = cass_future_error_code(connect_future);
  if (rc != CASS_OK) {
    print_error("Unable to connect session", connect_future);
    goto error;
  }


  const char *query = "SELECT release_version,partitioner FROM system.local";
  CassStatement *statement = cass_statement_new(query, 0);

  CassFuture *result_future = cass_session_execute(session, statement);

  cass_statement_free(statement);

  if (cass_future_error_code(result_future) != CASS_OK) {
    print_error("Unable to run query", result_future);
    cass_future_free(result_future);
    goto error;
  }

  /* Retrieve result set and get the first row */
  const CassResult *result = cass_future_get_result(result_future);
  const CassRow *row = cass_result_first_row(result);

  if (row) {
    cassandra_version = copy_str_value(cass_row_get_column_by_name(row, "release_version"));
    cassandra_parititioner = copy_str_value(cass_row_get_column_by_name(row, "partitioner"));
  }

  cass_future_free(result_future);
  cass_future_free(connect_future);
  return true;
error:
  cass_future_free(connect_future);
  cass_session_free(session);
  cass_cluster_free(cluster);
  return false;
}

void on_async(uv_async_t *handle) {
  // nop
}

void on_prepare(uv_prepare_t *handle) {
  process_client_queue(&to_flush, flush_client);
  process_client_queue(&use_keyspace_failed, set_keyspace_failed);
  process_client_queue(&use_keyspace_success, set_keyspace);
}

void help(const char *prog) {
  fprintf(stderr, "%s --bundle|-b --username|-u --password|-p [--bind|-n <ip>] [--port|-t <port>]\n", prog);
  exit(1);
}

int main(int argc, char **argv) {
  struct sockaddr_in addr;
  uv_loop_t loop;
  uv_tcp_t tcp;
  int rc;

  const char *ip = "127.0.0.1";
  int port = 9042;
  const char *bundle = NULL;
  const char *username = NULL;
  const char *password = NULL;

  cass_log_set_level(CASS_LOG_ERROR);

  for (int i = 0; i < argc; ++i) {
    char *arg = argv[i];
    if (strcmp("--bind", arg) == 0 || strcmp("-n", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      ip = argv[++i];
    } else if (strcmp("--port", arg) == 0 || strcmp("-t", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      char *arg = argv[++i];
      port = atoi(arg);
      if (port == 0) {
        fprintf(stderr, "Port is invalid: %s\n", arg);
        exit(1);
      }
    } else if (strcmp("--bundle", arg) == 0 || strcmp("-b", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      bundle = argv[++i];
    } else if (strcmp("--username", arg) == 0 || strcmp("-u", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      username = argv[++i];
    } else if (strcmp("--password", arg) == 0 || strcmp("-p", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      password = argv[++i];
    }
  }

  if (!bundle || !username || !password) {
    help(argv[0]);
  }

  if (!connect_session(bundle, username, password)) {
    exit(1);
  }

  if (!cassandra_version || !cassandra_parititioner) {
    cass_session_free(session);
    cass_cluster_free(cluster);
    fprintf(stderr, "Unable to determine Cassandra version or partitioner");
    exit(1);
  }

  init_client_queue(&to_flush);
  init_client_queue(&use_keyspace_success);
  init_client_queue(&use_keyspace_failed);

  print("Listening on %s:%d (version: %s, partitioner: %s)\n",
        ip, port, cassandra_version, cassandra_parititioner);

  uv_ip4_addr(ip, port, &addr);

  rc = uv_loop_init(&loop);
  assert(rc == 0 && "Unable to initialize loop");

  rc = uv_async_init(&loop, &async, on_async);
  assert(rc == 0 && "Unable to initialize async");

  rc = uv_prepare_init(&loop, &prepare);
  assert(rc == 0 && "Unable to initialize prepare");

  rc = uv_prepare_start(&prepare, on_prepare);
  assert(rc == 0 && "Unable to start prepare");

  rc = uv_tcp_init(&loop, &tcp);
  assert(rc == 0 && "Unable to initialize tcp");

  rc = uv_tcp_bind(&tcp, (struct sockaddr *)&addr, 0);
  assert(rc == 0 && "Unable to bind tcp");

  rc = uv_listen((uv_stream_t *)&tcp, 100, on_connection);
  assert(rc == 0 && "Unable to listen");

  uv_run(&loop, UV_RUN_DEFAULT);

  uv_loop_close(&loop);
}
