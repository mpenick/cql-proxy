#include <uv.h>

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

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

#define SELECT_LOCAL "SELECT * FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT * FROM system.peers"

#define MAX_CLIENTS 128

#define MAX_BATCH 64
#define MAX_BATCH_SIZE 1024


typedef struct column_type_s column_type_t;
typedef struct value_s value_t;

typedef struct client_s client_t;
typedef struct request_s request_t;
typedef struct response_s response_t;
typedef struct batch_s batch_t;

static CassSession *session;
static uv_async_t async;
static uv_prepare_t prepare;
static const char *cassandra_version = NULL;
static const char *cassandra_parititioner = NULL;

static client_t *to_flush[MAX_CLIENTS];
static size_t to_flush_count = 0;
static uv_mutex_t to_flush_mutex;

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
      pos = encode_long_string(pos, column->value.data, column->value.len);
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

typedef struct {
  int32_t count;
  uint16_t *pk_indexes;
} primary_keys_t;

static columns_t local_columns = {
    7,
    (column_t[]){{.name = "key", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "data_center", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rack", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "release_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rpc_address", {.basic = CQL_TYPE_INET}},
                 {.name = "partitioner", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "tokens",
                  {.basic = CQL_TYPE_SET, .sub_types = {CQL_TYPE_VARCHAR}}}}};

static columns_t peers_columns = {
    6,
    (column_t[]){{.name = "peer", {.basic = CQL_TYPE_INET}},
                 {.name = "data_center", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rack", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "release_version", {.basic = CQL_TYPE_VARCHAR}},
                 {.name = "rpc_address", {.basic = CQL_TYPE_INET}},
                 {.name = "tokens",
                  {.basic = CQL_TYPE_SET, .sub_types = {CQL_TYPE_VARCHAR}}}}};

static rows_t empty_rows = {0};

struct client_s {
  uv_tcp_t tcp;
  char data[64 * 1024];
  char body[8192];
  char *body_pos;
  frame_t frame;
  batch_t *batch;
  batch_t *batches[MAX_BATCH];
  size_t batch_count;
  batch_t *free_batch;
  uv_mutex_t mutex;
};

struct request_s {
  client_t *client;
  int16_t stream;
};

typedef void (*response_free_cb)(response_t* response);

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
  if (client->free_batch) {
    batch = client->free_batch;
    client->free_batch = client->free_batch->next;
  } else {
    batch = (batch_t *)malloc(sizeof(batch_t));
    batch->req.data = batch;
    batch->client = client;
  }
  batch->count = 0;
  return batch;
}

void write_response_body(client_t *client, int8_t opcode, int16_t stream, const char *body, size_t body_size);
void write_response_result(client_t *client, int16_t stream, const CassRawResult *result);

void on_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  client_t *client = (client_t *)handle->data;
  buf->base = client->data;
  buf->len = sizeof(client->data);
}

void do_error(client_t *client, int32_t code, const char *message) {
  char body[128];
  size_t len = strlen(message);
  if (len > sizeof(body) - 4 - 2) {
    len = sizeof(body) - 4 - 2;
  }
  char *pos = encode_int32(pos, code);
  encode_string(pos, message, len);
  write_response_body(client, CQL_OPCODE_ERROR, client->frame.stream, body, 4 + 2 + len);
}

void do_options(client_t *client) {
  char body[2];
  char *pos = body;
  pos = encode_uint16(pos, 0);
  write_response_body(client, CQL_OPCODE_SUPPORTED, client->frame.stream, body, 2);
}

void do_startup_or_register(client_t *client) {
  char body[1];
  write_response_body(client, CQL_OPCODE_SUPPORTED, client->frame.stream, body, 0);
}

void on_result(CassFuture *future, void *data) {
  const CassRawResult *result = cass_future_get_raw_result(future);
  if (result == NULL) {
    abort(); // TODO
  } else {
    request_t *request = (request_t*)data;
    write_response_result(request->client, request->stream, result);
    free(request);
  }
  cass_future_free(future);
}


void do_request(client_t *client) {
  CassFuture *future =  cass_session_execute_raw(session,
                                                 (cass_uint8_t)client->frame.opcode, (cass_uint8_t)client->frame.flags,
                                                 client->body, (size_t)client->frame.length);
  request_t *request = malloc(sizeof(request_t));
  request->client = client;
  request->stream = client->frame.stream;
  cass_future_set_callback(future, on_result, request);
}

void do_query(client_t *client) {
  char query[512];
  int32_t len = sizeof(query);

  decode_long_string(client->body, query, &len);

  if (strstr(query, SELECT_LOCAL) != NULL) {
    rows_t local_rows = (rows_t){
        1,
        (row_t[]){{
            (value_t[]){
                {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("local")},
                {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("dc1")},
                {.type = VALUE_TYPE_SIMPLE, .value = varchar_value("rack1")},
                {.type = VALUE_TYPE_SIMPLE,
                 .value = varchar_value(cassandra_version)},
                {.type = VALUE_TYPE_SIMPLE, .value = inet_value("127.0.0.1")},
                {.type = VALUE_TYPE_SIMPLE,
                 .value = varchar_value(cassandra_parititioner)},
                {.type = VALUE_TYPE_COLL,
                 .coll = {1, (bytes_t[]){varchar_value("0")}}}},

        }}};
    char body[512];
    char *pos = encode_rows(body, false, "system", "local", &local_columns, &local_rows);
    write_response_body(client, CQL_OPCODE_RESULT, client->frame.stream, body, (size_t)(pos - body));
  } else if (strstr(query, SELECT_PEERS) != NULL) {
    char body[512];
    char *pos = encode_rows(pos, false, "system", "peers", &peers_columns, &empty_rows);
    write_response_body(client, CQL_OPCODE_RESULT, client->frame.stream, body, (size_t)(pos - body));
  } else {
    do_request(client);
  }
}

void on_write(uv_write_t *req, int status) {
  batch_t *batch = (batch_t *)req->data;
  batch_t *prev = batch->client->free_batch;
  batch->client->free_batch = batch;
  batch->next = prev;
}

void on_frame_header_done(frame_t *frame) {
  client_t *client = (client_t *)frame->user_data;
  if (frame->length > (int32_t)sizeof(client->body)) {
    abort();
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

  if (!client->batch) {
    client->batch = alloc_batch(client);
  }

  switch (frame->opcode) {
  case CQL_OPCODE_OPTIONS:
    do_options(client);
    break;
  case CQL_OPCODE_STARTUP: // fall through
  case CQL_OPCODE_REGISTER:
    do_startup_or_register(client);
    break;
  case CQL_OPCODE_PREPARE:
  case CQL_OPCODE_EXECUTE: // fall through
    do_request(client);
    break;
  case CQL_OPCODE_QUERY:
    do_query(client);
    break;
  default:
    do_error(client, CQL_ERROR_PROTOCOL_ERROR, "Unsupported operation");
    break;
  }
}


void on_response_body_free(response_t *response) {
  free(response->body);
}

// Thread-safe
void add_to_flush(client_t *client) {
  uv_mutex_lock(&to_flush_mutex);
  for (size_t i = 0; i < to_flush_count; ++i) {
    if (to_flush[i] == client) {
      uv_mutex_unlock(&to_flush_mutex);
      return;
    }
  }
  to_flush[to_flush_count] = client;
  uv_mutex_unlock(&to_flush_mutex);
  uv_async_send(&async);
}

// Thread-safe
void flush_client(client_t *client) {
  uv_mutex_lock(&client->mutex);
  client->batches[client->batch_count++] = client->batch;
  for (size_t i = 0; i < client->batch_count; ++i) {
    batch_t *batch = client->batches[i];
    uv_write(&batch->req, (uv_stream_t *)&client->tcp,
             batch->bufs, batch->count, on_write);
  }
  client->batch = NULL;
  client->batch_count = 0;
  uv_mutex_unlock(&client->mutex);
}

// Thread-safe
void flush() {
  uv_mutex_lock(&to_flush_mutex);
  for (size_t i = 0; i < to_flush_count; ++i) {
    flush_client(to_flush[i]);
  }
  to_flush_count = 0;
  uv_mutex_unlock(&to_flush_mutex);
}

response_t *get_batch_reponse(client_t *client) {
  if (client->batch == NULL || client->batch->count + 2 >= MAX_BATCH_SIZE) {
    if (client->batch_count == MAX_BATCH - 2) {
      abort(); // TODO
    }
    client->batches[client->batch_count++] = client->batch;
    client->batch = alloc_batch(client);
  }
  return &client->batch->reqs[client->batch->count];
}


void add_response_to_batch(client_t *client, response_t *response) {
  client->batch->bufs[client->batch->count] =
      uv_buf_init(response->header, sizeof(response->header));
  client->batch->count++;
  client->batch->bufs[client->batch->count] =
      uv_buf_init(response->body, (unsigned int)response->len);
  client->batch->count++;

}

// Thread-safe
void write_response_body(client_t *client, int8_t opcode, int16_t stream, const char *body, size_t body_size) {
  uv_mutex_lock(&client->mutex);
  response_t *response = get_batch_reponse(client);
  char *pos = encode_header(response->header, stream, opcode);
  encode_int32(pos, (int32_t)body_size); // Length
  response->body = malloc(body_size);
  memcpy(response->body, body, body_size);
  response->free_cb = on_response_body_free;
  add_response_to_batch(client, response);
  uv_mutex_unlock(&client->mutex);

  add_to_flush(client);
}

void on_response_result_free(response_t *response) {
  cass_raw_result_free((CassRawResult*)response->data);
}

// Thread-safe
void write_response_result(client_t *client, int16_t stream, const CassRawResult *result) {
  uv_mutex_lock(&client->mutex);
  response_t *response = get_batch_reponse(client);
  char *pos = encode_header(response->header, stream, (int8_t)cass_raw_result_opcode(result));
  // TODO: Handle custom payloads, warnings, tracing
  encode_int32(pos, (int32_t)cass_raw_result_frame_length(result)); // Length
  response->body = (char*)cass_raw_result_frame(result);
  response->data = result;
  add_response_to_batch(client, response);
  uv_mutex_unlock(&client->mutex);

  add_to_flush(client);
}

void on_close(uv_handle_t *handle) {
  printf("Client closed\n");
  client_t *client = (client_t *)handle->data;
  batch_t *batch = client->free_batch;
  while (batch) {
    batch_t *next = batch->next;
    free(batch);
    batch = next;
  }
  free(client);
}

void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  client_t *client = (client_t *)stream->data;

  if (nread > 0) {
    client->batch = NULL;

    decode_frames(&client->frame, buf->base, (size_t)nread);

    if (client->batch) {
      uv_write(&client->batch->req, stream, client->batch->bufs,
               (unsigned int)client->batch->count, on_write);
      client->batch = NULL;
    }
  } else {
    uv_close((uv_handle_t *)&client->tcp, on_close);
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

  printf("Client connection\n");

  client->batch = NULL;
  client->batch_count = 0;

  client->free_batch = NULL;
  client->tcp.data = client;
  client->frame.user_data = client;
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
  strncpy(copy, str, str_length);
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
  CassCluster *cluster;

  cluster = cass_cluster_new();
  session = cass_session_new();

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
  cass_cluster_free(cluster);
  return true;
error:
  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
  return false;
}

void on_async(uv_async_t *handle) {
  // nop
}

void on_prepare(uv_prepare_t *handle) {
  flush();
}

void help(const char *prog) {
  fprintf(stderr, "%s --bundle|-b --username|-u --password|p [--bind|-b <ip>] [--port|-p <port>]\n", prog);
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

  for (int i = 0; i < argc; ++i) {
    char *arg = argv[i];
    if (strcmp("--bind", arg) == 0 || strcmp("-b", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      ip = argv[++i];
    } else if (strcmp("--port", arg) == 0 || strcmp("-p", arg) == 0) {
      if (i + 1 > argc) {
        help(argv[0]);
      }
      char* arg = argv[++i];
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
    fprintf(stderr, "Unable to determine cassandra version or parititioner");
    exit(1);
  }

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
