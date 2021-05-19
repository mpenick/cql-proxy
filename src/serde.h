#ifndef SERDE_H
#define SERDE_H

#include <assert.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define FRAME_HEADER_SIZE (1 + 1 + 2 + 1 + 4)

char *encode_int8(char *pos, int8_t value) {
  pos[0] = (char)value;
  return pos + 1;
}

char *encode_int16(char *pos, int16_t value) {
  pos[0] = (char)((value >> 8) & 0xFF);
  pos[1] = (char)(value & 0xFF);
  return pos + 2;
}

char *encode_uint16(char *pos, uint16_t value) {
  pos[0] = (char)((value >> 8) & 0xFF);
  pos[1] = (char)(value & 0xFF);
  return pos + 2;
}

char *encode_int32(char *pos, int32_t value) {
  pos[0] = (char)((value >> 24) & 0xFF);
  pos[1] = (char)((value >> 16) & 0xFF);
  pos[2] = (char)((value >> 8) & 0xFF);
  pos[3] = (char)(value & 0xFF);
  return pos + 4;
}

char *encode_string(char *pos, const char *str, size_t len) {
  pos = encode_int16(pos, (int16_t)len);
  memcpy(pos, str, len);
  return pos + len;
}

char *encode_const_string(char *pos, const char *str) {
  size_t len = strlen(str);
  pos = encode_int16(pos, (int16_t)len);
  memcpy(pos, str, len);
  return pos + len;
}

char *encode_long_string(char *pos, const char *str, size_t len) {
  pos = encode_int32(pos, (int32_t)len);
  memcpy(pos, str, len);
  return pos + len;
}

char *encode_frame(char *pos, int8_t version, int8_t flags, int16_t stream,
                   int8_t opcode, int32_t len) {
  pos = encode_int8(pos, version);
  pos = encode_int8(pos, flags);
  pos = encode_int16(pos, stream);
  pos = encode_int8(pos, opcode);
  pos = encode_int32(pos, len);
  return pos;
}

const char *decode_int8(const char *pos, int8_t *value) {
  *value = *pos;
  return pos + 1;
}

const char *decode_int16(const char *pos, int16_t *value) {
  *value = (int16_t)((uint8_t)pos[0] << 8) | (int16_t)((uint8_t)pos[1] << 0);
  return pos + 2;
}

const char *decode_uint16(const char *pos, uint16_t *value) {
  *value = (uint16_t)((uint8_t)pos[0] << 8) | (uint16_t)((uint8_t)pos[1] << 0);
  return pos + 2;
}

const char *decode_int32(const char *pos, int32_t *value) {
  *value = (int32_t)((uint8_t)pos[0] << 24) | (int32_t)((uint8_t)pos[1] << 16) |
           (int32_t)((uint8_t)pos[2] << 8) | (int32_t)((uint8_t)pos[3] << 0);
  return pos + 4;
}

const char *decode_string(const char *pos, char *str, uint16_t *len) {
  uint16_t max = *len;
  pos = decode_uint16(pos, len);
  memcpy(str, pos, max);
  return pos + *len;
}

const char *decode_long_string(const char *pos, char *str, int32_t *len) {
  int32_t max = *len;
  assert(max > 0);
  pos = decode_int32(pos, len);
  memcpy(str, pos, (size_t)max);
  return pos + *len;
}

typedef struct frame_s frame_t;

typedef enum { VERSION, FLAGS, STREAM, OPCODE, LENGTH, BODY } state_t;

typedef void (*frame_header_done_cb)(frame_t *frame);
typedef void (*frame_body_cb)(frame_t *frame, const char *data, size_t len);
typedef void (*frame_body_done_cb)(frame_t *frame);

struct frame_s {
  state_t state_;
  size_t remaining_;
  int8_t version;
  int8_t flags;
  int16_t stream;
  int8_t opcode;
  int32_t length;
  frame_header_done_cb header_cb;
  frame_body_cb body_cb;
  frame_body_done_cb done_cb;
  void *user_data;
};

void frame_nop(frame_t *frame) {}
void frame_nop_body(frame_t *frame, const char *data, size_t len) {}

void frame_init_ex(frame_t *frame, frame_header_done_cb header_cb,
                   frame_body_cb body_cb, frame_body_done_cb done_cb);

void frame_init(frame_t *frame, frame_body_done_cb done_cb) {
  frame_init_ex(frame, NULL, NULL, done_cb);
}

void frame_init_ex(frame_t *frame, frame_header_done_cb header_cb,
                   frame_body_cb body_cb, frame_body_done_cb done_cb) {
  frame->state_ = VERSION;
  frame->header_cb = header_cb ? header_cb : frame_nop;
  frame->body_cb = body_cb ? body_cb : frame_nop_body;
  frame->done_cb = done_cb ? done_cb : frame_nop;
}

void decode_frames(frame_t *frame, const char *data, size_t len) {
  const char *pos = data;
  const char *end = data + len;

  while (pos != end) {
    size_t nbytes;
    switch (frame->state_) {
    case VERSION:
      frame->version = *pos;
      frame->state_ = FLAGS;
      pos++;
      break;
    case FLAGS:
      frame->flags = *pos;
      frame->stream = 0;
      frame->remaining_ = 2;
      frame->state_ = STREAM;
      pos++;
      break;
    case STREAM:
      while (pos != end && frame->remaining_ > 0) {
        frame->stream |=
            (uint16_t)((uint8_t)*pos << (8 * (frame->remaining_ - 1)));
        frame->remaining_--;
        pos++;
      }
      if (frame->remaining_ == 0) {
        assert(frame->stream >= 0 &&
               "Stream ID should be greater than or equal to 0");
        frame->state_ = OPCODE;
      }
      break;
    case OPCODE:
      frame->opcode = *pos;
      frame->length = 0;
      frame->remaining_ = 4;
      frame->state_ = LENGTH;
      pos++;
      break;
    case LENGTH:
      while (pos != end && frame->remaining_ > 0) {
        frame->length |=
            (uint32_t)((uint8_t)*pos << (8 * (frame->remaining_ - 1)));
        frame->remaining_--;
        pos++;
      }
      if (frame->remaining_ == 0) {
        assert(frame->length >= 0 &&
               "Body length should be greater than or equal to 0");
        if (frame->length == 0) {
          frame->header_cb(frame);
          frame->done_cb(frame);
          frame->state_ = VERSION;
        } else {
          frame->remaining_ = (size_t)frame->length;
          frame->header_cb(frame);
          frame->state_ = BODY;
        }
      }
      break;
    case BODY:
      nbytes = (size_t)(end - pos);
      if (nbytes > (size_t)frame->remaining_) {
        nbytes = (size_t)frame->remaining_;
      }
      frame->body_cb(frame, pos, nbytes);
      frame->remaining_ -= nbytes;
      if (frame->remaining_ == 0) {
        frame->done_cb(frame);
        frame->state_ = VERSION;
      }
      pos += nbytes;
      break;
    }
  }
}

#endif // SERDE_H
