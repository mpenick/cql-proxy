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
#ifndef PARSE_H
#define PARSE_H

#include <stdbool.h>
#include <stddef.h>

#include "lex.h"

typedef struct statement_s statement_t;

typedef enum {
  STMT_SELECT,
  STMT_USE,
} statement_type_t;

struct statement_s {
  statement_type_t type;
  union {
    token_t table_type;
    char keyspace[128]; // TODO: How big can this be?
  };
};

static bool parse_select(statement_t *stmt, lex_t* lex) {
  token_t token;
  while ((token = lex_next_token(lex)) != TK_FROM) {
    // TODO: Support "column1, column2" and "*" and "COUNT(*)"
    // Copy expressions
  }

  if (token != TK_FROM) {
    return false;
  }

  token = lex_next_token(lex);
  if (token != TK_SYSTEM) {
    return false;
  }

  token = lex_next_token(lex);
  if (token != TK_DOT) {
    return false;
  }

  token = lex_next_token(lex);
  if (token != TK_LOCAL && token != TK_PEERS && token != TK_PEERS_V2) {
    return false;
  }

  stmt->table_type = token;
  stmt->type = STMT_SELECT;
  return true;
}

static bool parse_use(statement_t *stmt, lex_t* lex) {
  token_t token = lex_next_token(lex);
  if (token != TK_SYSTEM && token != TK_ID) {
    return false;
  }

  strncpy(stmt->keyspace, lex->val, sizeof(stmt->keyspace) - 1);
  stmt->type = STMT_USE;
  return true;
}

static bool parse(statement_t *stmt, const char* query, size_t len) {
  lex_t lex;
  lex_init(&lex, query, len);

  switch(lex_next_token(&lex)) {
    case TK_SELECT:
      return parse_select(stmt, &lex);
    case TK_USE:
      return parse_use(stmt, &lex);
    default:
      return false;
  }
}

#endif // PARSE_H
