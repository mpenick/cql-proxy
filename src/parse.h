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

#define MAX_EXPRS 20

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "lex.h"

typedef struct statement_s statement_t;
typedef struct statement_expr_s statement_expr_t;
typedef struct select_statement_s select_statement_t;
typedef struct use_statement_s use_statement_t;

typedef enum {
  STMT_EXPR_ID,
  STMT_EXPR_ALIAS,
  STMT_EXPR_STAR,
  STMT_EXPR_COUNT,
} statement_expr_type_t;

struct statement_expr_s {
  statement_expr_type_t type;
  char id[64];
  char alias[64];
};

typedef enum {
  STMT_SELECT,
  STMT_USE,
} statement_type_t;

struct select_statement_s {
  token_t table_type;
  statement_expr_t exprs[MAX_EXPRS];
  int32_t exprs_count;
};

struct use_statement_s {
  char keyspace[128]; // TODO: How big can this be?
};

struct statement_s {
  statement_type_t type;
  union {
    select_statement_t select;
    use_statement_t use;
  };
};

static statement_expr_t *next_expr(statement_t *stmt) {
  if (stmt->select.exprs_count == MAX_EXPRS) {
    return NULL;
  }

  return &stmt->select.exprs[stmt->select.exprs_count++];
}

static token_t parse_expr(statement_t *stmt, lex_t* lex, token_t token) {
  if (token == TK_ID) {
    statement_expr_t *expr = next_expr(stmt);
    if (expr) {
      strncpy(expr->id, lex->val, sizeof(expr->id) - 1);
    }
    token = lex_next_token(lex);
    if (token == TK_AS) {
      token = lex_next_token(lex);
      if (token != TK_ID) {
        return token;
      }
      statement_expr_t *expr = next_expr(stmt);
      if (expr) {
        expr->type = STMT_EXPR_ALIAS;
        strncpy(expr->alias, lex->val, sizeof(expr->alias) - 1);
      }
      token = lex_next_token(lex);
    } else {
      if (expr) {
        expr->type = STMT_EXPR_ID;
      }
    }
  } else if (token == TK_STAR) {
    statement_expr_t *expr = next_expr(stmt);
    if (expr) {
      expr->type = STMT_EXPR_STAR;
    }
    token = lex_next_token(lex);
  } else if (token == TK_COUNT) {
    token = lex_next_token(lex);
    if (token != TK_LPAREN) {
      return token;
    }
    token = lex_next_token(lex);
    if (token != TK_STAR && token != TK_ID) {
      return token;
    }
    token = lex_next_token(lex);
    if (token != TK_RPAREN) {
      return token;
    }
    statement_expr_t *expr = next_expr(stmt);
    if (expr) {
      expr->type = STMT_EXPR_COUNT;
    }
    token = lex_next_token(lex);
  }
  return token;
}

static bool parse_select(statement_t *stmt, lex_t* lex) {
  lex_mark(lex); // Mark exprs, we only copy the exprs if it's a query to intercept
  token_t token = lex_next_token(lex);
  while (token != TK_FROM && token != TK_EOF) {
    token = lex_next_token(lex);
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

  stmt->select.table_type = token;
  stmt->type = STMT_SELECT;

  // Copy exprs
  lex_rewind(lex); // Rewind to exprs
  token = lex_next_token(lex);
  while (token != TK_FROM && token != TK_EOF) {
    token = parse_expr(stmt, lex, token);
    if (token == TK_COMMA) {
      token = lex_next_token(lex);
    }
  }

  return true;
}

static bool parse_use(statement_t *stmt, lex_t* lex) {
  token_t token = lex_next_token(lex);
  if (token != TK_SYSTEM && token != TK_ID) {
    return false;
  }

  strncpy(stmt->use.keyspace, lex->val, sizeof(stmt->use.keyspace) - 1);
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
