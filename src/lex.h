
#line 1 "lex.rh"
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
#ifndef LEX_RH
#define LEX_RH

#include <string.h>
#include <stdio.h>

typedef enum {
  TK_INVALID,
  TK_TOO_BIG,
  TK_SELECT,
  TK_STAR,
  TK_DOT,
  TK_COMMA,
  TK_FROM,
  TK_SYSTEM,
  TK_LOCAL,
  TK_PEERS,
  TK_PEERS_V2,
  TK_USE,
  TK_ID,
  TK_EOF
} token_t;

typedef struct lex_s lex_t;

struct lex_s {
  const char* buf;
  size_t buf_size;
  const char* p;
  const char* mark;
  int line;
  char val[128];
};

#define lex_mark(lex) lex->mark = lex->p
#define lex_rewind(lex) lex->p = lex->mark


#line 57 "lex.h"
static const char _lex_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	5, 1, 6, 1, 7, 1, 8, 1, 
	9, 1, 10, 1, 11, 1, 12, 1, 
	13, 1, 14, 1, 15, 1, 16, 1, 
	17, 1, 18, 1, 19, 1, 20, 1, 
	21, 1, 22, 2, 2, 3, 2, 2, 
	4
};

static const char _lex_key_offsets[] = {
	0, 4, 8, 9, 10, 11, 12, 13, 
	14, 15, 16, 17, 18, 19, 20, 21, 
	22, 23, 24, 25, 26, 27, 29, 30, 
	31, 32, 33, 34, 35, 36, 37, 38, 
	39, 40, 41, 42, 43, 44, 45, 46, 
	47, 48, 49, 50, 51, 52, 53, 54, 
	55, 56, 57, 58, 59, 60, 61, 62, 
	63, 64, 65, 66, 67, 68, 69, 70, 
	71, 72, 73, 74, 75, 76, 77, 78, 
	79, 80, 81, 82, 83, 84, 85, 86, 
	87, 88, 102, 103, 107, 110, 113
};

static const char _lex_trans_keys[] = {
	10, 13, 34, 92, 10, 13, 34, 92, 
	111, 99, 97, 108, 41, 124, 40, 34, 
	108, 111, 99, 97, 108, 34, 41, 101, 
	101, 114, 115, 41, 95, 124, 40, 34, 
	112, 101, 101, 114, 115, 34, 41, 118, 
	50, 41, 124, 40, 34, 112, 101, 101, 
	114, 115, 95, 118, 50, 34, 41, 121, 
	115, 116, 101, 109, 41, 124, 40, 34, 
	115, 121, 115, 116, 101, 109, 34, 41, 
	114, 111, 109, 47, 105, 101, 108, 101, 
	99, 116, 47, 105, 115, 101, 47, 105, 
	9, 10, 13, 32, 34, 40, 42, 44, 
	46, 47, 65, 90, 97, 122, 10, 10, 
	13, 34, 92, 108, 112, 115, 102, 115, 
	117, 95, 48, 57, 65, 90, 97, 122, 
	0
};

static const char _lex_single_lengths[] = {
	4, 4, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 2, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 10, 1, 4, 3, 3, 1
};

static const char _lex_range_lengths[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 2, 0, 0, 0, 0, 3
};

static const short _lex_index_offsets[] = {
	0, 5, 10, 12, 14, 16, 18, 20, 
	22, 24, 26, 28, 30, 32, 34, 36, 
	38, 40, 42, 44, 46, 48, 51, 53, 
	55, 57, 59, 61, 63, 65, 67, 69, 
	71, 73, 75, 77, 79, 81, 83, 85, 
	87, 89, 91, 93, 95, 97, 99, 101, 
	103, 105, 107, 109, 111, 113, 115, 117, 
	119, 121, 123, 125, 127, 129, 131, 133, 
	135, 137, 139, 141, 143, 145, 147, 149, 
	151, 153, 155, 157, 159, 161, 163, 165, 
	167, 169, 182, 184, 189, 193, 197
};

static const char _lex_trans_targs[] = {
	81, 81, 81, 1, 0, 81, 81, 83, 
	1, 0, 3, 81, 4, 81, 5, 81, 
	6, 81, 7, 81, 8, 81, 9, 81, 
	10, 81, 11, 81, 12, 81, 13, 81, 
	14, 81, 15, 81, 16, 81, 81, 81, 
	18, 81, 19, 81, 20, 81, 21, 81, 
	22, 32, 81, 23, 81, 24, 81, 25, 
	81, 26, 81, 27, 81, 28, 81, 29, 
	81, 30, 81, 31, 81, 81, 81, 33, 
	81, 34, 81, 35, 81, 36, 81, 37, 
	81, 38, 81, 39, 81, 40, 81, 41, 
	81, 42, 81, 43, 81, 44, 81, 45, 
	81, 46, 81, 47, 81, 81, 81, 49, 
	81, 50, 81, 51, 81, 52, 81, 53, 
	81, 54, 81, 55, 81, 56, 81, 57, 
	81, 58, 81, 59, 81, 60, 81, 61, 
	81, 62, 81, 63, 81, 64, 81, 81, 
	81, 66, 81, 67, 81, 68, 81, 69, 
	81, 81, 81, 71, 81, 72, 81, 73, 
	81, 74, 81, 75, 81, 76, 81, 81, 
	81, 78, 81, 79, 81, 80, 81, 81, 
	81, 81, 81, 82, 81, 83, 84, 81, 
	81, 81, 85, 86, 86, 81, 81, 81, 
	81, 81, 81, 1, 0, 2, 17, 48, 
	81, 65, 70, 77, 81, 86, 86, 86, 
	86, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	81, 81, 81, 81, 81, 81, 81, 81, 
	0
};

static const char _lex_trans_actions[] = {
	41, 41, 27, 0, 0, 41, 41, 43, 
	0, 0, 0, 39, 0, 39, 0, 39, 
	0, 39, 0, 39, 0, 39, 0, 39, 
	0, 39, 0, 39, 0, 39, 0, 39, 
	0, 39, 0, 39, 0, 39, 15, 39, 
	0, 39, 0, 39, 0, 39, 0, 39, 
	0, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 17, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 19, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 13, 
	39, 0, 39, 0, 39, 0, 39, 0, 
	39, 9, 39, 0, 39, 0, 39, 0, 
	39, 0, 39, 0, 39, 0, 39, 7, 
	39, 0, 39, 0, 39, 0, 39, 11, 
	39, 31, 29, 0, 31, 46, 5, 21, 
	23, 25, 5, 0, 0, 33, 29, 37, 
	41, 41, 27, 0, 0, 0, 0, 0, 
	37, 0, 0, 0, 37, 0, 0, 0, 
	0, 35, 41, 41, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 39, 39, 39, 39, 39, 
	39, 39, 39, 37, 41, 37, 37, 35, 
	0
};

static const char _lex_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 1, 0, 0, 0, 0, 0
};

static const char _lex_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 3, 0, 0, 0, 0, 0
};

static const short _lex_eof_trans[] = {
	285, 285, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 283, 283, 283, 283, 283, 283, 283, 
	283, 0, 287, 285, 287, 287, 288
};

static const int lex_start = 81;
static const int lex_first_final = 81;
static const int lex_error = -1;

static const int lex_en_main = 81;


#line 56 "lex.rh"


static int copy_value(lex_t* lex, const char* ts, const char* te, int token) {
  size_t token_size = te - ts;
  if (token_size >= sizeof(lex->val)) { // need space for '\0'
    return TK_TOO_BIG;
  }
  memcpy(lex->val, ts, token_size);
  lex->val[token_size] = '\0';
  return token;
}

void lex_init(lex_t* lex, const char* str, size_t len) {
  lex->buf = str;
  lex->buf_size = strlen(lex->buf);
  lex->p = lex->buf;
  lex->line = 1;
}

static token_t lex_next_token(lex_t* lex) {
  token_t token = TK_INVALID;

  const char* p = lex->p;
  const char* pe = lex->buf + lex->buf_size;
  const char* ts;
  const char* te;
  const char* eof = pe;
  int cs, act;

  if (p == eof) return TK_EOF;

  lex->val[0] = '\0';

  (void) lex_first_final;
  (void) lex_error;
  (void) lex_en_main;

  
#line 311 "lex.h"
	{
	cs = lex_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 319 "lex.h"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
_resume:
	_acts = _lex_actions + _lex_from_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 1:
#line 1 "NONE"
	{ts = p;}
	break;
#line 338 "lex.h"
		}
	}

	_keys = _lex_trans_keys + _lex_key_offsets[cs];
	_trans = _lex_index_offsets[cs];

	_klen = _lex_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _lex_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
_eof_trans:
	cs = _lex_trans_targs[_trans];

	if ( _lex_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _lex_actions + _lex_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 2:
#line 1 "NONE"
	{te = p+1;}
	break;
	case 3:
#line 109 "lex.rh"
	{act = 11;}
	break;
	case 4:
#line 112 "lex.rh"
	{act = 14;}
	break;
	case 5:
#line 99 "lex.rh"
	{te = p+1;{ token = TK_SELECT; {p++; goto _out; } }}
	break;
	case 6:
#line 100 "lex.rh"
	{te = p+1;{ token = TK_FROM; {p++; goto _out; } }}
	break;
	case 7:
#line 101 "lex.rh"
	{te = p+1;{ token = TK_USE; {p++; goto _out; } }}
	break;
	case 8:
#line 102 "lex.rh"
	{te = p+1;{ token = TK_SYSTEM; {p++; goto _out; } }}
	break;
	case 9:
#line 103 "lex.rh"
	{te = p+1;{ token = TK_LOCAL; {p++; goto _out; } }}
	break;
	case 10:
#line 104 "lex.rh"
	{te = p+1;{ token = TK_PEERS; {p++; goto _out; } }}
	break;
	case 11:
#line 105 "lex.rh"
	{te = p+1;{ token = TK_PEERS_V2; {p++; goto _out; } }}
	break;
	case 12:
#line 106 "lex.rh"
	{te = p+1;{ token = TK_STAR; {p++; goto _out; } }}
	break;
	case 13:
#line 107 "lex.rh"
	{te = p+1;{ token = TK_COMMA; {p++; goto _out; } }}
	break;
	case 14:
#line 108 "lex.rh"
	{te = p+1;{ token = TK_DOT; {p++; goto _out; } }}
	break;
	case 15:
#line 109 "lex.rh"
	{te = p+1;{ token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }}
	break;
	case 16:
#line 110 "lex.rh"
	{te = p+1;{ lex->line++; }}
	break;
	case 17:
#line 111 "lex.rh"
	{te = p+1;{ /* Skip */ }}
	break;
	case 18:
#line 112 "lex.rh"
	{te = p+1;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 19:
#line 109 "lex.rh"
	{te = p;p--;{ token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }}
	break;
	case 20:
#line 112 "lex.rh"
	{te = p;p--;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 21:
#line 112 "lex.rh"
	{{p = ((te))-1;}{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 22:
#line 1 "NONE"
	{	switch( act ) {
	case 11:
	{{p = ((te))-1;} token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }
	break;
	case 14:
	{{p = ((te))-1;} token = TK_INVALID; {p++; goto _out; } }
	break;
	}
	}
	break;
#line 495 "lex.h"
		}
	}

_again:
	_acts = _lex_actions + _lex_to_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 0:
#line 1 "NONE"
	{ts = 0;}
	break;
#line 508 "lex.h"
		}
	}

	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	if ( _lex_eof_trans[cs] > 0 ) {
		_trans = _lex_eof_trans[cs] - 1;
		goto _eof_trans;
	}
	}

	_out: {}
	}

#line 117 "lex.rh"


  token = token == TK_INVALID && p == eof ? TK_EOF : token;
  lex->p = p;

  return token;
}

#endif // LEX_RH
