
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
  TK_LPAREN,
  TK_RPAREN,
  TK_COMMA,
  TK_AS,
  TK_COUNT,
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


#line 61 "lex.h"
static const char _lex_actions[] = {
	0, 1, 0, 1, 1, 1, 13, 1, 
	14, 1, 15, 1, 16, 1, 17, 1, 
	18, 1, 19, 1, 20, 1, 21, 1, 
	22, 1, 23, 1, 24, 1, 25, 1, 
	26, 1, 27, 1, 28, 1, 29, 1, 
	30, 2, 2, 3, 2, 2, 4, 2, 
	2, 5, 2, 2, 6, 2, 2, 7, 
	2, 2, 8, 2, 2, 9, 2, 2, 
	10, 2, 2, 11, 2, 2, 12
};

static const short _lex_key_offsets[] = {
	0, 4, 8, 13, 18, 23, 28, 32, 
	37, 42, 47, 52, 57, 62, 67, 71, 
	76, 81, 86, 91, 96, 100, 128, 129, 
	136, 140, 149, 156, 165, 174, 183, 192, 
	201, 210, 219, 228, 237, 246, 255, 264, 
	273, 282, 291, 298, 307, 315, 326, 335, 
	344, 353, 362, 371, 380, 389, 398, 407
};

static const char _lex_trans_keys[] = {
	10, 13, 34, 92, 10, 13, 34, 92, 
	10, 13, 34, 92, 111, 10, 13, 34, 
	92, 99, 10, 13, 34, 92, 97, 10, 
	13, 34, 92, 108, 10, 13, 34, 92, 
	10, 13, 34, 92, 101, 10, 13, 34, 
	92, 101, 10, 13, 34, 92, 114, 10, 
	13, 34, 92, 115, 10, 13, 34, 92, 
	95, 10, 13, 34, 92, 118, 10, 13, 
	34, 50, 92, 10, 13, 34, 92, 10, 
	13, 34, 92, 121, 10, 13, 34, 92, 
	115, 10, 13, 34, 92, 116, 10, 13, 
	34, 92, 101, 10, 13, 34, 92, 109, 
	10, 13, 34, 92, 9, 10, 13, 32, 
	34, 40, 41, 42, 44, 46, 65, 67, 
	70, 76, 80, 83, 85, 97, 99, 102, 
	108, 112, 115, 117, 66, 90, 98, 122, 
	10, 10, 13, 34, 92, 108, 112, 115, 
	10, 13, 34, 92, 83, 95, 115, 48, 
	57, 65, 90, 97, 122, 95, 48, 57, 
	65, 90, 97, 122, 79, 95, 111, 48, 
	57, 65, 90, 97, 122, 85, 95, 117, 
	48, 57, 65, 90, 97, 122, 78, 95, 
	110, 48, 57, 65, 90, 97, 122, 84, 
	95, 116, 48, 57, 65, 90, 97, 122, 
	82, 95, 114, 48, 57, 65, 90, 97, 
	122, 79, 95, 111, 48, 57, 65, 90, 
	97, 122, 77, 95, 109, 48, 57, 65, 
	90, 97, 122, 79, 95, 111, 48, 57, 
	65, 90, 97, 122, 67, 95, 99, 48, 
	57, 65, 90, 97, 122, 65, 95, 97, 
	48, 57, 66, 90, 98, 122, 76, 95, 
	108, 48, 57, 65, 90, 97, 122, 69, 
	95, 101, 48, 57, 65, 90, 97, 122, 
	69, 95, 101, 48, 57, 65, 90, 97, 
	122, 82, 95, 114, 48, 57, 65, 90, 
	97, 122, 83, 95, 115, 48, 57, 65, 
	90, 97, 122, 95, 48, 57, 65, 90, 
	97, 122, 86, 95, 118, 48, 57, 65, 
	90, 97, 122, 50, 95, 48, 57, 65, 
	90, 97, 122, 69, 89, 95, 101, 121, 
	48, 57, 65, 90, 97, 122, 76, 95, 
	108, 48, 57, 65, 90, 97, 122, 69, 
	95, 101, 48, 57, 65, 90, 97, 122, 
	67, 95, 99, 48, 57, 65, 90, 97, 
	122, 84, 95, 116, 48, 57, 65, 90, 
	97, 122, 83, 95, 115, 48, 57, 65, 
	90, 97, 122, 84, 95, 116, 48, 57, 
	65, 90, 97, 122, 69, 95, 101, 48, 
	57, 65, 90, 97, 122, 77, 95, 109, 
	48, 57, 65, 90, 97, 122, 83, 95, 
	115, 48, 57, 65, 90, 97, 122, 69, 
	95, 101, 48, 57, 65, 90, 97, 122, 
	0
};

static const char _lex_single_lengths[] = {
	4, 4, 5, 5, 5, 5, 4, 5, 
	5, 5, 5, 5, 5, 5, 4, 5, 
	5, 5, 5, 5, 4, 24, 1, 7, 
	4, 3, 1, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 1, 3, 2, 5, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3
};

static const char _lex_range_lengths[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 2, 0, 0, 
	0, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3
};

static const short _lex_index_offsets[] = {
	0, 5, 10, 16, 22, 28, 34, 39, 
	45, 51, 57, 63, 69, 75, 81, 86, 
	92, 98, 104, 110, 116, 121, 148, 150, 
	158, 163, 170, 175, 182, 189, 196, 203, 
	210, 217, 224, 231, 238, 245, 252, 259, 
	266, 273, 280, 285, 292, 298, 307, 314, 
	321, 328, 335, 342, 349, 356, 363, 370
};

static const char _lex_indicies[] = {
	0, 0, 2, 3, 1, 0, 0, 4, 
	3, 1, 5, 5, 2, 3, 6, 1, 
	5, 5, 2, 3, 7, 1, 5, 5, 
	2, 3, 8, 1, 5, 5, 2, 3, 
	9, 1, 5, 5, 10, 3, 1, 5, 
	5, 2, 3, 11, 1, 5, 5, 2, 
	3, 12, 1, 5, 5, 2, 3, 13, 
	1, 5, 5, 2, 3, 14, 1, 5, 
	5, 15, 3, 16, 1, 5, 5, 2, 
	3, 17, 1, 5, 5, 2, 18, 3, 
	1, 5, 5, 19, 3, 1, 5, 5, 
	2, 3, 20, 1, 5, 5, 2, 3, 
	21, 1, 5, 5, 2, 3, 22, 1, 
	5, 5, 2, 3, 23, 1, 5, 5, 
	2, 3, 24, 1, 5, 5, 25, 3, 
	1, 27, 28, 29, 27, 30, 31, 32, 
	33, 34, 35, 36, 38, 39, 40, 41, 
	42, 43, 36, 38, 39, 40, 41, 42, 
	43, 37, 37, 26, 28, 44, 44, 44, 
	2, 3, 45, 46, 47, 1, 48, 48, 
	2, 3, 1, 49, 37, 49, 37, 37, 
	37, 48, 37, 37, 37, 37, 0, 50, 
	37, 50, 37, 37, 37, 48, 51, 37, 
	51, 37, 37, 37, 48, 52, 37, 52, 
	37, 37, 37, 48, 53, 37, 53, 37, 
	37, 37, 48, 54, 37, 54, 37, 37, 
	37, 48, 55, 37, 55, 37, 37, 37, 
	48, 56, 37, 56, 37, 37, 37, 48, 
	57, 37, 57, 37, 37, 37, 48, 58, 
	37, 58, 37, 37, 37, 48, 59, 37, 
	59, 37, 37, 37, 48, 60, 37, 60, 
	37, 37, 37, 48, 61, 37, 61, 37, 
	37, 37, 48, 62, 37, 62, 37, 37, 
	37, 48, 63, 37, 63, 37, 37, 37, 
	48, 64, 37, 64, 37, 37, 37, 48, 
	66, 37, 37, 37, 65, 67, 37, 67, 
	37, 37, 37, 48, 68, 37, 37, 37, 
	37, 48, 69, 70, 37, 69, 70, 37, 
	37, 37, 48, 71, 37, 71, 37, 37, 
	37, 48, 72, 37, 72, 37, 37, 37, 
	48, 73, 37, 73, 37, 37, 37, 48, 
	74, 37, 74, 37, 37, 37, 48, 75, 
	37, 75, 37, 37, 37, 48, 76, 37, 
	76, 37, 37, 37, 48, 77, 37, 77, 
	37, 37, 37, 48, 78, 37, 78, 37, 
	37, 37, 48, 79, 37, 79, 37, 37, 
	37, 48, 80, 37, 80, 37, 37, 37, 
	48, 0
};

static const char _lex_trans_targs[] = {
	21, 0, 21, 1, 24, 21, 3, 4, 
	5, 6, 21, 8, 9, 10, 11, 21, 
	12, 13, 14, 21, 16, 17, 18, 19, 
	20, 21, 21, 21, 21, 22, 23, 21, 
	21, 21, 21, 21, 25, 26, 27, 31, 
	34, 38, 45, 54, 21, 2, 7, 15, 
	21, 26, 28, 29, 30, 26, 32, 33, 
	26, 35, 36, 37, 26, 39, 40, 41, 
	42, 21, 43, 44, 26, 46, 50, 47, 
	48, 49, 26, 51, 52, 53, 26, 55, 
	26
};

static const char _lex_trans_actions[] = {
	39, 0, 23, 0, 65, 37, 0, 0, 
	0, 0, 7, 0, 0, 0, 0, 9, 
	0, 0, 0, 11, 0, 0, 0, 0, 
	0, 5, 29, 27, 25, 0, 68, 19, 
	21, 13, 15, 17, 0, 65, 0, 0, 
	0, 0, 0, 0, 35, 0, 0, 0, 
	33, 50, 0, 0, 0, 53, 0, 0, 
	44, 0, 0, 0, 59, 0, 0, 0, 
	0, 31, 0, 0, 62, 0, 0, 0, 
	0, 0, 41, 0, 0, 0, 56, 0, 
	47
};

static const char _lex_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 1, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0
};

static const char _lex_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 3, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0
};

static const short _lex_eof_trans[] = {
	1, 1, 6, 6, 6, 6, 6, 6, 
	6, 6, 6, 6, 6, 6, 6, 6, 
	6, 6, 6, 6, 6, 0, 45, 45, 
	49, 49, 1, 49, 49, 49, 49, 49, 
	49, 49, 49, 49, 49, 49, 49, 49, 
	49, 49, 66, 49, 49, 49, 49, 49, 
	49, 49, 49, 49, 49, 49, 49, 49
};

static const int lex_start = 21;
static const int lex_first_final = 21;
static const int lex_error = -1;

static const int lex_en_main = 21;


#line 60 "lex.rh"


static token_t copy_value(lex_t* lex, const char* ts, const char* te, token_t token) {
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

  
#line 325 "lex.h"
	{
	cs = lex_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 333 "lex.h"
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
#line 352 "lex.h"
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
	_trans = _lex_indicies[_trans];
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
#line 103 "lex.rh"
	{act = 1;}
	break;
	case 4:
#line 104 "lex.rh"
	{act = 2;}
	break;
	case 5:
#line 105 "lex.rh"
	{act = 3;}
	break;
	case 6:
#line 106 "lex.rh"
	{act = 4;}
	break;
	case 7:
#line 107 "lex.rh"
	{act = 5;}
	break;
	case 8:
#line 108 "lex.rh"
	{act = 6;}
	break;
	case 9:
#line 109 "lex.rh"
	{act = 7;}
	break;
	case 10:
#line 111 "lex.rh"
	{act = 9;}
	break;
	case 11:
#line 117 "lex.rh"
	{act = 15;}
	break;
	case 12:
#line 120 "lex.rh"
	{act = 18;}
	break;
	case 13:
#line 108 "lex.rh"
	{te = p+1;{ token = TK_SYSTEM; {p++; goto _out; } }}
	break;
	case 14:
#line 109 "lex.rh"
	{te = p+1;{ token = TK_LOCAL; {p++; goto _out; } }}
	break;
	case 15:
#line 110 "lex.rh"
	{te = p+1;{ token = TK_PEERS; {p++; goto _out; } }}
	break;
	case 16:
#line 111 "lex.rh"
	{te = p+1;{ token = TK_PEERS_V2; {p++; goto _out; } }}
	break;
	case 17:
#line 112 "lex.rh"
	{te = p+1;{ token = TK_STAR; {p++; goto _out; } }}
	break;
	case 18:
#line 113 "lex.rh"
	{te = p+1;{ token = TK_COMMA; {p++; goto _out; } }}
	break;
	case 19:
#line 114 "lex.rh"
	{te = p+1;{ token = TK_DOT; {p++; goto _out; } }}
	break;
	case 20:
#line 115 "lex.rh"
	{te = p+1;{ token = TK_LPAREN; {p++; goto _out; } }}
	break;
	case 21:
#line 116 "lex.rh"
	{te = p+1;{ token = TK_RPAREN; {p++; goto _out; } }}
	break;
	case 22:
#line 117 "lex.rh"
	{te = p+1;{ token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }}
	break;
	case 23:
#line 118 "lex.rh"
	{te = p+1;{ lex->line++; }}
	break;
	case 24:
#line 119 "lex.rh"
	{te = p+1;{ /* Skip */ }}
	break;
	case 25:
#line 120 "lex.rh"
	{te = p+1;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 26:
#line 110 "lex.rh"
	{te = p;p--;{ token = TK_PEERS; {p++; goto _out; } }}
	break;
	case 27:
#line 117 "lex.rh"
	{te = p;p--;{ token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }}
	break;
	case 28:
#line 120 "lex.rh"
	{te = p;p--;{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 29:
#line 120 "lex.rh"
	{{p = ((te))-1;}{ token = TK_INVALID; {p++; goto _out; } }}
	break;
	case 30:
#line 1 "NONE"
	{	switch( act ) {
	case 1:
	{{p = ((te))-1;} token = TK_SELECT; {p++; goto _out; } }
	break;
	case 2:
	{{p = ((te))-1;} token = TK_FROM; {p++; goto _out; } }
	break;
	case 3:
	{{p = ((te))-1;} token = TK_USE; {p++; goto _out; } }
	break;
	case 4:
	{{p = ((te))-1;} token = TK_AS; {p++; goto _out; } }
	break;
	case 5:
	{{p = ((te))-1;} token = TK_COUNT; {p++; goto _out; } }
	break;
	case 6:
	{{p = ((te))-1;} token = TK_SYSTEM; {p++; goto _out; } }
	break;
	case 7:
	{{p = ((te))-1;} token = TK_LOCAL; {p++; goto _out; } }
	break;
	case 9:
	{{p = ((te))-1;} token = TK_PEERS_V2; {p++; goto _out; } }
	break;
	case 15:
	{{p = ((te))-1;} token = copy_value(lex, ts, te, TK_ID); {p++; goto _out; } }
	break;
	case 18:
	{{p = ((te))-1;} token = TK_INVALID; {p++; goto _out; } }
	break;
	}
	}
	break;
#line 566 "lex.h"
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
#line 579 "lex.h"
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

#line 125 "lex.rh"


  token = token == TK_INVALID && p == eof ? TK_EOF : token;
  lex->p = p;

  return token;
}

#endif // LEX_RH
