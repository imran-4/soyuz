/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar Datalog;

// This grammar is written for Datalog version 2.6
// datalog official: http://datalog.sourceforge.net/datalog.html

/*
 * Parser Rules
 */

compileUnit
    : schema ( fact
    | ruleClause
    | query
    | retraction
    )+ EOF
    ;
schema
    : DATABASE_KEYWORD '(' '{' CONSTANT   '(' (CONSTANT | VARIABLE) ':' DATATYPES columnsList ')' '}' ')' '.'
    ;
columnsList
    : ( ','  (CONSTANT | VARIABLE) ':' DATATYPES )*
    ;
ruleClause
    : predicate ':-' predicateList '.'
    ;
fact
    :  ( CONSTANT | STRING )  '(' constantList ')' '.'
    ;
constantList
    : CONSTANT ( ',' CONSTANT )*
    ;
query
    : predicate '?'
    ;
retraction
    : predicate '~'
    ;
predicateList
    : ( predicate | notPredicate | primitivePredicate ) ( ',' ( predicate | notPredicate | primitivePredicate ) )*
    ;
notPredicate   // only use in predicateList
    : 'not' predicate
    ;
primitivePredicate // only use in predicateList
    : ( CONSTANT | VARIABLE | DECIMAL ) ('!')? '=' ( CONSTANT | VARIABLE | DECIMAL )
    ;
predicate
    : ( CONSTANT | STRING )  '(' termList  ')'
    ;
termList
    : term ( ',' term )*
    ;
term
    : VARIABLE
    | CONSTANT
    | '-'? ( integer )+
    | '{' termList '}'
    | '[' termList ( '|' term )? ']'
    | <assoc=right> term OPERATOR term
    | atom
    ;
atom
    : '{' '}'
    | '[' ']'
    | ';'
    | '!'
    | SYMBOL_TOKEN
    | STRING
    ;
integer
    : DECIMAL+
    | OCTAL
    | BINARY
    | HEX
    ;
/*
 * Lexer Rules
 */

DATABASE_KEYWORD
    : 'database'
    ;
DATATYPES
    : 'Integer' | 'Float' | 'String' | 'Char' | 'Boolean'
    ;
OPERATOR //TODO: add more binary operators
    : '+' | '*' | '-' | '/'
    | '>' | '<'
    | '>>' | '<<'
    ;
UNARY_OPERATOR //TODO: add more unary operators
    : '-' | '+'
    ;
fragment SINGLE_QUOTED_STRING
    : '\'' ( ESC_SEQ | ~( '\\'|'\'' ) )* '\''
    ;
fragment DOUBLE_QUOTED_STRING
    : '"' ( ESC_SEQ | ~('\\'|'"') )* '"'
    ;
fragment INVERTED_QUOTE_STRING
    : '`' ( ESC_SEQ | ~( '\\'|'`' ) )* '`'
    ;
fragment SYMBOL
    : [<>#&$*+/@^] | '-'
    ;
fragment HEX_DIGIT
    : [a-fA-F0-9]
    ;
fragment ESC_SEQ
    : '\\' ('b'|'t'|'n'|'f'|'r'|'"'|'\''|'\\')
    | UNICODE_ESC
    | OCTAL_ESC
    ;
fragment OCTAL_ESC
    : '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    | '\\' ('0'..'7') ('0'..'7')
    | '\\' ('0'..'7')
    ;
fragment UNICODE_ESC
    : '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;
fragment CAPITAL_LETTER
    : [A-Z]
    ;
fragment ALPHANUMERIC
    : ALPHA | DIGIT
    ;
fragment ALPHA
    :  SMALL_LETTER | CAPITAL_LETTER | '_'
    ;
fragment SMALL_LETTER
    : [a-z_]
    ;
fragment DIGIT
    : [0-9]
    ;
CONSTANT
    : SMALL_LETTER ALPHANUMERIC*
    ;
VARIABLE
    : CAPITAL_LETTER ALPHANUMERIC*
    ;
STRING
    : SINGLE_QUOTED_STRING
    | DOUBLE_QUOTED_STRING
    | INVERTED_QUOTE_STRING
    ;
SYMBOL_TOKEN
    : ( SYMBOL | '\\' )+
    ;
DECIMAL
    : DIGIT+
    ;
OCTAL
    : '0o' [0-7]+
    ;
BINARY
    : '0b' [01]+
    ;
HEX
    : '0x' HEX_DIGIT+
    ;
COMMENT
    : '%' ~[\n\r]* ( [\n\r] | EOF) -> channel(HIDDEN)
    ;
MULTILINE_COMMENT
    : '/*' ( MULTILINE_COMMENT | . )*? ( '*/' | EOF ) -> channel(HIDDEN)
    ;
WHITESPACE
    : [ \t\r\n]+ -> skip
    ;
