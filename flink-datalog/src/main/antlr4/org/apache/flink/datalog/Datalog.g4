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

datalog_text
        :  'Schemes' ':' scheme (scheme | EMPTY )*   'Facts' ':' (fact | EMPTY)*   'Rules' ':' (rule | EMPTY)*   'Queries' ':' query (query | EMPTY)*
        ;
scheme
        : ID '(' ID ( ',' ID | EMPTY)* ')'
        ;
fact
        : ID '(' STRING (',' STRING | EMPTY)* ')' '.'
        ;
predicate
        : ID '(' parameter ( ','  parameter | EMPTY)* ')'
        ;
parameter
        : ID | STRING | expression
        ;
expression
        : '(' parameter operator parameter ')'
        ;
rule
        : head_predicate ':-' predicate ( ',' predicate| EMPTY )*  '.'
        ;
head_predicate
        : ID '(' ID ( ',' ID | EMPTY )* ')'
        ;
operator
        : '+' | '*'
        ;
query
        : predicate '?'
        ;
ID
        : ('a'..'z'|'A'..'Z') ('a'..'z'|'A'..'Z'|'0'..'9')*
        ;
STRING
        : '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\''
        ;
fragment EMPTY
        : [ \t\r\n]*
        ;
fragment HEX_DIGIT
        : ('0'..'9'
        |'a'..'f'
        |'A'..'F'
        )
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
COMMENT
        : ( '#' ~('\n'|'\r')* '\r'? '\n'
        | '#|' (.)* '|#' ) -> skip
        ;

WHITESPACE
        : [ \t\r\n]+ -> skip
        ;
