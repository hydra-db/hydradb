parser grammar CypherPreParser;

import Cypher25Parser;

options { tokenVocab = CypherPreLexer; }

preparsedStatement:
   preparserOption* statement;

preparserKeyword:
   EXPLAIN | PROFILE | CYPHER; 

preparserOption:
   EXPLAIN | PROFILE | cypherOptions;

cypher: 
   CYPHER;

cypherOptions:
   cypher cypherVersion? cypherOption*;

cypherOption:
   cypherOptionName EQ cypherOptionValue;

cypherOptionValue:
   (unescapedSymbolicNameString | numberLiteral);

cypherOptionName:
   unescapedSymbolicNameString;

cypherVersion:
   (EXTENDED_IDENTIFIER | UNSIGNED_DECIMAL_INTEGER | DOT | DECIMAL_DOUBLE)+;