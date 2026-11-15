lexer grammar CypherCmdLexer;

import CypherPreLexer;

PARAM: P A R A M S?;

CLEAR: C L E A R;

HISTORY: H I S T O R Y;

CONNECT: C O N N E C T;

DISCONNECT: D I S C O N N E C T;

WELCOME: W E L C O M E;

SYSINFO: S Y S I N F O;

STYLE: S T Y L E;

RESET: R E S E T;

PLAY: P L A Y;

ACCESSMODE: A C C E S S '-' M O D E;

HELP: H E L P;
// Overrides RCURLY from Cypher25Lexer, whose embedded action is Java
// (that grammar is generated for the Java target in the Neo4j monorepo).
// This is the equivalent action for the antlr4ng TypeScript runtime.
RCURLY
   : '}' { if (this.modeStack.length > 0) this.popMode(); }
   ;
