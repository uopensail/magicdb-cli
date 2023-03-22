lexer grammar magicdbLexer;
options { caseInsensitive=true; }

SCOL: ';';
DOT: '.';
OPEN_PAR: '(';
CLOSE_PAR: ')';
COMMA: ',';
ASSIGN: '=';
STAR: '*';
QUOTA: '"';

// http://www.sqlite.org/lang_keywords.html

PROPERTIES: 'PROPERTIES';
WITH: 'WITH';
DROP: 'DROP';
DATABASE: 'DATABASE';
IF: 'IF';
EXISTS: 'EXISTS';
SHOW: 'SHOW';
DATABASES: 'DATABASES';
CREATE: 'CREATE';
NOT: 'NOT';
MACHINES: 'MACHINES';
ALTER: 'ALTER';
UPDATE: 'UPDATE';
CURRENT: 'CURRENT';
MACHINE: 'MACHINE';
ADD: 'ADD';
TABLE: 'TABLE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
VERSIONS: 'VERSIONS';
VERSION: 'VERSION';
SET: 'SET';
LOAD: 'LOAD';
DATA: 'DATA';
INTO: 'INTO';
SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
TABLES: 'TABLES';
TRUE: 'TRUE';
FALSE: 'FALSE';

// 定义变量
VARNAME  options { caseInsensitive=false; } : [_a-zA-Z][_a-zA-Z0-9]*; // 变量

// 定义字符串
STRING options { caseInsensitive=false; } : QUOTA (ESC | SAFECODEPOINT)* QUOTA;

fragment ESC: '\\' (["\\/bfnrt] | UNICODE);
fragment UNICODE: 'u' HEX HEX HEX HEX;
fragment HEX options { caseInsensitive=false; }  : [0-9a-fA-F];
fragment SAFECODEPOINT: ~ ["\\\u0000-\u001F];

// 定义数字
NUMBER: '-'? INT ('.' [0-9]+)? EXP?;

fragment INT: '0' | [1-9] [0-9]*;

// no leading zeros

fragment EXP options { caseInsensitive=false; }  : [Ee] [+\-]? INT;

SINGLE_LINE_COMMENT: '--' ~[\r\n]* (('\r'? '\n') | EOF) -> skip;

MULTILINE_COMMENT: '/*' .*? '*/' -> skip;

// \- since - means "range" inside [...]

// 定义空白字符
WS: [ \t\n\r]+ -> skip;