//定义magicdb的命令
//antlr -Dlanguage=Python3 magicdb.g4
//antlr -no-listener -visitor -Dlanguage=Python3 magicdb.g4
grammar magicdb;
start: command EOF;

command
    : database_cmd
    | machine_cmd
    | table_cmd
    | version_cmd
    | load_data
    | select_data
    ;


// 数据库相关的操作
database_cmd
   : drop_database
   | show_databases
   | create_database
   ;
drop_database
   : DROP DATABASE (IF EXISTS)? database_name ';'
   ;
show_databases
   : SHOW DATABASES ';'
   ;
create_database
   : CREATE DATABASE (IF NOT EXISTS)? database_name properties ';'
   ;

// 机器相关的操作
machine_cmd
   : show_machines
   | delete_machine
   | add_machine
   ;
show_machines
   : SHOW MACHINES database_name ';'
   ;
delete_machine
   : ALTER DATABASE database_name DROP MACHINE '(' STRING ')' ';'
   ;
add_machine
   : ALTER DATABASE database_name ADD MACHINE '(' STRING ')' ';'
   ;

// 定义表的相关操作
table_cmd
   : show_tables
   | drop_table
   | create_table
   | desc_table
   ;
show_tables
   : SHOW TABLES database_name ';'
   ;
drop_table
   : DROP TABLE (IF EXISTS)? table ';'
   ;
create_table
   : CREATE TABLE (IF NOT EXISTS)? table properties ';'
   ;
desc_table
   : DESC table ';'
   | DESCRIBE table ';'
   ;

// 定义版本的相关操作
version_cmd
   : show_versions
   | show_current_version
   | update_version
   | drop_version
   ;
show_versions
   : SHOW VERSIONS table ';'
   ;
show_current_version
   : SHOW CURRENT VERSION table ';'
   ;
update_version
   : UPDATE TABLE table SET CURRENT VERSION '=' STRING ';'
   ;
drop_version
   : ALTER TABLE table DROP VERSION '(' STRING ')' ';'
   ;

// 上传数据
load_data
   : LOAD DATA STRING INTO TABLE table properties? ';'
   ;

// 查询数据
select_data
   : SELECT '*' FROM table WHERE VARNAME '=' STRING ';'
   ;

// 属性
properties
   : WITH PROPERTIES '(' pair (',' pair)* ')'
   ;

PROPERTIES 
   : 'PROPERTIES'
   | 'properties'
   ;
WITH 
   : 'WITH'
   |'with'
   ;
DROP 
   : 'DROP'
   | 'drop'
   ;
DATABASE 
   : 'DATABASE'
   | 'database'
   ;
IF 
   : 'IF'
   | 'if'
   ;
EXISTS 
   : 'EXISTS'
   | 'exists'
   ;
SHOW
   : 'SHOW'
   | 'show'
   ;
DATABASES 
   : 'DATABASES'
   | 'databases'
   ;
CREATE 
   : 'CREATE'
   | 'create'
   ;
NOT 
   : 'NOT'
   | 'not'
   ;
MACHINES 
   : 'MACHINES'
   | 'machines'
   ;
ALTER 
   : 'ALTER'
   | 'alter'
   ;
UPDATE
   : 'UPDATE'
   | 'update'
   ;
CURRENT
   : 'CURRENT'
   | 'current'
   ;
MACHINE 
   : 'MACHINE'
   | 'machine'
   ;
ADD 
   : 'ADD'
   | 'add'
   ;
TABLE 
   : 'TABLE'
   | 'table'
   ;
DESC 
   : 'DESC'
   | 'desc'
   ;
DESCRIBE 
   : 'DESCRIBE'
   | 'describe'
   ;
VERSIONS 
   : 'VERSIONS'
   | 'versions'
   ;
VERSION 
   : 'VERSION'
   | 'version'
   ;
SET 
   : 'SET'
   | 'set'
   ;
LOAD 
   : 'LOAD'
   | 'load'
   ;
DATA 
   : 'DATA'
   | 'data'
   ;
INTO 
   : 'INTO'
   | 'into'
   ;
SELECT 
   : 'SELECT'
   | 'select'
   ;
FROM 
   : 'FROM'
   | 'from'
   ;
WHERE 
   : 'WHERE'
   | 'where'
   ;
KEY 
   : 'KEY'
   | 'key'
   ;
TABLES 
   : 'TABLES'
   | 'tables'
   ;

// 表名
table
   : VARNAME'.'VARNAME
   ;

// 数据库名
database_name
   : VARNAME
   ;

// 定义键值对
pair
   : STRING '=' value
   ;

value
   : STRING
   | NUMBER
   | 'true'
   | 'false'
   ;


// 定义变量
VARNAME : [_a-zA-Z][_a-zA-Z0-9]*; // 变量



// 定义字符串
STRING
   : '"' (ESC | SAFECODEPOINT)* '"'
   ;


fragment ESC
   : '\\' (["\\/bfnrt] | UNICODE)
   ;
fragment UNICODE
   : 'u' HEX HEX HEX HEX
   ;
fragment HEX
   : [0-9a-fA-F]
   ;
fragment SAFECODEPOINT
   : ~ ["\\\u0000-\u001F]
   ;

// 定义数字
NUMBER
   : '-'? INT ('.' [0-9] +)? EXP?
   ;


fragment INT
   : '0' | [1-9] [0-9]*
   ;

// no leading zeros

fragment EXP
   : [Ee] [+\-]? INT
   ;

// \- since - means "range" inside [...]

// 定义空白字符
WS
   : [ \t\n\r] + -> skip
   ;