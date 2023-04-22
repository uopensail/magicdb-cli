//定义magicdb的命令
//antlr -Dlanguage=Python3 magicdb.g4
//antlr -no-listener -visitor -Dlanguage=Python3 magicdb.g4
parser grammar magicdbParser;

options {
    tokenVocab = magicdbLexer;
}
parse: command_list EOF;

command_list
   : SCOL* command (SCOL+ command)* SCOL*
   ;

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
   : DROP DATABASE (IF EXISTS)? database_name
   ;
show_databases
   : SHOW DATABASES
   ;
create_database
   : CREATE DATABASE (IF NOT EXISTS)? database_name properties
   ;

// 机器相关的操作
machine_cmd
   : show_machines
   | delete_machine
   | add_machine
   ;
show_machines
   : SHOW MACHINES database_name
   ;
delete_machine
   : ALTER DATABASE database_name DROP MACHINE OPEN_PAR STRING CLOSE_PAR
   ;
add_machine
   : ALTER DATABASE database_name ADD MACHINE OPEN_PAR STRING CLOSE_PAR
   ;

// 定义表的相关操作
table_cmd
   : show_tables
   | drop_table
   | create_table
   | desc_table
   ;
show_tables
   : SHOW TABLES database_name
   ;
drop_table
   : DROP TABLE (IF EXISTS)? table
   ;
create_table
   : CREATE TABLE (IF NOT EXISTS)? table properties
   ;
desc_table
   : DESC table
   | DESCRIBE table
   ;

// 定义版本的相关操作
version_cmd
   : show_versions
   | show_current_version
   | update_version
   | drop_version
   ;
show_versions
   : SHOW VERSIONS table
   ;
show_current_version
   : SHOW CURRENT VERSION table
   ;
update_version
   : UPDATE TABLE table SET CURRENT VERSION ASSIGN STRING
   ;
drop_version
   : ALTER TABLE table DROP VERSION OPEN_PAR STRING CLOSE_PAR
   ;

// 上传数据
load_data
   : LOAD DATA STRING INTO TABLE table properties?
   ;

// 查询数据
select_data
   : SELECT STAR FROM table WHERE VARNAME ASSIGN STRING
   ;

// 属性
properties
   : WITH PROPERTIES OPEN_PAR pair (COMMA pair)* CLOSE_PAR
   ;


// 表名
table
   : VARNAME DOT VARNAME
   ;

// 数据库名
database_name
   : VARNAME
   ;

// 定义键值对
pair
   : STRING ASSIGN value
   ;

value
   : STRING
   | NUMBER
   | TRUE
   | FALSE
   ;