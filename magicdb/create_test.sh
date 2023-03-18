#!/bin/bash

TMPCMDFILE=/tmp/magicdb_unittest.cmd
cat <<EOF | tee ${TMPCMDFILE}
create database if not exists database1 with 
    properties("access_key" = "${MAGICDB_BUCKET_ACCESS_KEY}","secret_key" = "${MAGICDB_BUCKET_SECRET_KEY}",
                "bucket"="oss://uopensail-test","endpoint"="https://oss-cn-shanghai.aliyuncs.com",
                "region"="cn-shanghai");


show databases;
create table database1.table1 with 
    properties("data_dir"="test/magicdb/database1/table1/datas",
    "meta_dir"="test/magicdb/database1/table1/datas", "key"="idx");

show tables database1;

show versions database1.table1;
load data "test/parquet/" into table database1.table1;

alter database database1 drop machine("192.168.1.6:6528");
alter database database1 add machine("192.168.1.6:6528");
update table database1.table1 set current version = "table1.json@1679150222";

EOF

ls -al ${TMPCMDFILE}
which magicdbcli
magicdbcli --host 127.0.0.1 --name unittest -f ${TMPCMDFILE}


