#!/bin/bash -x

TMPCMDFILE=/tmp/magicdb_unittest.cmd
cat <<EOF | tee ${TMPCMDFILE}
-- database command --
/*
export MAGICDB_BUCKET_ACCESS_KEY=LTAI5t*********
export MAGICDB_BUCKET_SECRET_KEY=LiYFE9*********
bucket: bucket name example: oss://bucket-name | s3://bucket-name
*/
drop database database1;
create database if not exists database1 with 
    properties("access_key" = "${MAGICDB_BUCKET_ACCESS_KEY}","secret_key" = "${MAGICDB_BUCKET_SECRET_KEY}",
                "bucket"="oss://uopensail-test","endpoint"="https://oss-cn-shanghai.aliyuncs.com",
                "region"="cn-shanghai");
show databases;

-- table command --
create table database1.table1 with 
    properties("data_dir"="test/magicdb/database1/table1/datas",
    "meta_dir"="test/magicdb/database1/table1/datas", "key"="idx");
show tables database1;

-- version command --
show versions database1.table1;
update table database1.table1 set current version = "table1.json@1679150222";

-- load command --
load data "test/parquet/" into table database1.table1;



-- machine command --
alter database database1 drop machine("192.168.1.6:6528");
alter database database1 add machine("192.168.1.6:6528");

EOF

ls -al ${TMPCMDFILE}
which magicdbcli
magicdbcli --host 127.0.0.1 --name unittest -f ${TMPCMDFILE}


