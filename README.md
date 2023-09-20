# Install 


```shell
pip install magicdb_cli
```

# Tutorials

## Database Operations

### List All Databases
```sql
show databases;
```

### Create A Database
```sql
-- db_name: the name of this database
create database [if not exists] db_name with properties ("k1"="v1", "k2"="v2");
```
properties `MUST CONTAIN` these keys:
1. bucket: bucket name of OSS/S3 example: (s3://bucket-name | oss://test-bucket)
2. endpoint: endpoint for OSS/S3
3. access_key: token for account
4. secret_key: token for account

### Delete A Database
```sql
drop database [if exists] db_name;
```

## Table Operations
### List All Tables Of A Database
```sql
-- db_name: the name of this database
show tables db_name;
```
### Drop A Table
```sql
-- db_name: the name of this database
drop table [if exists] db_name.table_name;
```

### Create A Table Of A Database
```sql
-- db_name: the name of this database
-- table_name: the name of this table
create table [if not exists] db_name.table_name with properties ("k1"="v1", "k2"="v2");
```
properties `MUST CONTAIN` these keys:
1. data: the path to put sqlite files
2. meta: the path to put meta files

### Show Table Info
```sql
-- db_name: the name of this database
-- table_name: the name of this table
desc db_name.table_name;
describe db_name.table_name;
```

## Versions Operations

### List All Versions Of A Table

```sql
-- db_name: the name of this database
-- table_name: the name of this table
show versions db_name.table_name;
```

### Show Current Version Of A Table

```sql
-- db_name: the name of this database
-- table_name: the name of this table
show current version db_name.table_name;
```

### Update Current Version Of A Table

```sql
-- db_name: the name of this database
-- table_name: the name of this table
-- version1: the name of current version
update table db_name.table_name set current version = "version1";
```

### Drop A Version Of A Table

```sql
-- db_name: the name of this database
-- table_name: the name of this table
-- version1: the name of current version

-- if `version1` is current version, then the new current version is set `nil`.
alter table db_name.table_name drop version("version1");
```


## Machine Operations

### List ALL Machines of A Database
```sql
-- db_name: the name of this database
show machines db_name;
```

### Delete A Machine Of A Database
```sql
-- db_name: the name of this database
-- machine_ip: ip of this machine
alter database db_name drop machine("machine_ip");
```

### Add A Machine Of A Database
```sql
-- db_name: the name of this database
-- machine_ip: ip of this machine
alter database db_name add machine("machine_ip");
```


## Load And Select Operations

### Load Data To A Table
```sql
-- db_name: the name of this database
-- table_name: the name of this table
-- properties are optional
-- path: hive table path

load data "path" into db_name.table_name [with with properties ("k1"="v1", "k2":"v2")];

```
properties `MUST CONTAIN` these keys:
1. key: primary key of table

properties `MAY CONTAIN` these keys:
1. partitions: partitions to split, default: 100
2. workdir: where to save the load data, defalue: /tmp/$db_name/$table_name/$timestamp/
3. workers: process num, default: max(cup()-1, 1)



### Select Data From Table
```sql
-- db_name: the name of this database
-- table_name: the name of this table
-- key: the primary key value
-- field: the primary key field

select * from db_name.table_name where field = 'key';
```
 