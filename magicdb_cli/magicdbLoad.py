#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# `magicdb-cli` - 'client for magicdb'
# Copyright (C) 2019 - present timepi <timepi123@gmail.com>
# `magicdb-cli` is provided under: GNU Affero General Public License
# (AGPL3.0) https:#www.gnu.org/licenses/agpl-3.0.html unless stated otherwise.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#

# transform hive table in parquet format to sqlite,
# and then upload to oss/s3

import argparse
import json
import os
import shutil
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor
from enum import IntEnum
from multiprocessing import cpu_count
from typing import List

import mmh3
import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from magicdb_cli.bucket_util import *

PY_ARROW_INTEGER_TYPE = [
    pyarrow.lib.Type_INT8,
    pyarrow.lib.Type_INT16,
    pyarrow.lib.Type_INT32,
    pyarrow.lib.Type_INT64,
    pyarrow.lib.Type_UINT8,
    pyarrow.lib.Type_UINT16,
    pyarrow.lib.Type_UINT32,
    pyarrow.lib.Type_UINT64,
]
PY_ARROW_FLOAT_TYPE = [
    pyarrow.lib.Type_FLOAT,
    pyarrow.lib.Type_DOUBLE,
    pyarrow.lib.Type_DECIMAL128,
    pyarrow.lib.Type_DECIMAL256,
]
PY_ARROW_STRING_TYPE = [pyarrow.lib.Type_STRING, pyarrow.lib.Type_LARGE_STRING]

MERGE_TABLE_SCRIPT = """#!/bin/bash -xv
rm -rf PATH
sqlite3 PATH <<__SQL__
CREATE_SCRIPT
ATTACH_SCRIPT
INSERT_SCRIPT
__SQL__
"""


def pyarrow_type_to_sqlite_type(field: pyarrow.lib.Field) -> str:
    """map pyarrow type to sqlite type

    Args:
        field (pyarrow.lib.Field): pyarrow type

    Raises:
        TypeError: not support complex types

    Returns:
        str: sqlite type
    """
    if field.type.id in PY_ARROW_INTEGER_TYPE:
        return "INTEGER"
    elif field.type.id in PY_ARROW_FLOAT_TYPE:
        return "REAL"
    elif field.type.id in PY_ARROW_STRING_TYPE:
        return "TEXT"
    raise TypeError(f"{field.type} not support")


class DataType(IntEnum):
    StringListType = 1
    Int64ListType = 2
    Float32ListType = 3


class StoreType(IntEnum):
    TextType = 1
    IntegerType = 2
    RealType = 3


def generate_features_from_schema(schema: pyarrow.Schema) -> dict:
    """generate features from schema

    Args:
        schema (pyarrow.Schema): schame of parquet table

    Returns:
        list: features list
    """
    feature_list = []

    def parquet_type_to_feature_type(field: pyarrow.lib.Field):
        if field.type.id in PY_ARROW_INTEGER_TYPE:
            return DataType.Int64ListType, StoreType.IntegerType
        elif field.type.id in PY_ARROW_FLOAT_TYPE:
            return DataType.Float32ListType, StoreType.RealType
        elif field.type.id in PY_ARROW_STRING_TYPE:
            return DataType.StringListType, StoreType.TextType
        raise TypeError(f"{field.type} not support")

    for col in schema:
        dtype, stype = parquet_type_to_feature_type(col)
        feature_list.append({"column": col.name,
                         "dtype": dtype.value, "stype": stype.value})
    return feature_list


def generate_sqlite_ddl(schema: pyarrow.Schema, key_name: str, table_name: str) -> str:
    """generate sqlite table ddl

    Args:
        schema (pyarrow.Schema): schame of parquet table
        key_name (str): primary key, str or int
        table_name (str): table name

    Raises:
        TypeError: primary key type error

    Returns:
        str: return ddl
    """
    items = []
    for col in schema:
        col_type = pyarrow_type_to_sqlite_type(col)
        if col.name == key_name:
            if col_type not in ("TEXT", "INTEGER"):
                raise TypeError(f"{col.type} not support for primary key")
            items.append(f"{key_name} {col_type} PRIMARY KEY NOT NULL")
        else:
            items.append(f"{col.name} {col_type}")
    return "create table %s (%s\n);" % (table_name, ",\n".join(items))


def get_sqlite_insert_sql(schema: pyarrow.Schema, table_name: str) -> str:
    """generate sqlite table insert sql

    Args:
        schema (pyarrow.Schema): schame of parquet table
        table_name (str): table name

    Returns:
        str: return dml
    """
    columns = list(map(lambda c: c.name, schema))
    dml = "insert into %s (%s) values (%s);" % (
        table_name,
        ",".join(columns),
        ",".join(["?"] * len(columns)),
    )
    return dml


def get_table_schema(bucket: str,
                     path: str,
                     work_dir: str,
                     **kwargs: str,
                     ) -> pyarrow.Schema:
    """get the schema of table

    Args:
        path (str): remote path
        work_dir (str): current work dir
        endpoint (str, optional): endpoint of s3/oss. Defaults to "".

    Returns:
        pyarrow.Schema: return the schema
    """

    base_name = os.path.basename(path)
    local_file = os.path.join(work_dir, base_name)
    bucket_download_file(bucket=bucket, remote_path=path, dst_path=local_file, **kwargs)
    table = pq.read_table(local_file)
    return table.schema


def parquet_to_raw_sqlite(
        bucket: str,
        path: str,
        schema: pyarrow.Schema,
        key_name: str,
        table_name: str,
        work_dir: str,
        partitions: int,
        **kwargs: str,
):
    """transform parquet file tp sqlite file

    Args:
        path (str): remote s3/oss file path
        bucket (str): bucket name
        schema (pyarrow.Schema): schema of parquet
        key_name (str): primary key
        table_name (str): table name
        work_dir (str): current work dir
        partitions (int, optional): _description_. Defaults to 100.
    """
    base_name = os.path.basename(path)
    local_parquet_file = os.path.join(work_dir, base_name)
    db_path = os.path.join(work_dir, f"{base_name}.db")
    ddls, dmls = [], []
    print(f"processing parquet file: {path} to sqlite: {db_path}...")

    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(local_parquet_file):
        os.remove(local_parquet_file)

    bucket_download_file(bucket=bucket, remote_path=path, dst_path=local_parquet_file, **kwargs)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # create tables
    for i in range(partitions):
        ddl = generate_sqlite_ddl(
            schema=schema, key_name=key_name, table_name=f"{table_name}_{i}"
        )
        dml = get_sqlite_insert_sql(
            schema=schema, table_name=f"{table_name}_{i}")
        ddls.append(ddl)
        dmls.append(dml)
        cur.execute(ddl)
    conn.commit()
    cur.execute("""PRAGMA synchronous = OFF;""")
    cur.execute("""PRAGMA journal_mode = OFF;""")

    def hash_func(x):
        return mmh3.hash64(str(x[key_name]), signed=False)[0] % partitions

    df = pd.read_parquet(path=local_parquet_file)
    df[bucket] = df.apply(hash_func, axis=1)

    # write data to sqlite
    for i in range(partitions):
        tmp = df[df[bucket] == i]
        del tmp[bucket]
        data = tmp.to_numpy()
        cur.executemany(dmls[i], data)
        conn.commit()
    cur.close()
    conn.close()
    print(f"finish processing parquet file: {path} to sqlite: {db_path}")


def parquets_to_raw_sqlites(
        schema: pyarrow.Schema,
        bucket: str,
        input_files: List[str],
        output_dir: str,
        key_name: str,
        table_name: str,
        worker: int,
        partitions: int = 100,
        **kwargs: str,
) -> List[str]:
    """transform all parquet files to sqlite files

    Args:
        schema (pyarrow.Schema): parquet schema
        bucket (str): bucket name
        input_files (List[str]): parquet file list
        output_dir (str): where to put the sqlite files
        key_name (str): primary key
        table_name (str): table name
        worker (int): work number
        partitions (int, optional): how many tables to split. Defaults to 100.

    Returns:
        List[str]: return the sqlite file list
    """
    output_files = []
    pool = ProcessPoolExecutor(max_workers=worker)
    for i in range(len(input_files)):
        base_name = os.path.basename(input_files[i])
        local_file = os.path.join(output_dir, f"{base_name}.db")
        output_files.append(local_file)
        pool.submit(
            parquet_to_raw_sqlite,
            bucket,
            input_files[i],
            schema,
            key_name,
            table_name,
            output_dir,
            partitions,
            **kwargs,
        )
    pool.shutdown(wait=True)
    return output_files


def build_table_partition(
        output_dir: str,
        bash_dir: str,
        partition: int,
        table_name: str,
        ddl: str,
        inputs: List[str],
):
    """build partition data of this table

    Args:
        output_dir (str): where to put the partition data
        bash_dir (str): where to put bash file
        partition (int): how many tables to split
        table_name (str): table name
        ddl (str): to create the sqlite table
        inputs (List[str]): origin sqlite input files
    """
    index_str = "{:0>5d}".format(partition)
    local_path = os.path.join(output_dir, f"{index_str}.db")

    # generate bash script files and then use bash to merge sqlites
    attach_items, insert_items = [], []
    for i, sfile in enumerate(inputs):
        attach_items.append(f"attach database '{sfile}' as 'data_{i}';")
        insert_items.append(
            f"insert into {table_name} select * from data_{i}.{table_name}_{partition};"
        )
    attach_script = "\n".join(attach_items)
    insert_script = "\n".join(insert_items)

    bash_script = (
        MERGE_TABLE_SCRIPT.replace("ATTACH_SCRIPT", attach_script)
        .replace("PATH", local_path)
        .replace("INSERT_SCRIPT", insert_script)
        .replace("CREATE_SCRIPT", ddl)
    )

    bash_file = os.path.join(bash_dir, f"bash_{partition}.sh")
    f = open(bash_file, "w")
    f.write(bash_script)
    f.close()
    os.system(f"bash {bash_file}")


def r_listfiles(bucket: str,
                path: str,
                **kwargs: str,
                ) -> List[str]:
    """list the file on oss/s3

    Args:
        path (str): remote oss/s3 dir
        endpoint (str, optional): endpoint of oss/s3. Defaults to "".

    Returns:
        List[str]: files in remote dir
    """
    return bucket_list_file_on_cur_dir(bucket=bucket, prefix=path, **kwargs)


def build_table(
        table_name: str,
        ddl: str,
        input_files: List[str],
        output_dir: str,
        bash_dir: str,
        partitions: int,
        worker: int,
) -> List[str]:
    """build table data

    Args:
        table_name (str): table name
        ddl (str): create table
        input_files (List[str]): origin sqlite files
        output_dir (str): where to put the table data
        bash_dir (str): where to put the bash file
        partitions (int): how many tables to split
        worker (int): work number to process

    Returns:
        List[str]: return table partition files
    """
    pool = ProcessPoolExecutor(max_workers=worker)
    output_files = []
    for i in range(partitions):
        index_str = "{:0>5d}".format(i)
        output_files.append(os.path.join(output_dir, f"{index_str}.db"))
        pool.submit(
            build_table_partition,
            output_dir,
            bash_dir,
            i,
            table_name,
            ddl,
            input_files,
        )
    pool.shutdown(wait=True)
    return output_files


def to_magicdb(
        work_dir: str,
        workers: int,
        bucket: str,
        hive_table_dir: str,
        s3_data_dir: str,
        s3_meta_dir: str,
        key_name: str,
        table_name: str,
        partitions: int = 100,
        **kwargs,
) -> str:
    """to magicdb data

    Args:
        work_dir (str): current work dir
        workers (int): work number to process
        bucket (str): bucket name example: oss://bucket1 | s3://bucket1
        hive_table_dir (str): hvie table path
        s3_data_dir (str): magicdb table data path
        s3_meta_dir (str): magicdb table meta path
        key_name (str): primary key
        table_name (str): table name
        partitions (int, optional): how many tables to split. Defaults to 100.

    Returns:
        str: version of magicdb table
    """

    timestamp = int(time.time())
    work_dir = os.path.join(work_dir, str(timestamp))
    _raw_sqlite_dir = os.path.join(work_dir, "_sqlite")
    _bash_dir = os.path.join(work_dir, "_bash")
    table_dir = os.path.join(work_dir, "sqlite")

    remote_dir = os.path.join(s3_data_dir, str(timestamp))
    version = f"{table_name}.json@{timestamp}"
    local_meta_file = os.path.join(work_dir, f"{table_name}.json")
    remote_meta_file = os.path.join(s3_meta_dir, version)

    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)

    os.makedirs(work_dir)
    os.makedirs(_raw_sqlite_dir)
    os.makedirs(_bash_dir)
    os.makedirs(table_dir)

    parquet_files = r_listfiles(bucket, hive_table_dir, **kwargs)
    assert len(parquet_files) > 0
    schema = get_table_schema(bucket=bucket,
                              path=parquet_files[0], work_dir=work_dir, **kwargs
                              )
    ddl = generate_sqlite_ddl(
        schema=schema, key_name=key_name, table_name=table_name)

    raw_sqlite_files = parquets_to_raw_sqlites(
        bucket=bucket,
        schema=schema,
        input_files=parquet_files,
        output_dir=_raw_sqlite_dir,
        key_name=key_name,
        table_name=table_name,
        worker=workers,
        partitions=partitions,
        **kwargs,
    )

    partition_files = build_table(
        table_name=table_name,
        ddl=ddl,
        input_files=raw_sqlite_files,
        output_dir=table_dir,
        bash_dir=_bash_dir,
        partitions=partitions,
        worker=workers,
    )

    remote_paths = []
    for pfile in partition_files:
        base_name = os.path.basename(pfile)
        remote_path = os.path.join(remote_dir, base_name)
        remote_paths.append(remote_path)
        bucket_upload_file(local_path=pfile, bucket=bucket, remote_path=remote_path, **kwargs)

    data = {
        "name": table_name,
        "partitions": remote_paths,
        "key": key_name,
        "version": version,
        "features": generate_features_from_schema(schema=schema),
    }

    json.dump(data, open(local_meta_file, "w"))
    bucket_upload_file(local_path=local_meta_file, bucket=bucket, remote_path=remote_meta_file, **kwargs)
    shutil.rmtree(work_dir)
    return version


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work_dir", type=str, required=True, help="work dir")
    parser.add_argument(
        "--data", type=str, required=True, help="emote path to put data file"
    )
    parser.add_argument(
        "--meta", type=str, required=True, help="remote path to put meta file"
    )
    parser.add_argument(
        "--bucket", type=str, required=True, help="remote bucket name, example: s3://bucket-name"
    )
    parser.add_argument(
        "--path", type=str, required=True, help="remote path to of table"
    )
    parser.add_argument("--table", type=str, required=True, help="table name")
    parser.add_argument("--key", type=str, required=True,
                        help="primary key of table")
    parser.add_argument("--access_key", type=str,
                        default="", help="access key")
    parser.add_argument("--secret_key", type=str,
                        default="", help="secret key")
    parser.add_argument("--region", type=str, default="", help="region")
    parser.add_argument("--endpoint", type=str, default="", help="endpoint")
    parser.add_argument(
        "--partition", type=int, default=100, help="partition number of table"
    )
    parser.add_argument(
        "--worker",
        type=int,
        default=max(1, cpu_count() - 1),
        help="worker number to process",
    )
    args = parser.parse_args()

    boto3_kwargs = {}
    if args.access_key != "":
        boto3_kwargs["aws_access_key_id"] = args.access_key
    if args.secret_key != "":
        boto3_kwargs["aws_secret_access_key"] = args.secret_key
    if args.region != "":
        boto3_kwargs["region_name"] = args.region
    if args.endpoint != "":
        boto3_kwargs["endpoint_url"] = args.endpoint

    to_magicdb(
        work_dir=args.work_dir,
        workers=args.worker,
        hive_table_dir=args.path,
        s3_data_dir=args.data,
        s3_meta_dir=args.meta,
        table_name=args.table,
        key_name=args.key,
        partitions=args.partition,
        endpoint=args.endpoint,
        **boto3_kwargs,
    )


if __name__ == "__main__":
    main()
