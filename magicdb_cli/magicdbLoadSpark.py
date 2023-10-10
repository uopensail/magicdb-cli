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

# use pyspark to process magicdb sqlite files

# spark-submit \
#  --queue default \
#  --master yarn \
#  --num-executors 10 \
#  --executor-cores 4 \
#  --executor-memory 8G \
#  --driver-memory 20G \
#  --conf spark.default.parallelism=600 \
#  --conf spark.debug.maxToStringFields=1000 \
#  --conf spark.dynamicAllocation.enabled=false \
#  --conf spark.storage.memoryFraction=0.4 \
#  magicdbLoadSpark.py --input oss://xxx/data/dt=20230626 \
#  --table test_table \
#  --key pk \
#  --partition 100 \
#  --data_dir oss://xxx/tmp/db/data \
#  --meta_dir oss://xxx/tmp/db/meta \
#  --cmd "/opt/apps/HADOOP-COMMON/hadoop-common-current/bin/hadoop fs -put -f %s %s"


import argparse
import json
import os
import random
import shutil
import sqlite3
import time
from enum import IntEnum
from typing import Callable, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    ByteType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)


class FeatureDataType(IntEnum):
    StringListType = 1
    Int64ListType = 2
    Float32ListType = 3


class FeatureStoreType(IntEnum):
    TextType = 1
    IntegerType = 2
    RealType = 3

    @property
    def name(self) -> str:
        """get the name of the feature store type

        Args:
            stype (FeatureStoreType): store type

        Raises:
            TypeError: not supported

        Returns:
            str: name of the feature store type
        """
        if self.value == FeatureStoreType.TextType:
            return "TEXT"
        elif self.value == FeatureStoreType.IntegerType:
            return "INTEGER"
        elif self.value == FeatureStoreType.RealType:
            return "REAL"
        else:
            raise TypeError(f"store type: {self.value} not supported")


def unsigned2signed(v):
    usv = v & 0xFFFFFFFFFFFFFFFF
    if usv & 0x8000000000000000 == 0:
        return usv
    return -((v ^ 0xFFFFFFFFFFFFFFFF) + 1)


def fmix(k):
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & 0xFFFFFFFFFFFFFFFF
    k ^= k >> 33
    return k


def hash64(key, seed=0, signed=True):
    length = len(key)
    nblocks = int(length / 16)
    h1 = seed
    h2 = seed

    c1 = 0x87C37B91114253D5
    c2 = 0x4CF5AD432745937F
    # body
    for block_start in range(0, nblocks * 8, 8):
        # ??? big endian?
        k1 = (
            ord(key[2 * block_start + 7]) << 56
            | ord(key[2 * block_start + 6]) << 48
            | ord(key[2 * block_start + 5]) << 40
            | ord(key[2 * block_start + 4]) << 32
            | ord(key[2 * block_start + 3]) << 24
            | ord(key[2 * block_start + 2]) << 16
            | ord(key[2 * block_start + 1]) << 8
            | ord(key[2 * block_start + 0])
        )

        k2 = (
            ord(key[2 * block_start + 15]) << 56
            | ord(key[2 * block_start + 14]) << 48
            | ord(key[2 * block_start + 13]) << 40
            | ord(key[2 * block_start + 12]) << 32
            | ord(key[2 * block_start + 11]) << 24
            | ord(key[2 * block_start + 10]) << 16
            | ord(key[2 * block_start + 9]) << 8
            | ord(key[2 * block_start + 8])
        )
        k1 = (c1 * k1) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 << 31 | k1 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k1 = (c2 * k1) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1

        h1 = (h1 << 27 | h1 >> 37) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
        h1 = (h1 * 5 + 0x52DCE729) & 0xFFFFFFFFFFFFFFFF

        k2 = (c2 * k2) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 << 33 | k2 >> 31) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k2 = (c1 * k2) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2

        h2 = (h2 << 31 | h2 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
        h2 = (h2 * 5 + 0x38495AB5) & 0xFFFFFFFFFFFFFFFF
    # tail
    tail_index = nblocks * 16
    k1 = 0
    k2 = 0
    tail_size = length & 15

    if tail_size >= 15:
        k2 ^= ord(key[tail_index + 14]) << 48
    if tail_size >= 14:
        k2 ^= ord(key[tail_index + 13]) << 40
    if tail_size >= 13:
        k2 ^= ord(key[tail_index + 12]) << 32
    if tail_size >= 12:
        k2 ^= ord(key[tail_index + 11]) << 24
    if tail_size >= 11:
        k2 ^= ord(key[tail_index + 10]) << 16
    if tail_size >= 10:
        k2 ^= ord(key[tail_index + 9]) << 8
    if tail_size >= 9:
        k2 ^= ord(key[tail_index + 8])

    if tail_size > 8:
        k2 = (k2 * c2) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 << 33 | k2 >> 31) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k2 = (k2 * c1) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2

    if tail_size >= 8:
        k1 ^= ord(key[tail_index + 7]) << 56
    if tail_size >= 7:
        k1 ^= ord(key[tail_index + 6]) << 48
    if tail_size >= 6:
        k1 ^= ord(key[tail_index + 5]) << 40
    if tail_size >= 5:
        k1 ^= ord(key[tail_index + 4]) << 32
    if tail_size >= 4:
        k1 ^= ord(key[tail_index + 3]) << 24
    if tail_size >= 3:
        k1 ^= ord(key[tail_index + 2]) << 16
    if tail_size >= 2:
        k1 ^= ord(key[tail_index + 1]) << 8
    if tail_size >= 1:
        k1 ^= ord(key[tail_index + 0])

    if tail_size > 0:
        k1 = (k1 * c1) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 << 31 | k1 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k1 = (k1 * c2) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1

    # finalization
    h1 ^= length
    h2 ^= length

    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF

    h1 = fmix(h1)
    h2 = fmix(h2)

    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF

    if signed:
        return unsigned2signed(h1), unsigned2signed(h2)
    return h1, h2


def repartition(df: DataFrame, key: str, partition: int) -> RDD:
    """use mmh to repartition

    Args:
        df (DataFrame): parquet dataframe
        key (str): hash key
        partition (int): partition number

    Returns:
        RDD: repartitioned rdd
    """
    return df.rdd.map(lambda x: (x[key], x)).partitionBy(
        numPartitions=partition,
        partitionFunc=lambda _: hash64(_, seed=0, signed=False)[0],
    )


def type_transform(dtype: DataType) -> Tuple[FeatureStoreType, FeatureDataType]:
    """type of feature

    Args:
        dtype (DataType): data type to transform

    Raises:
        TypeError: not supported type

    Returns:
        Tuple[FeatureStoreType, FeatureDataType]: types of the features
    """
    if isinstance(dtype, (StringType, BinaryType)):
        return FeatureStoreType.TextType, FeatureDataType.StringListType
    elif isinstance(dtype, (IntegerType, LongType, ByteType, ShortType)):
        return FeatureStoreType.IntegerType, FeatureDataType.Int64ListType
    elif isinstance(dtype, (FloatType, DoubleType, DecimalType)):
        return FeatureStoreType.RealType, FeatureDataType.Float32ListType
    raise TypeError(f"{dtype} not supported")


def to_magicdb(
    df: DataFrame,
    key: str,
    partition: int,
    table: str,
    upload_func: Callable[[str, str], None],
    data_dir: str,
    meta_dir: str,
) -> None:
    """parquet to magicdb

    Args:
        df (DataFrame): data frame of parquet
        key (str): primary key
        partition (int): partition number
        table (str): table name
        upload_func (Callable[[str, str], None]): upload function
        data_dir (str): magicdb data directory
        meta_dir (str): magicdb meta directory
    """
    schema = df.schema
    items, features = [], []
    for field in schema.fields:
        stype, dtype = type_transform(field.dataType)
        features.append(
            {"column": field.name, "dtype": dtype.value, "stype": stype.value}
        )
        if field.name == key:
            items.append(f"{field.name} {stype.name} PRIMARY KEY NOT NULL")
        else:
            if not field.nullable:
                items.append(f"{field.name} {stype.name} NOT NULL")
            else:
                items.append(f"{field.name} {stype.name}")

    ddl = "CREATE TABLE %s (%s \n);" % (table, ",\n".join(items))
    dml = "INSERT INTO %s (%s) VALUES (%s);" % (
        table,
        ",".join(schema.names),
        ",".join(["?"] * len(schema.names)),
    )

    print(ddl, dml)
    rdd = repartition(df, key, partition)

    def func(splitIndex, iterator):
        output_dir = "/tmp/%s-%s" % (
            int(time.time()),
            "{:0>5d}".format(random.randint(0, 99999)),
        )
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)
        index_str = "{:0>5d}".format(splitIndex)
        local_path = os.path.join(output_dir, f"{index_str}.db")
        remote_path = os.path.join(data_dir, f"{index_str}.db")
        conn = sqlite3.connect(local_path)
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.execute("""PRAGMA synchronous = OFF;""")
        cur.execute("""PRAGMA journal_mode = OFF;""")
        rows = []
        for r in iterator:
            rows.append(r[1])
            if len(rows) > 1000:
                cur.executemany(dml, rows)
                rows = []
        if len(rows) > 0:
            cur.executemany(dml, rows)
        conn.commit()
        cur.close()
        conn.close()
        upload_func(local_path, remote_path)
        shutil.rmtree(output_dir)
        return [remote_path]

    files = rdd.mapPartitionsWithIndex(func).collect()
    data = {
        "name": table,
        "version": int(time.time()),
        "partitions": files,
        "features": features,
        "key": key,
    }
    local_meta = "/tmp/%s-%s-%s.json" % (
        int(time.time()),
        "{:0>5d}".format(random.randint(0, 99999)),
        table,
    )
    remote_mata = os.path.join(meta_dir, f"{table}.json")
    json.dump(data, open(local_meta, "w"))
    upload_func(local_meta, remote_mata)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True, help="input dir")
    parser.add_argument("--table", type=str, required=True, help="table name")
    parser.add_argument("--key", type=str, required=True, help="primary key of table")
    parser.add_argument("--partition", type=int, default=100, help="partition number")
    parser.add_argument("--data_dir", type=str, required=True, help="data dir")
    parser.add_argument("--meta_dir", type=str, required=True, help="meta dir")
    parser.add_argument("--cmd", type=str, required=True, help="upload command format")

    def upload_func(local, remote):
        os.system(f"{args.cmd % (local, remote)}")

    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("yarn")
        .appName(f"magicdb-cli-{args.table}")
        .getOrCreate()
    )

    df = spark.read.parquet(args.input)

    to_magicdb(
        df=df,
        key=args.key,
        partition=args.partition,
        table=args.table,
        upload_func=upload_func,
        data_dir=args.data_dir,
        meta_dir=args.meta_dir,
    )


if __name__ == "__main__":
    main()