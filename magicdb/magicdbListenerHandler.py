#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb
author: rand
"""
import time
from multiprocessing import cpu_count

import antlr4
import requests
from magicdb.magicdbEtcdClient import MagicDBEtcdClient
from magicdb.magicdbLexer import magicdbLexer
from magicdb.magicdbListener import magicdbListener
from magicdb.magicdbLoad import to_magicdb
from magicdb.magicdbParser import magicdbParser

ENGINE_NAMESPACE = ""


def set_engine_namespace(namespace: str):
    ENGINE_NAMESPACE = namespace


def get_engine_namespace() -> str:
    return ENGINE_NAMESPACE


class MagicDBListenerHandler(magicdbListener):
    def __init__(self, etcd_client: MagicDBEtcdClient) -> None:
        super().__init__()
        self.etcd_client = etcd_client
        self.stack = []

    def exitDrop_database(self, ctx: magicdbParser.Drop_databaseContext):
        db = ctx.database_name().getText()
        _, msg = self.etcd_client.drop_database(db)
        print(msg)

    def exitShow_databases(self, ctx: magicdbParser.Show_databasesContext):
        print("exitShow_databases")
        databases = self.etcd_client.show_databases()
        print("database list: ")
        print("[" + "\n".join(map(lambda _: f"`{_}`", databases)) + "]")

    def exitCreate_database(self, ctx: magicdbParser.Create_databaseContext):
        db = ctx.database_name().getText()
        properties = self.stack.pop(-1)
        _, msg = self.etcd_client.create_database(db, properties)
        print(msg)

    def exitShow_machines(self, ctx: magicdbParser.Show_machinesContext):
        database = ctx.database_name().getText()
        machines = self.etcd_client.show_machines(database)
        print("machine list: ")
        print("[" + "\n".join(map(lambda _: f"`{_}`", machines)) + "]")

    def exitDelete_machine(self, ctx: magicdbParser.Delete_machineContext):
        db = ctx.database_name().getText()
        machine = ctx.STRING().getText()[1:-1]
        _, msg = self.etcd_client.delete_machine(db, machine)
        print(msg)

    def exitAdd_machine(self, ctx: magicdbParser.Add_machineContext):
        database = ctx.database_name().getText()
        machine = ctx.STRING().getText()[1:-1]
        _, msg = self.etcd_client.add_machine(database, machine)
        print(msg)

    def exitShow_tables(self, ctx: magicdbParser.Show_tablesContext):
        database = ctx.database_name().getText()
        tables = self.etcd_client.show_tables(database)
        print("table list: ")
        print("[" + "\n".join(map(lambda _: f"`{_}`", tables)) + "]")

    def exitDrop_table(self, ctx: magicdbParser.Drop_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        _, msg = self.etcd_client.drop_table(database, table)
        print(msg)

    def exitCreate_table(self, ctx: magicdbParser.Create_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        db, table = items[0], items[1]
        properties = self.stack.pop(-1)
        _, msg = self.etcd_client.create_table(db, table, properties)
        print(msg)

    def exitDesc_table(self, ctx: magicdbParser.Desc_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        info = self.etcd_client.get_table_info(database, table)
        print(info)

    def exitShow_versions(self, ctx: magicdbParser.Show_versionsContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        versions = self.etcd_client.show_versions(database, table)
        print("version list: ")
        print("[" + "\n".join(map(lambda _: f"`{_}`", versions)) + "]")

    def exitShow_current_version(self, ctx: magicdbParser.Show_current_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        version = self.etcd_client.show_current_version(database, table)
        print(f"current version: `{version}`")

    def exitUpdate_version(self, ctx: magicdbParser.Update_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        version = ctx.STRING().getText()[1:-1]
        _, msg = self.etcd_client.update_current_version(
            database, table, version)
        print(msg)

    def exitDrop_version(self, ctx: magicdbParser.Drop_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        version = ctx.STRING().getText()[1:-1]
        _, msg = self.etcd_client.drop_version(database, table, version)
        print(msg)

    def exitLoad_data(self, ctx: magicdbParser.Load_dataContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        remote_path = ctx.STRING().getText()
        properties = self.stack.pop(-1)
        db_info = self.etcd_client.get_db_info(database=database)
        table_info = self.etcd_client.get_table_info(
            database=database, table=table)

        boto3_kwargs = {
            "aws_access_key_id": db_info["access_key"],
            "aws_secret_access_key": db_info["access_key"],
            "aws_secret_access_key": db_info["secret_key"],
            "region_name": db_info["region"],
        }

        version = to_magicdb(
            work_dir=properties.get(
                "workdir", "/tmp/magicdb/%s/%s/%d" % (
                    database, table, int(time.time()))
            ),
            worker=properties.get("workers", max(cpu_count() - 1, 1)),
            hive_table_dir=remote_path,
            s3_data_dir=table_info["data"],
            s3_meta_dir=table_info["meta"],
            table_name=table,
            key_name=properties["key"],
            partitions=properties.get("partitions", 100),
            endpoint=db_info["endpoint"],
            **boto3_kwargs,
        )

        self.etcd_client.add_version(database, table, version)
        self.etcd_client.update_current_version(database, table, version)

    def exitSelect_data(self, ctx: magicdbParser.Select_dataContext):
        table_str = ctx.table().getText()
        items = table_str.split(".")
        database, table = items[0], items[1]
        key = ctx.STRING().getText()
        field = ctx.VARNAME().getText()

        machines = self.etcd_client.show_machines(database=database)
        if len(machines) == 0:
            print("err! table:`%s.%s` not serving" % (database, table))
            return
        table_info = self.etcd_client.get_table_info(
            database=database, table=table)
        if table_info["key"] != field:
            print("err! not support select record using filed: %s" % field)
            return
        url = "http://%s:%d/api/get" % (machines[0], 9528)
        value = requests.post(url, json={"key": key})
        print(value["features"])

    def exitProperties(self, ctx: magicdbParser.PropertiesContext):
        properties = {}
        while len(self.stack) > 0:
            pair = self.stack.pop(-1)
            properties.update(pair)
        self.stack.append(properties)
        return properties

    def exitPair(self, ctx: magicdbParser.PairContext):
        key = ctx.STRING().getText()
        value = ctx.value().getText()
        pair = eval("{%s:%s}" % (key, value))
        self.stack.append(pair)
        return pair


def parse(command: str, etcd_client: MagicDBEtcdClient):
    lexer = magicdbLexer(antlr4.InputStream(command))
    stream = antlr4.CommonTokenStream(lexer)
    parser = magicdbParser(stream)
    walker = antlr4.ParseTreeWalker()
    tree = parser.start()
    client = MagicDBListenerHandler(etcd_client)
    walker.walk(client, tree)


if __name__ == "__main__":
    drop_database = "drop database if exists database1;"
    create_database = 'create database if not exists database1 with properties("access_key" = "access_key","secret_key" = "secret_key","bucket"="bucket","endpoint"="endpoint","cloud"="s3","region"="region");'
    show_databases = "show databases;"
    add_machine = 'alter database database1 add machine("10.0.0.3");'
    show_machines = "show machines database1;"
    del_machine_1 = 'alter database database1 drop machine("10.0.0.4");'
    del_machine_2 = 'alter database database1 drop machine("10.0.0.3");'
    show_tables_1 = "show tables database1;"
    show_tables_2 = "show tables database2;"
    create_table = 'create table database1.table1 with properties("data_dir"="data_path","meta_dir"="meta_path", "key"="key";'
    drop_table = "drop table if exists database1.table1;"

    show_versions_1 = "show versions database1.table1;"
    show_versions_2 = "show versions database1.table2;"
    show_current_versions = "show current version database1.table1;"
    load_data = 'load data "oss://xxxx/dt=20221124/" into table database1.table1 with PROPERTIES("k1" = "v1","k2" = "v2");'

    drop_version_1 = 'alter table database1.table1 drop version("version1");'
    drop_version_2 = 'alter table database1.table1 drop version("version2");'
    desc_table = "desc database1.table1;"
    etcd_client = MagicDBEtcdClient("test", "192.168.1.5", 2379)

    parse(drop_database, etcd_client)
    parse(show_databases, etcd_client)
    parse(create_database, etcd_client)
    parse(create_database, etcd_client)
    parse(show_databases, etcd_client)
    parse(show_machines, etcd_client)
    parse(add_machine, etcd_client)
    parse(show_machines, etcd_client)
    parse(del_machine_1, etcd_client)
    parse(del_machine_1, etcd_client)
    parse(del_machine_2, etcd_client)
    parse(show_machines, etcd_client)
    parse(show_tables_1, etcd_client)
    parse(show_tables_2, etcd_client)
    parse(create_table, etcd_client)
    parse(show_tables_1, etcd_client)

    # parse(drop_table, etcd_client)
    parse(show_versions_1, etcd_client)
    parse(show_versions_2, etcd_client)
    parse(show_current_versions, etcd_client)
    #parse(load_data, etcd_client)
    parse(show_current_versions, etcd_client)
    parse(show_versions_1, etcd_client)

    parse(drop_version_2, etcd_client)
    parse(drop_version_1, etcd_client)
    parse(show_databases, etcd_client)
    parse(desc_table, etcd_client)
