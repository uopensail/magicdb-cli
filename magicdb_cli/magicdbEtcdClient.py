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

# magicdb client to update ectd keys

import json
from typing import List, Tuple

import etcd3


class MagicDBEtcdClient:
    def __init__(self, namespace: str, host: str, port: int, passwd: str = None) -> None:
        self.client = etcd3.Etcd3Client(
            host=host, port=port, password=passwd, timeout=5)
        print(self.client.status())
        self.namespace = namespace
        self.prefix_name = "magicdb/%s/storage" % namespace
        self.locker = "magicdb/%s/storage/locker" % namespace

    def db_key(self, database: str) -> str:
        return "/%s/databases/%s" % (self.prefix_name, database)

    def machine_key(self, machine: str) -> str:
        return "/magicdb/storage/machines/%s" % (machine)

    def table_key(self, database: str, table: str) -> str:
        return "/%s/databases/%s/%s" % (self.prefix_name, database, table)

    def check_database(self, database: str) -> bool:
        resp = self.client.get_response(key=self.db_key(database))
        return resp.count >= 1

    def create_database(self, database: str, properties: dict) -> Tuple[bool, str]:
        values = json.dumps(
            {
                "name": database,
                "bucket": properties["bucket"],
                "endpoint": properties["endpoint"],
                "region": properties["region"],
                "access_key": properties["access_key"],
                "secret_key": properties["secret_key"],
                "machines": [],
                "tables": [],
            }
        )
        status, msg = True, "success"
        with self.client.lock(self.locker, ttl=10):
            if self.check_database(database):
                status, msg = False, "database:`%s` exists" % database
            else:
                self.client.put(key=self.db_key(database), value=values)
        return status, msg

    def drop_database(self, database: str) -> Tuple[bool, str]:
        db_key = self.db_key(database)
        status, msg = True, "success"
        with self.client.lock(self.locker, ttl=10):
            if not self.check_database(database):
                status, msg = False, "database:`%s` not exists" % database
            else:
                self.client.delete_prefix(prefix=db_key)
        return status, msg

    def get_db_info(self, database: str):
        if not self.check_database(database):
            return {}
        value, _ = self.client.get(self.db_key(database))
        return json.loads(value) if value is not None else {}

    def show_databases(self) -> List[str]:
        db_prefix = "/%s/databases/" % self.prefix_name
        values = self.client.get_prefix(db_prefix)
        dbs = []
        for value in values:
            key = value[1].key.decode("utf8", "ignore")
            tmp = key[len(db_prefix):]
            if tmp.find("/") < 0:
                dbs.append(tmp)
        return dbs

    def check_machine(self, database: str, machine: str) -> bool:
        if not self.check_database(database):
            return False
        resp = self.client.get_response(key=self.machine_key(machine))
        return resp.count >= 1

    def show_machines(self, database: str) -> List[str]:
        info = self.get_db_info(database)
        return info.get("machines", [])

    def add_machine(self, database: str, machine: str) -> Tuple[bool, str]:
        machine_key = self.machine_key(machine)
        db_key = self.db_key(database)
        status, msg = True, "success"
        with self.client.lock(self.locker, ttl=10):
            if not self.check_database(database):
                status, msg = False, f"database:`%s` not exists" % database
            else:
                db_info = self.get_db_info(database)
                if machine not in db_info["machines"]:
                    db_info["machines"].append(machine)
                self.client.transaction(
                    compare=[],
                    success=[
                        self.client.transactions.put(
                            db_key, json.dumps(db_info)),
                        self.client.transactions.put(key=machine_key, value=json.dumps(
                            {"database": database, "namespace": self.namespace}))
                    ],
                    failure=[]
                )
        return status, msg

    def delete_machine(self, database: str, machine: str) -> Tuple[bool, str]:
        machine_key = self.machine_key(machine)
        db_key = self.db_key(database)
        status, msg = True, "success"
        with self.client.lock(self.locker, ttl=10):
            if not self.check_machine(database, machine):
                status, msg = False, f"database:`%s` machine: `%s` not exists" % (
                    database,
                    machine,
                )
            else:
                db_info = self.get_db_info(database)
                if machine in db_info["machines"]:
                    db_info["machines"].remove(machine)
                self.client.transaction(
                    compare=[],
                    success=[
                        self.client.transactions.delete(machine_key),
                        self.client.transactions.put(
                            db_key, json.dumps(db_info))
                    ],
                    failure=[]
                )

        return status, msg

    def check_table(self, database: str, table: str) -> bool:
        if not self.check_database(database):
            print(f"database:`{database}` not exists")
            return False
        resp = self.client.get_response(key=self.table_key(database, table))
        return resp.count >= 1

    def drop_table(self, database: str, table: str) -> Tuple[bool, str]:
        db_key = self.db_key(database)
        table_key = self.table_key(database, table)
        status, msg = True, "success"
        with self.client.lock(self.locker, ttl=10):
            if not self.check_table(database, table):
                status, msg = False, "table:`%s.%s` not exists" % (
                    database, table)
            else:
                db_info = self.get_db_info(database)
                if table in db_info["tables"]:
                    db_info["tables"].remove(table)
                self.client.transaction(
                    compare=[],
                    success=[
                        self.client.transactions.put(
                            db_key, json.dumps(db_info)),
                        self.client.transactions.delete(table_key)
                        # self.client.transactions.delete_prefix(prefix=table_key)
                    ],
                    failure=[]
                )

        return status, msg

    def get_table_info(self, database: str, table: str) -> dict:
        if not self.check_table(database, table):
            return {}
        value, _ = self.client.get(self.table_key(database, table))
        return json.loads(value) if value is not None else {}

    def show_tables(self, database: str) -> List[str]:
        info = self.get_db_info(database)
        return info.get("tables", [])

    def create_table(
        self, database: str, table: str, properties: dict
    ) -> Tuple[bool, str]:
        status, msg = True, "success"
        db_key = self.db_key(database)
        table_key = self.table_key(database, table)
        values = json.dumps(
            {
                "name": table,
                "database": database,
                "data": properties["data_dir"],
                "meta": properties["meta_dir"],
                "current": "nil",
                "versions": [],
                "partitions": properties.get("partitions", 100),
                "key": properties["key"],
            }
        )
        with self.client.lock(self.locker, ttl=10):
            if self.check_table(database, table):
                status, msg = False, "table:`%s.%s` exists" % (database, table)
            elif not self.check_database(database):
                status, msg = False, "database:`%s` not exists" % database
            else:
                db_info = self.get_db_info(database)
                if table not in db_info["tables"]:
                    db_info["tables"].append(table)

                    self.client.transaction(
                        compare=[],
                        success=[
                            self.client.transactions.put(
                                db_key, json.dumps(db_info)),
                            self.client.transactions.put(
                                key=table_key,
                                value=values,
                            )
                        ],
                        failure=[]
                    )
                else:
                    self.client.put(
                        key=table_key,
                        value=values,
                    )
        return status, msg

    def show_versions(self, database: str, table: str) -> List[str]:
        info = self.get_table_info(database, table)
        return info.get("versions", [])

    def show_current_version(self, database: str, table: str) -> str:
        info = self.get_table_info(database, table)
        return info.get("current_version", "nil")

    def add_version(self, database: str, table: str, version: str) -> Tuple[bool, str]:
        status, msg = True, "success"
        table_key = self.table_key(database, table)
        with self.client.lock(self.locker, ttl=10):
            if not self.check_table(database, table):
                status, msg = False, "table:`%s.%s` not exists" % (
                    database, table)
            else:
                table_info = self.get_table_info(database, table)
                if version not in table_info["versions"]:
                    table_info["versions"].append(version)
                    self.client.put(table_key, json.dumps(table_info))
                else:
                    status, msg = False, "version:%s exists" % (version)
        return status, msg

    def update_current_version(
        self, database: str, table: str, version: str
    ) -> Tuple[bool, str]:
        status, msg = True, "success"
        table_key = self.table_key(database, table)
        with self.client.lock(self.prefix_name, ttl=10):
            table_info = self.get_table_info(database, table)
            if version not in table_info["versions"]:
                status, msg = False, "version:%s not exists" % version
            else:
                table_info["current_version"] = version
                self.client.put(table_key, json.dumps(table_info))
        return status, msg

    def add_update_version(self, database: str, table: str, version: str
                           ) -> Tuple[bool, str]:
        status, msg = True, "success"
        table_key = self.table_key(database, table)
        with self.client.lock(self.locker, ttl=10):
            if not self.check_table(database, table):
                status, msg = False, "table:`%s.%s` not exists" % (
                    database, table)
            else:
                table_info = self.get_table_info(database, table)
                if version not in table_info["versions"]:
                    table_info["versions"].append(version)
                    table_info["current_version"] = version
                    self.client.put(table_key, json.dumps(table_info))
                else:
                    status, msg = False, "version:%s exists" % (version)
        return status, msg

        pass

    def drop_version(self, database: str, table: str, version: str) -> Tuple[bool, str]:
        status, msg = True, "success"
        table_key = self.table_key(database, table)
        with self.client.lock(self.locker, ttl=10):
            table_info = self.get_table_info(database, table)
            if version in table_info["versions"]:
                table_info["versions"].remove(version)
            else:
                status, msg = False, "version:%s not exists" % version
            if version == table_info["current"]:
                table_info["current"] = "nil"
            self.client.put(table_key, json.dumps(table_info))
        return status, msg
