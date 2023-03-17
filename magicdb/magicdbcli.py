#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb-cli
author: Julong
"""
from magicdb.magicdbListenerHandler import parse, set_engine_namespace
from magicdb.magicdbEtcdClient import MagicDBEtcdClient
import argparse
import sys,traceback
import etcd3

import cmd

class MagicDBCmd(cmd.Cmd):
    def __init__(self, etcd_client,stdin=None):
        super().__init__(stdin=stdin)
        self.prompt = 'magicdb> '
        self.multiline = False
        self.buffer = ''
        self.etcd_client = etcd_client
        
    def default(self, line):
        if not self.multiline:
            if line.strip().endswith(';'):
                self.execute(line.strip())
            else:
                self.multiline = True
                self.buffer = line.strip() + ' '
                self.prompt = '... '
        else:
            if line.strip().endswith(';'):
                self.multiline = False
                self.buffer += line.strip()
                self.execute(self.buffer)
                self.prompt = 'magicdb> '
            else:
                self.buffer += line.strip() + ' '

    def execute(self, sql):
        command = ""
        try:
            # 这里执行 SQL 语句
            print(f"Executing SQL: {sql}")
            line = sql.strip()
            command += " " + line
            parse(command, self.etcd_client)
        except etcd3.exceptions.Etcd3Exception:
            print("Etcd Exception Occur, Exit")
            exit(1)
        except Exception as e:
            #continue
            print("Exception: ", e)
            traceback.print_exc(file=sys.stdout)
            pass

    def do_exit(self, arg):
        """Exit magicdb."""
        print("Exiting magicdb.")
        return True
    
    def emptyline(self):
        # 如果用户输入了空行，则不做任何处理
        pass



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, required=True, help="etch host")
    parser.add_argument("--port", type=int, help="etcd port", default=2379)
    parser.add_argument("--password", type=str,
                        help="etcd password", default=None)
    parser.add_argument("--name", type=str, required=True, help="namespace")
    parser.add_argument('-e', '--exec', type=str, help='Execute command')
    parser.add_argument('-f', '--file', type=str, help='Execute command File')
    args = parser.parse_args()

    set_engine_namespace(args.name)
    host, port, passwd = args.host, args.port, args.password
    etcd_client = MagicDBEtcdClient(args.name, host, port, passwd)

    cli = MagicDBCmd(etcd_client)
    if args.exec:
        cli.execute(args.exec)
    elif args.file:
        with open(args.file, 'r') as fd:
            content = fd.read()
            cli.execute(content)
    else:
        cli = MagicDBCmd(etcd_client)
        cli.cmdloop()


if __name__ == '__main__':
    main()

