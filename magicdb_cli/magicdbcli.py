#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb-cli
author: Julong
"""
from magicdb_cli.magicdbLexer import magicdbLexer
from magicdb_cli.magicdbListenerHandler import parse, set_engine_namespace
from magicdb_cli.magicdbEtcdClient import MagicDBEtcdClient
import argparse
import sys, traceback
import etcd3
import cmd
import readline
import os.path
import atexit


class MagicDBCmd(cmd.Cmd):
    def __init__(self, etcd_client):
        super().__init__()
        self.prompt = 'magicdbcli> '
        self.multiline = False
        self.buffer = ''
        self.etcd_client = etcd_client

        self.hist_file = os.path.expanduser('~/.magicdbcli_history')
        self.hist_file_size = 1000
        self.keywords = [eval(name) for name in magicdbLexer().literalNames[1:]]


    def preloop(self):
        if readline and os.path.exists(self.hist_file):
            readline.read_history_file(self.hist_file)
            atexit.register(readline.write_history_file, self.hist_file)
        if 'libedit' in readline.__doc__:
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")

    def postloop(self):
        if readline:
            readline.set_history_length(self.hist_file_size)
            readline.write_history_file(self.hist_file)

    def completedefault(self, *args):
        text = args[0].upper()
        return [a for a in self.keywords if a.startswith(text)]

    def completenames(self, text, *ignored):
        dotext = 'do_' + text
        inline_cmd = [a[3:] for a in self.get_names() if a.startswith(dotext)]
        text = text.upper()
        keywords = [a for a in self.keywords if a.startswith(text)]
        if len(inline_cmd) > 0:
            inline_cmd.extend(keywords)
            return inline_cmd
        else:
            return keywords

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
                self.prompt = 'magicdbcli> '
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
            # continue
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
