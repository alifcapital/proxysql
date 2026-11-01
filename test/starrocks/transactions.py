#!/usr/bin/env python3
"""Statements around explicit transactions must always get an answer; emits TAP13.

StarRocks never sets the autocommit bit in the status flags of its OK packets. A
ProxySQL that compares that bit with the autocommit of the session sends SET autocommit
again and again, and the client waits for ever. Each sequence below ran into that
(a user variable, or SET autocommit=0, before an explicit transaction). It works with
the sticky setup of setup.py and with --fe-auth.
"""
import argparse
import json
from pathlib import Path
import sys
import traceback

import pymysql

from e2e import sql

SEQUENCES = {
    "transaction": ["BEGIN", "SELECT 1", "COMMIT", "SELECT 2"],
    "user variable": ["SET @px_x = 5", "SELECT 3", "SELECT 4"],
    "user variable and COMMIT": ["SET @px_x = 5", "BEGIN", "SELECT 5", "COMMIT", "SELECT 6"],
    "user variable and ROLLBACK": ["SET @px_x = 5", "BEGIN", "SELECT 5", "ROLLBACK", "SELECT 6"],
    "autocommit=0 and COMMIT": ["SET autocommit=0", "SELECT 1", "BEGIN", "SELECT 5", "COMMIT", "SELECT 6"],
    "autocommit=0 then 1": ["SET autocommit=0", "SELECT 1", "SET autocommit=1", "SELECT 2"],
    "autocommit=0 and repeated selects": ["SET autocommit=0", "SELECT 1", "SELECT 2", "SELECT 3"],
}


def run(config, statements):
    connection = pymysql.connect(autocommit=True, connect_timeout=10, read_timeout=15, charset="utf8mb4",
                                 use_unicode=False, **config["proxy"])
    try:
        for statement in statements:
            sql(connection, statement)
    finally:
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    config = json.loads(parser.parse_args().config.read_text())
    print("TAP version 13\n1..%d" % len(SEQUENCES), flush=True)
    failures = 0
    for number, (name, statements) in enumerate(SEQUENCES.items(), 1):
        try:
            run(config, statements)
            print("ok %d - %s" % (number, name), flush=True)
        except Exception:
            failures += 1
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
            print("not ok %d - %s" % (number, name), flush=True)
    sys.exit(bool(failures))
