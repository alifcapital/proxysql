#!/usr/bin/env python3
"""A compressed result that ProxySQL spills must reach the client whole; emits TAP13.

The result spill hands the response to the client in blocks of any size, and each block
becomes one compressed packet. When such a packet ended inside the 4 byte header of a MySQL
packet, mysql-connector-python failed with "unpack requires a buffer of 4 bytes". The query
below returns about a million rows of one short column, so many blocks end inside a header
(with rows of 4 KB this happened in one run of 30). Without the fix this fails in every run.
"""
import argparse
import json
from pathlib import Path
import sys
import traceback

import mysql.connector

from e2e import fingerprint

QUERY = "SELECT generate_series FROM TABLE(generate_series(1, 1000000))"
ROW_COUNT = 1000000


def result(endpoint, compress):
    options = dict(endpoint)
    options.update(autocommit=True, connection_timeout=60, use_pure=True, ssl_disabled=True, compress=compress)
    connection = mysql.connector.connect(**options)
    try:
        if compress:
            assert connection._socket._netbroker.__class__.__name__ == "NetworkBrokerCompressed", "compression not negotiated"
        cursor = connection.cursor()
        cursor.execute(QUERY)
        return fingerprint(cursor)
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--repeat", type=int, default=3)
    arguments = parser.parse_args()
    config = json.loads(arguments.config.read_text())
    expected = result(config["backend"], False)
    assert expected[0] == ROW_COUNT, expected
    print("TAP version 13\n1..%d" % arguments.repeat, flush=True)
    failures = 0
    for number in range(1, arguments.repeat + 1):
        try:
            actual = result(config["proxy"], True)
            assert actual == expected, (actual, expected)
            print("ok %d - compressed spilled result of short rows is whole" % number, flush=True)
        except Exception:
            failures += 1
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
            print("not ok %d - compressed spilled result of short rows is whole" % number, flush=True)
    sys.exit(bool(failures))


if __name__ == "__main__":
    main()
