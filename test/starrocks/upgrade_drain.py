#!/usr/bin/env python3
"""Verify an in-flight query survives FE drain and both new/old clients use the other FE."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import traceback

from e2e import (connect, sql, server_statuses, primary_only, drain_primary,
                 query_counts, assert_migrated, eventually)


def run(config):
    admin, oracle = connect(config["admin"]), connect(config["backend"])
    statuses = server_statuses(admin, config)
    client = newcomer = None
    query = "SELECT sleep(6), 'in-flight-old-fe'"
    try:
        primary_only(admin, config)
        client = connect(config["proxy"])
        expected = sql(oracle, query)
        with ThreadPoolExecutor(max_workers=1) as pool:
            running = pool.submit(sql, client, query)

            def old_fe_busy():
                rows = sql(admin, "SELECT COALESCE(SUM(ConnUsed),0) FROM stats_mysql_connection_pool "
                           "WHERE hostgroup=%s AND srv_host=%s AND srv_port=%s",
                           (config["hostgroup"], config["backend"]["host"], config["backend"]["port"]))
                return int(rows[0][0]) > 0 and not running.done()

            eventually(old_fe_busy, 4)
            drain_primary(admin, config)
            assert not running.done(), "query finished before the drain; test did not exercise in-flight routing"
            before = query_counts(admin, config)
            newcomer = connect(config["proxy"])
            assert sql(newcomer, "SELECT 'new-client-on-new-fe'") == ((b"new-client-on-new-fe",),)
            assert_migrated(admin, config, before)
            assert running.result(timeout=30) == expected, "old FE response was lost or changed during drain"
            before = query_counts(admin, config)
            assert sql(client, "SELECT 'existing-client-on-new-fe'") == ((b"existing-client-on-new-fe",),)
            assert_migrated(admin, config, before)
        print("# old query completed; new and existing frontend connections used the other FE", flush=True)
    finally:
        if newcomer is not None:
            newcomer.close()
        if client is not None:
            client.close()
        for host, port, status in statuses:
            sql(admin, "UPDATE mysql_servers SET status=%s WHERE hostgroup_id=%s AND hostname=%s AND port=%s",
                (status, config["hostgroup"], host, port))
        sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")
        oracle.close()
        admin.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    print("TAP version 13\n1..1", flush=True)
    try:
        run(json.loads(args.config.read_text()))
        print("ok 1 - in-flight FE drain preserves the query and the frontend connection", flush=True)
    except Exception:
        for line in traceback.format_exc().splitlines():
            print("# " + line, flush=True)
        print("not ok 1 - in-flight FE drain", flush=True)
        raise SystemExit(1)
