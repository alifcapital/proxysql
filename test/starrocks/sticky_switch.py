#!/usr/bin/env python3
"""Verify that a client keeps its FE connection, and leaves a drained FE safely.

The hostgroup must have the attribute multiplex=0 and backend_type=starrocks.
Every case starts with config["backend"] as the only server of the hostgroup.
config["new_fe"] is a second FE of the same cluster (host and port) that the
case adds to the hostgroup when it switches. The original rows of the hostgroup
are restored at the end.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import traceback

from e2e import connect, sql, eventually

DATABASE = "sticky_switch_test"


def physical_id(connection):
    # "+0" keeps ProxySQL from answering CONNECTION_ID() itself.
    return sql(connection, "SELECT CONNECTION_ID()+0")[0][0]


def pool_used(admin, config, host, port):
    rows = sql(admin, "SELECT COALESCE(SUM(ConnUsed),0) FROM stats_mysql_connection_pool "
               "WHERE hostgroup=%s AND srv_host=%s AND srv_port=%s", (config["hostgroup"], host, port))
    return int(rows[0][0])


def only_backend(admin, config):
    sql(admin, "DELETE FROM mysql_servers WHERE hostgroup_id=%s AND NOT (hostname=%s AND port=%s)",
        (config["hostgroup"], config["backend"]["host"], config["backend"]["port"]))
    sql(admin, "UPDATE mysql_servers SET status='ONLINE' WHERE hostgroup_id=%s", (config["hostgroup"],))
    sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")


def switch_to_new_fe(admin, config):
    sql(admin, "INSERT INTO mysql_servers(hostgroup_id,hostname,port,max_connections,status) "
        "VALUES(%s,%s,%s,50,'ONLINE')", (config["hostgroup"], config["new_fe"]["host"], config["new_fe"]["port"]))
    sql(admin, "UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostgroup_id=%s AND hostname=%s AND port=%s",
        (config["hostgroup"], config["backend"]["host"], config["backend"]["port"]))
    sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")


def saved_servers(admin, config):
    rows = sql(admin, "SELECT hostgroup_id,hostname,port,gtid_port,weight,status,compression,max_connections,"
               "max_replication_lag,use_ssl,max_latency_ms,comment FROM mysql_servers WHERE hostgroup_id=%s",
               (config["hostgroup"],))
    return [tuple(value.decode() if isinstance(value, bytes) else value for value in row) for row in rows]


def restore_servers(admin, config, rows):
    sql(admin, "DELETE FROM mysql_servers WHERE hostgroup_id=%s", (config["hostgroup"],))
    for row in rows:
        sql(admin, "INSERT INTO mysql_servers(hostgroup_id,hostname,port,gtid_port,weight,status,compression,"
            "max_connections,max_replication_lag,use_ssl,max_latency_ms,comment) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
    sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")


def old_fe_released(admin, config):
    return pool_used(admin, config, config["backend"]["host"], config["backend"]["port"]) == 0


class Suite:
    def __init__(self, config):
        self.config = config
        self.admin = connect(config["admin"])
        self.oracle = connect(config["backend"])
        self.results = []

    def case(self, name, function):
        try:
            function()
            self.results.append((True, name))
        except Exception:
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
            self.results.append((False, name))

    def connection_stays(self):
        # Other clients keep the pool busy, so a multiplexed statement would change connections.
        clients = [connect(self.config["proxy"]) for _ in range(6)]
        try:
            with ThreadPoolExecutor(max_workers=len(clients)) as pool:
                list(pool.map(lambda c: sql(c, "SELECT sleep(1)"), clients))
            client = connect(self.config["proxy"])
            try:
                first = physical_id(client)
                for _ in range(30):
                    for other in clients:
                        sql(other, "SELECT 1")
                    assert physical_id(client) == first, "statement ran on another FE connection"
            finally:
                client.close()
        finally:
            for c in clients:
                c.close()

    def switch_with_running_query(self):
        client = connect(self.config["proxy"])
        idle = connect(self.config["proxy"])
        try:
            query = "SELECT sleep(8), 'running-on-old-fe'"
            old_id = physical_id(client)
            idle_old_id = physical_id(idle)
            thread_id, idle_thread_id = client.thread_id(), idle.thread_id()
            with ThreadPoolExecutor(max_workers=1) as pool:
                running = pool.submit(sql, client, query)
                eventually(lambda: pool_used(self.admin, self.config, self.config["backend"]["host"],
                                             self.config["backend"]["port"]) >= 2, 5)
                switch_to_new_fe(self.admin, self.config)
                assert not running.done(), "the query ended before the switch; nothing was tested"
                result = running.result(timeout=60)
                assert len(result) == 1 and result[0][1:] == (b"running-on-old-fe",), ("the running query lost its result", result)
            # The running client released its connection when the query ended; the idle one has not sent anything.
            eventually(lambda: pool_used(self.admin, self.config, self.config["backend"]["host"],
                                         self.config["backend"]["port"]) == 1, 5)
            assert physical_id(client) != old_id, "the next statement stayed on the old FE"
            assert physical_id(idle) != idle_old_id, "the idle client's next statement stayed on the old FE"
            eventually(lambda: old_fe_released(self.admin, self.config), 5)
            assert (client.thread_id(), idle.thread_id()) == (thread_id, idle_thread_id)
            client.ping(reconnect=False)
            idle.ping(reconnect=False)
        finally:
            client.close()
            idle.close()

    def transaction_is_not_moved(self):
        sql(self.oracle, "CREATE DATABASE IF NOT EXISTS " + DATABASE)
        sql(self.oracle, "CREATE TABLE IF NOT EXISTS %s.t (id INT) PROPERTIES('replication_num'='1')" % DATABASE)
        sql(self.oracle, "TRUNCATE TABLE %s.t" % DATABASE)
        client = connect(self.config["proxy"])
        try:
            sql(client, "USE " + DATABASE)
            old_id = physical_id(client)
            sql(client, "BEGIN")
            sql(client, "INSERT INTO t VALUES (1)")
            switch_to_new_fe(self.admin, self.config)
            sql(client, "INSERT INTO t VALUES (2)")
            assert physical_id(client) == old_id, "the session moved in the middle of a transaction"
            assert pool_used(self.admin, self.config, self.config["backend"]["host"],
                             self.config["backend"]["port"]) == 1
            sql(client, "COMMIT")
            eventually(lambda: sql(self.oracle, "SELECT id FROM %s.t ORDER BY id" % DATABASE) == ((1,), (2,)), 30)
            eventually(lambda: old_fe_released(self.admin, self.config), 5)
            assert physical_id(client) != old_id, "the session stayed on the old FE after COMMIT"
        finally:
            client.close()
            sql(self.oracle, "DROP DATABASE IF EXISTS %s FORCE" % DATABASE)

    def run(self):
        original = saved_servers(self.admin, self.config)
        try:
            for name, function in (("client keeps its FE connection between statements", self.connection_stays),
                                   ("running query survives an FE switch and the idle sessions leave the old FE", self.switch_with_running_query),
                                   ("open transaction stays on the old FE until COMMIT", self.transaction_is_not_moved)):
                only_backend(self.admin, self.config)
                self.case(name, function)
        finally:
            restore_servers(self.admin, self.config, original)
            self.oracle.close()
            self.admin.close()
        print("TAP version 13\n1..%d" % len(self.results))
        for number, (passed, name) in enumerate(self.results, 1):
            print("%s %d - %s" % ("ok" if passed else "not ok", number, name))
        return all(passed for passed, _ in self.results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    raise SystemExit(0 if Suite(json.loads(args.config.read_text())).run() else 1)
