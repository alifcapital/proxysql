#!/usr/bin/env python3
"""Verify that a client keeps its FE connection, and leaves a drained FE safely.

The FE authenticates the clients, so a session cannot move to another FE. When the
old FE is set to OFFLINE_SOFT, new logins go to the ONLINE FE, and every session of
the old FE, with its running request and its open transaction, stays there until the
client disconnects.

Every case starts with config["backend"] as the only server of the hostgroup.
config["new_fe"] is a second FE of the same cluster (host and port) that the
case adds to the hostgroup when it switches. The original rows of the hostgroup
are restored at the end.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import socket
import threading
import traceback

from e2e import connect, sql, eventually

DATABASE = "sticky_switch_test"
# Runs on the compute nodes until it is stopped.
HEAVY_QUERY = "SELECT count(*) FROM TABLE(generate_series(1,100000000000)) t WHERE generate_series % 7 = 3"


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


def closed_by_proxysql(connection, timeout=20):
    """True when ProxySQL closes the client connection without a new command from the client."""
    connection._sock.settimeout(timeout)
    try:
        return connection._sock.recv(1) == b""
    except (ConnectionResetError, socket.timeout):
        return False


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
        # Other clients keep FE connections busy; the client must stay on its own one.
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

    def switch_keeps_sessions_on_the_old_fe(self):
        client = connect(self.config["proxy"])
        idle = connect(self.config["proxy"])
        old = (self.config["backend"]["host"], self.config["backend"]["port"])
        new = (self.config["new_fe"]["host"], self.config["new_fe"]["port"])
        try:
            client_fe, idle_fe = physical_id(client), physical_id(idle)
            query = "SELECT sleep(8), 'running-on-old-fe'"
            assert pool_used(self.admin, self.config, *old) == 2
            with ThreadPoolExecutor(max_workers=1) as pool:
                running = pool.submit(sql, client, query)
                eventually(lambda: any(row[5] == b"Query" and (row[9] or b"").startswith(b"SELECT sleep(8)")
                                       for row in sql(self.oracle, "SHOW PROCESSLIST")), 5)
                switch_to_new_fe(self.admin, self.config)
                assert not running.done(), "the query ended before the switch; nothing was tested"
                result = running.result(timeout=60)
                assert len(result) == 1 and result[0][1:] == (b"running-on-old-fe",), ("the running query lost its result", result)
            # OFFLINE_SOFT only sends new logins elsewhere: both sessions go on with their FE connections.
            assert not closed_by_proxysql(idle, timeout=3), "ProxySQL closed the idle client of the drained FE"
            assert physical_id(client) == client_fe, "the client left its FE connection"
            assert physical_id(idle) == idle_fe, "the idle client left its FE connection"
            assert pool_used(self.admin, self.config, *old) == 2
            # A new connection lands on the ONLINE FE.
            newcomer = connect(self.config["proxy"])
            try:
                assert sql(newcomer, "SELECT 'new-client'") == ((b"new-client",),)
                assert pool_used(self.admin, self.config, *new) == 1
            finally:
                newcomer.close()
            # The old FE is free when its clients leave by themselves.
            client.close()
            idle.close()
            eventually(lambda: pool_used(self.admin, self.config, *old) == 0, 10)
        finally:
            for c in (client, idle):
                if c.open:
                    c.close()

    def transaction_finishes_on_the_old_fe(self):
        sql(self.oracle, "CREATE DATABASE IF NOT EXISTS " + DATABASE)
        sql(self.oracle, "CREATE TABLE IF NOT EXISTS %s.t (id INT) PROPERTIES('replication_num'='1')" % DATABASE)
        sql(self.oracle, "TRUNCATE TABLE %s.t" % DATABASE)
        client = connect(self.config["proxy"])
        try:
            fe = physical_id(client)
            sql(client, "USE " + DATABASE)
            sql(client, "BEGIN")
            sql(client, "INSERT INTO t VALUES (1)")
            switch_to_new_fe(self.admin, self.config)
            # The session stays on its FE, so its transaction goes on there and commits.
            sql(client, "INSERT INTO t VALUES (2)")
            sql(client, "COMMIT")
            assert physical_id(client) == fe, "the session left its FE connection"
            assert sql(self.oracle, "SELECT COUNT(*) FROM %s.t" % DATABASE) == ((2,),)
        finally:
            if client.open:
                client.close()
            sql(self.oracle, "DROP DATABASE IF EXISTS %s FORCE" % DATABASE)

    def kill_on_the_old_fe(self):
        """After a switch, Ctrl+C of a session that stays on the old FE stops its query."""
        old = (self.config["backend"]["host"], self.config["backend"]["port"])
        new = (self.config["new_fe"]["host"], self.config["new_fe"]["port"])
        victim = connect(self.config["proxy"])
        killer = None
        outcome = {}

        def run():
            try:
                outcome["rows"] = sql(victim, HEAVY_QUERY)
            except Exception as error:
                outcome["error"] = error

        thread = threading.Thread(target=run, daemon=True)
        try:
            thread.start()
            eventually(lambda: any(row[5] == b"Query" and b"generate_series(1,100000000000)" in (row[9] or b"")
                                   for row in sql(self.oracle, "SHOW PROCESSLIST")), 10)
            switch_to_new_fe(self.admin, self.config)
            # The mysql client and Connector/J cancel over a new connection, which lands on the ONLINE FE.
            killer = connect(self.config["proxy"])
            assert pool_used(self.admin, self.config, *new) == 1
            assert not sql(killer, "KILL QUERY %d" % victim.thread_id()), "the KILL answered with rows"
            thread.join(30)
            assert not thread.is_alive(), "the query on the old FE was not stopped"
            assert "error" in outcome, outcome
            eventually(lambda: not any(b"generate_series(1,100000000000)" in (row[9] or b"")
                                       for row in sql(self.oracle, "SHOW PROCESSLIST")), 10)
            eventually(lambda: pool_used(self.admin, self.config, *old) == 0, 10)
        finally:
            for connection in (killer, victim):
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass

    def run(self):
        original = saved_servers(self.admin, self.config)
        try:
            cases = (("client keeps its FE connection between statements", self.connection_stays),
                     ("after a switch the running query finishes, both sessions stay on the old FE, and a new client lands on the ONLINE FE",
                      self.switch_keeps_sessions_on_the_old_fe),
                     ("an open transaction finishes and commits on the old FE after a switch", self.transaction_finishes_on_the_old_fe),
                     ("after a switch, Ctrl+C of a session on the old FE stops its query", self.kill_on_the_old_fe))
            for name, function in cases:
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
