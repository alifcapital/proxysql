#!/usr/bin/env python3
"""Real StarRocks/ProxySQL differential integration tests; emits TAP13."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback

import pymysql


def sql(connection, statement, args=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, args)
        return cursor.fetchall()


def connect(endpoint, stream=False):
    options = dict(endpoint)
    options.update(autocommit=True, connect_timeout=10, read_timeout=120,
                   charset="utf8mb4", use_unicode=False)
    if stream:
        options["cursorclass"] = pymysql.cursors.SSCursor
    return pymysql.connect(**options)


def fingerprint(cursor):
    digest = hashlib.sha256()
    count = size = 0
    for row in cursor:
        count += 1
        for value in row:
            if value is None:
                digest.update(b"N")
            else:
                value = value if isinstance(value, bytes) else str(value).encode()
                digest.update(b"V" + struct.pack("!Q", len(value)) + value)
                size += len(value)
    return count, size, digest.hexdigest()


def eventually(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("condition did not become true before deadline")


def server_statuses(admin, config):
    return sql(admin, "SELECT hostname,port,status FROM mysql_servers WHERE hostgroup_id=%s", (config["hostgroup"],))


def primary_only(admin, config):
    sql(admin, "UPDATE mysql_servers SET status=CASE WHEN hostname=%s AND port=%s THEN 'ONLINE' "
        "ELSE 'OFFLINE_SOFT' END WHERE hostgroup_id=%s", (config["backend"]["host"], config["backend"]["port"], config["hostgroup"]))
    sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")


def query_counts(admin, config):
    return {(text_host.decode() if isinstance(text_host, bytes) else text_host, int(port)): int(count)
            for text_host, port, count in sql(admin, "SELECT srv_host,srv_port,Queries FROM stats_mysql_connection_pool WHERE hostgroup=%s", (config["hostgroup"],))}


def drain_primary(admin, config):
    sql(admin, "UPDATE mysql_servers SET status=CASE WHEN hostname=%s AND port=%s THEN 'OFFLINE_SOFT' "
        "ELSE 'ONLINE' END WHERE hostgroup_id=%s", (config["backend"]["host"], config["backend"]["port"], config["hostgroup"]))
    sql(admin, "LOAD MYSQL SERVERS TO RUNTIME")


def assert_migrated(admin, config, before):
    after = query_counts(admin, config)
    primary = (config["backend"]["host"], config["backend"]["port"])
    assert after.get(primary, 0) == before.get(primary, 0), ("drained FE still received queries", before, after)
    assert any(after.get(server, 0) > before.get(server, 0) for server in after if server != primary), ("other FE received no new queries", before, after)


class Suite:
    def __init__(self, config):
        self.c = config
        self.admin = connect(config["admin"])
        self.direct = connect(config["backend"])
        self.pid = int(config["proxysql_pid"])
        self.baseline_fds = self.spill_fds()

    def rss(self):
        for line in Path(f"/proc/{self.pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        raise AssertionError("Linux VmRSS unavailable")

    def spill_fds(self):
        result = {}
        for fd in Path(f"/proc/{self.pid}/fd").iterdir():
            try:
                target = os.readlink(fd)
                if self.c["spill_file_marker"] in target:
                    result[str(fd)] = fd.stat().st_size
            except FileNotFoundError:
                pass
        return result

    def used(self):
        return int(sql(self.admin, "SELECT COALESCE(SUM(ConnUsed),0) FROM "
                       "stats_mysql_connection_pool WHERE hostgroup=%s",
                       (self.c["hostgroup"],))[0][0])

    def backend_active(self):
        if not self.c.get("sticky_connection"):
            return self.used() > 0
        # A sticky client keeps its FE connection while it reads, so ConnUsed
        # stays above zero. The FE itself tells whether the query is still running.
        marker = self.c["result_query"][:64].encode()
        return any(row[5] == b"Query" and (row[9] or b"").startswith(marker)
                   for row in sql(self.direct, "SHOW PROCESSLIST"))

    def backends_idle(self):
        """No FE connection is busy with a fixture query.

        A multiplexed client gives its FE connection back to the pool, so ConnUsed is 0. A sticky
        client (and any client of the FE authentication) keeps it, so the FE tells whether a
        fixture query still runs. All fixture queries read TABLE(generate_series(...)).
        """
        if not self.c.get("sticky_connection"):
            return self.used() == 0
        return not any(row[5] == b"Query" and b"generate_series" in (row[9] or b"")
                       for row in sql(self.direct, "SHOW PROCESSLIST"))

    def cleanup(self):
        eventually(lambda: self.spill_fds() == self.baseline_fds)

    def slow_result(self, disconnect=False, pipeline=False, early_disconnect=False):
        query = self.c["result_query"]
        if not disconnect:
            with self.direct.cursor() as cursor:
                cursor.execute(query)
                expected = fingerprint(cursor)
            assert expected[1] >= self.c.get("minimum_result_bytes", 64 << 20), expected
        connection = connect(self.c["proxy"], stream=True)
        # Below two local TCP MSS segments Linux can retain a tiny rcv_ssthresh
        # even after increasing SO_RCVBUF, making the final drain take hours.
        connection._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        cursor = connection.cursor()
        before = self.rss()
        peak = before
        evidence = {}
        try:
            cursor.execute(query)
            if pipeline:
                payload = b"\x03SELECT 999"
                connection._sock.sendall(struct.pack("<I", len(payload))[:3] + b"\x00" + payload)
            def completed_on_backend():
                nonlocal peak, evidence
                peak = max(peak, self.rss())
                fds = self.spill_fds()
                if any(size > 0 for fd, size in fds.items() if fd not in self.baseline_fds):
                    evidence = fds
                return bool(evidence) and (self.backend_active() if early_disconnect else not self.backend_active())
            eventually(completed_on_backend, self.c.get("timeout", 120))
            assert peak - before <= self.c.get("maximum_rss_growth_bytes", 32 << 20), (before, peak)
            print("# spill evidence " + json.dumps({"fds": evidence, "rss_before": before,
                                                   "rss_peak": peak}), flush=True)
            if not disconnect:
                connection._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
                actual = fingerprint(cursor)
                assert actual == expected, (actual, expected)
                if pipeline:
                    connection._next_seq_id = 1
                    connection._read_query_result(unbuffered=True)
                    cursor._do_get_result()
                    assert fingerprint(cursor) == (1, 3, hashlib.sha256(b"V" + struct.pack("!Q", 3) + b"999").hexdigest()), "pipelined response order/content changed"
                print("# ordered result " + json.dumps(actual), flush=True)
        finally:
            # SSCursor.close() drains unread results; raw close tests abrupt disconnect.
            if connection._sock is not None:
                try:
                    connection._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            if cursor._result is not None:
                cursor._result.unbuffered_active = False
            cursor.connection = None
            connection._force_close()
            self.cleanup()
            if early_disconnect:
                eventually(lambda: self.used() == 0)

    def driver_result(self, variant):
        import mysql.connector
        fingerprints = []
        for endpoint in (self.c["backend"], self.c["proxy"]):
            options = {"autocommit": True, "connection_timeout": 10, "use_pure": True,
                       "ssl_disabled": True}
            if endpoint is self.c["proxy"]:
                if variant == "compressed":
                    options["compress"] = True
                if variant == "tls":
                    options.update(self.c["tls_options"])
                    options["ssl_disabled"] = False
            connection = mysql.connector.connect(**endpoint, **options)
            try:
                if variant == "tls":
                    if endpoint is self.c["proxy"]:
                        assert isinstance(connection._socket.sock, __import__("ssl").SSLSocket), "TLS not negotiated"
                if variant == "compressed" and endpoint is self.c["proxy"]:
                    assert connection._socket._netbroker.__class__.__name__ == "NetworkBrokerCompressed", "compression not negotiated"
                cursor = connection.cursor(prepared=variant == "prepared")
                query = self.c["result_query"]
                if variant == "multi_results":
                    query += "; SELECT 17, 'final-result'"
                if variant == "late_error":
                    query += "; SELECT definitely_missing_starrocks_test_column"
                cursor.execute(query)
                resultsets = []
                while True:
                    resultsets.append(fingerprint(cursor))
                    try:
                        if not cursor.nextset():
                            break
                    except mysql.connector.Error as error:
                        if variant != "late_error":
                            raise
                        resultsets.append(("error", error.errno))
                        break
                fingerprints.append(resultsets)
                cursor.close()
            finally:
                connection.close()
        assert fingerprints[0] == fingerprints[1], fingerprints
        if variant == "multi_results":
            assert len(fingerprints[0]) == 2, fingerprints
        if variant == "late_error":
            assert len(fingerprints[0]) == 2 and fingerprints[0][-1][0] == "error", fingerprints
        self.cleanup()

    def disk_failure(self, variable, value, remove_after_load=False):
        previous = sql(self.admin, "SELECT variable_value FROM global_variables WHERE variable_name=%s",
                       (variable,))[0][0]
        connection = None
        try:
            sql(self.admin, "UPDATE global_variables SET variable_value=%s WHERE variable_name=%s",
                (str(value), variable))
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            if remove_after_load:
                Path(value).rmdir()
            connection = connect(self.c["proxy"], stream=True)
            failed = False
            try:
                cursor = connection.cursor()
                cursor.execute(self.c["result_query"])
                # Keep frontend blocked so an actual small filesystem fills.
                time.sleep(2)
                fingerprint(cursor)
            except (pymysql.OperationalError, pymysql.InterfaceError) as error:
                failed = True
                print("# explicit disk failure " + repr(error.args), flush=True)
            assert failed, "large result unexpectedly reported success with unavailable spill disk"
        finally:
            if connection is not None:
                if connection._result is not None:
                    connection._result.unbuffered_active = False
                connection._force_close()
            sql(self.admin, "UPDATE global_variables SET variable_value=%s WHERE variable_name=%s",
                (previous, variable))
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            self.cleanup()
        control = connect(self.c["proxy"])
        try:
            assert sql(control, "SELECT 1") == ((1,),)
        finally:
            control.close()

    def missing_directory(self):
        directory = tempfile.mkdtemp(prefix="proxysql-removed-after-load-", dir=self.c.get("fault_directory"))
        self.disk_failure("mysql-resultset_spill_directory", directory, remove_after_load=True)

    def invalid_directory_setting(self):
        variable = "mysql-resultset_spill_directory"
        previous = sql(self.admin, "SELECT variable_value FROM runtime_global_variables WHERE variable_name=%s", (variable,))[0][0]
        try:
            sql(self.admin, "UPDATE global_variables SET variable_value='/nonexistent/proxysql-spill-test' WHERE variable_name=%s", (variable,))
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            assert sql(self.admin, "SELECT variable_value FROM runtime_global_variables WHERE variable_name=%s", (variable,))[0][0] == previous
        finally:
            sql(self.admin, "UPDATE global_variables SET variable_value=%s WHERE variable_name=%s", (previous, variable))
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")

    def filesystem_failure(self, kind):
        directory = Path(tempfile.mkdtemp(prefix="proxysql-spill-fault-",
                                          dir=self.c["fault_directory"]))
        mounted = False
        try:
            if kind == "enospc":
                subprocess.run(["sudo", "mount", "-t", "tmpfs", "-o",
                                f"size=16m,uid={os.getuid()},gid={os.getgid()}",
                                "tmpfs", str(directory)], check=True)
                mounted = True
            else:
                assert os.geteuid() != 0, "EACCES requires an unprivileged ProxySQL/test user"
                directory.chmod(0o555)
            self.disk_failure("mysql-resultset_spill_directory", str(directory))
        finally:
            if mounted:
                subprocess.run(["sudo", "umount", str(directory)], check=True)
            directory.chmod(0o700)
            directory.rmdir()

    def oversized_cache(self):
        names = ("mysql-resultset_spill_enabled", "mysql-threshold_resultset_size")
        previous = {name: sql(self.admin, "SELECT variable_value FROM global_variables WHERE variable_name=%s", (name,))[0][0] for name in names}
        rule_id = 941000001
        assert not sql(self.admin, "SELECT rule_id FROM mysql_query_rules WHERE rule_id=%s", (rule_id,))
        connection = None
        def hits():
            return int(sql(self.admin, "SELECT Variable_Value FROM stats_mysql_global WHERE Variable_Name='Query_Cache_count_GET_OK'")[0][0])
        try:
            sql(self.admin, "INSERT INTO mysql_query_rules(rule_id,active,username,match_pattern,cache_ttl,apply) VALUES(%s,1,%s,%s,600000,1)",
                (rule_id, self.c["proxy"]["user"], "^" + re.escape(self.c["result_query"]) + "$"))
            sql(self.admin, "LOAD MYSQL QUERY RULES TO RUNTIME")
            sql(self.admin, "UPDATE global_variables SET variable_value='false' WHERE variable_name='mysql-resultset_spill_enabled'")
            sql(self.admin, "UPDATE global_variables SET variable_value='268435456' WHERE variable_name='mysql-threshold_resultset_size'")
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            connection = connect(self.c["proxy"], stream=True)
            cursor = connection.cursor()
            cursor.execute(self.c["result_query"])
            expected = fingerprint(cursor)
            before = hits()
            cursor.execute(self.c["result_query"])
            assert fingerprint(cursor) == expected
            assert hits() > before, "large result was not actually cached"
            connection.close()
            connection = None
            sql(self.admin, "UPDATE global_variables SET variable_value='true' WHERE variable_name='mysql-resultset_spill_enabled'")
            sql(self.admin, "UPDATE global_variables SET variable_value='1048576' WHERE variable_name='mysql-threshold_resultset_size'")
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            before = hits()
            self.slow_result()
            assert hits() == before, "oversized cache entry was copied instead of using bounded spill"
        finally:
            if connection is not None:
                connection.close()
            for name, value in previous.items():
                sql(self.admin, "UPDATE global_variables SET variable_value=%s WHERE variable_name=%s", (value, name))
            sql(self.admin, "LOAD MYSQL VARIABLES TO RUNTIME")
            sql(self.admin, "DELETE FROM mysql_query_rules WHERE rule_id=%s", (rule_id,))
            sql(self.admin, "LOAD MYSQL QUERY RULES TO RUNTIME")
            sql(self.admin, "PROXYSQL FLUSH QUERY CACHE")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--only", help="regular expression matching test descriptions")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    suite = Suite(config)
    tests = [("slow reader exact ordered result, disk spill, bounded RSS, FE released", suite.slow_result),
             ("abrupt slow reader disconnect removes spill files", lambda: suite.slow_result(True)),
             ("pipelined commands preserve spilled result order", lambda: suite.slow_result(pipeline=True)),
             ("disconnect during active backend spill cleans workers and files", lambda: suite.slow_result(True, early_disconnect=True))]
    if config.get("disk_failure_tests", True):
        tests.extend([
            ("disk quota failure is explicit and cleans files", lambda: suite.disk_failure("mysql-resultset_spill_limit", 1)),
            ("spill directory removed after LOAD fails safely", suite.missing_directory),
            ("invalid directory setting preserves previous runtime configuration", suite.invalid_directory_setting)])
    if config.get("fault_directory"):
        tests.extend([("actual filesystem ENOSPC fails explicitly and recovers", lambda: suite.filesystem_failure("enospc")),
                      ("actual EACCES creation failure cleans and recovers", lambda: suite.filesystem_failure("eacces"))])
    if config.get("cache_test", True):
        tests.append(("oversized existing cache entry uses bounded real spill", suite.oversized_cache))
    for variant in config.get("protocol_variants", []):
        assert variant in ("prepared", "compressed", "tls", "multi_results", "late_error"), variant
        tests.append((variant + " ordered result matches direct FE", lambda v=variant: suite.driver_result(v)))
    if args.only:
        tests = [(name, test) for name, test in tests if re.search(args.only, name)]
        assert tests, "--only selected no tests"
    print("TAP version 13\n1.." + str(len(tests)), flush=True)
    failures = 0
    for number, (name, test) in enumerate(tests, 1):
        try:
            test()
            print(f"ok {number} - {name}", flush=True)
        except Exception:
            failures += 1
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
            print(f"not ok {number} - {name}", flush=True)
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
