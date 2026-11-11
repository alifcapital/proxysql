#!/usr/bin/env python3
"""StarRocks FE authentication through ProxySQL (mysql-starrocks_auth_hostgroup); emits TAP13.

ProxySQL holds no passwords in this mode. The FE authenticates every client, and the
FE connection that took the login belongs to that client until it disconnects.
The cases create their own FE users through config["backend"] (a user that can run
CREATE USER) and drop them at the end. ProxySQL must be configured with
`setup.py --fe-auth`.

    python3 fe_auth.py CONFIG [--connector-jar mysql-connector-j.jar] [--mysql-cli mysql]
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback

import pymysql
from pymysql.constants import COMMAND
from pymysql.protocol import FieldDescriptorPacket

from e2e import connect, eventually, sql


def field_list(connection, table):
    """Sends COM_FIELD_LIST and returns the name, type, length, flags and charset of each column."""
    connection._execute_command(COMMAND.COM_FIELD_LIST, table + b"\0")
    columns = []
    while True:
        packet = connection._read_packet()
        if packet.is_eof_packet():
            return columns
        field = FieldDescriptorPacket(packet.get_all_data(), connection.encoding)
        columns.append((field.name, field.type_code, field.length, field.flags, field.charsetnr))

COM_RESET_CONNECTION = 0x1f
# Runs on the compute nodes until it is stopped.
HEAVY_QUERY = "SELECT count(*) FROM TABLE(generate_series(1,100000000000)) t WHERE generate_series % 7 = 3"
HEAVY_MARKER = b"generate_series(1,100000000000)"
PASSWORD_USER = "px_fe_auth_pw"
PASSWORD = "Px-fe-auth-Pw-1"
EMPTY_USER = "px_fe_auth_empty"
LDAP_USER = "px_fe_auth_ldap"
USERS = (PASSWORD_USER, EMPTY_USER, LDAP_USER)


def login(config, user, password, **options):
    endpoint = dict(config["proxy"])
    endpoint.update(user=user, password=password)
    endpoint.update(options)
    return pymysql.connect(autocommit=True, connect_timeout=10, read_timeout=60,
                           charset="utf8mb4", use_unicode=False, **endpoint)


def direct_login(config, user, password, **options):
    endpoint = dict(config["backend"])
    endpoint.update(user=user, password=password)
    endpoint.update(options)
    return pymysql.connect(autocommit=True, connect_timeout=10, read_timeout=60,
                           charset="utf8mb4", use_unicode=False, **endpoint)


def login_error(function):
    try:
        function().close()
    except pymysql.err.OperationalError as error:
        return error.args
    raise AssertionError("the login was accepted")


def fe_connection_id(connection):
    # "+0" keeps ProxySQL from answering CONNECTION_ID() itself.
    return sql(connection, "SELECT CONNECTION_ID()+0")[0][0]


def fe_users(oracle):
    """FE connections by id: the user and the command of each one."""
    # The columns are ServerName, Id, User, ...
    return {int(row[1]): (row[2].decode() if isinstance(row[2], bytes) else row[2])
            for row in sql(oracle, "SHOW PROCESSLIST")}


def heavy_queries(oracle):
    """Ids of the FE connections that run the heavy query now."""
    return {int(row[1]) for row in sql(oracle, "SHOW PROCESSLIST")
            if row[5] == b"Query" and HEAVY_MARKER in (row[9] or b"")}


def cn_queries(oracle):
    """Ids of the FE connections whose query still runs on the compute nodes."""
    return {int(row[3]) for row in sql(oracle, "SHOW PROC '/current_queries'")}


def pool_counts(admin, config):
    rows = sql(admin, "SELECT COALESCE(SUM(ConnUsed),0), COALESCE(SUM(ConnFree),0) FROM "
               "stats_mysql_connection_pool WHERE hostgroup=%s", (config["hostgroup"],))
    return int(rows[0][0]), int(rows[0][1])


class Suite:
    def __init__(self, config, connector_jar, mysql_cli):
        self.config = config
        self.jar = connector_jar
        self.cli = mysql_cli
        self.admin = connect(config["admin"])
        self.oracle = connect(config["backend"])
        self.results = []
        self.seen_fe_ids = set()

    def case(self, name, function):
        try:
            skipped = function()
            self.results.append(("skip " + skipped if skipped else "ok", name))
        except Exception:
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
            self.results.append(("not ok", name))
        # A case must not leave a query on the FE for the cases that follow.
        time.sleep(1)
        running = heavy_queries(self.oracle)
        if running:
            print("# the heavy query still runs on the FE connections %s after: %s" % (sorted(running), name), flush=True)

    def create_users(self):
        for user in USERS:
            sql(self.oracle, "DROP USER IF EXISTS '%s'@'%%'" % user)
        sql(self.oracle, "CREATE USER '%s'@'%%' IDENTIFIED BY '%s'" % (PASSWORD_USER, PASSWORD))
        sql(self.oracle, "CREATE USER '%s'@'%%'" % EMPTY_USER)
        self.ldap_user = True
        try:
            sql(self.oracle, "CREATE USER '%s'@'%%' IDENTIFIED WITH authentication_ldap_simple AS 'uid=px,dc=example,dc=com'"
                % LDAP_USER)
        except pymysql.err.Error:
            self.ldap_user = False

    def drop_users(self):
        for user in USERS:
            try:
                sql(self.oracle, "DROP USER IF EXISTS '%s'@'%%'" % user)
            except pymysql.err.Error:
                pass

    # a) login

    def no_users_in_proxysql(self):
        assert int(sql(self.admin, "SELECT COUNT(*) FROM runtime_mysql_users")[0][0]) == 0, "mysql_users must be empty"
        value = sql(self.admin, "SELECT variable_value FROM global_variables WHERE variable_name='mysql-starrocks_auth_hostgroup'")
        assert int(value[0][0]) == self.config["hostgroup"], value

    def password_login(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            assert sql(connection, "SELECT 41+1") == ((42,),)
            assert PASSWORD_USER in sql(connection, "SELECT CURRENT_USER()")[0][0].decode()
            assert fe_users(self.oracle)[fe_connection_id(connection)] == PASSWORD_USER
        finally:
            connection.close()

    def wrong_password(self):
        through = login_error(lambda: login(self.config, PASSWORD_USER, "wrong"))
        direct = login_error(lambda: direct_login(self.config, PASSWORD_USER, "wrong"))
        assert through[0] == 1045 and through == direct, ("the FE error must reach the client unchanged", through, direct)
        assert not str(through[1]).startswith("ProxySQL"), through

    def unknown_user(self):
        through = login_error(lambda: login(self.config, "px_fe_auth_nobody", "x"))
        direct = login_error(lambda: direct_login(self.config, "px_fe_auth_nobody", "x"))
        assert through[0] == 1045 and through == direct, (through, direct)

    def empty_password(self):
        connection = login(self.config, EMPTY_USER, "")
        try:
            assert sql(connection, "SELECT 1") == ((1,),)
            assert fe_users(self.oracle)[fe_connection_id(connection)] == EMPTY_USER
        finally:
            connection.close()
        through = login_error(lambda: login(self.config, EMPTY_USER, "something"))
        direct = login_error(lambda: direct_login(self.config, EMPTY_USER, "something"))
        assert through[0] == 1045 and through == direct, (through, direct)

    def password_ending_with_zero_byte(self):
        # The native response is 20 random bytes. We cannot choose it, so we log in many times:
        # a response that ends with a 0 byte must be relayed whole.
        for _ in range(300):
            connection = login(self.config, PASSWORD_USER, PASSWORD)
            connection.close()

    def database_at_login(self):
        sql(self.oracle, "CREATE DATABASE IF NOT EXISTS px_fe_auth_db")
        try:
            connection = login(self.config, PASSWORD_USER, PASSWORD, database="information_schema")
            try:
                assert sql(connection, "SELECT DATABASE()") == ((b"information_schema",),)
            finally:
                connection.close()
            error = login_error(lambda: login(self.config, PASSWORD_USER, PASSWORD, database="px_no_such_database"))
            direct = login_error(lambda: direct_login(self.config, PASSWORD_USER, PASSWORD, database="px_no_such_database"))
            assert error == direct and "px_no_such_database" in error[1], ("the FE error about the database must reach the client", error, direct)
        finally:
            sql(self.oracle, "DROP DATABASE IF EXISTS px_fe_auth_db FORCE")

    def non_native_user(self):
        if not self.ldap_user:
            return "the FE could not create a user with the LDAP plugin"
        error = login_error(lambda: login(self.config, LDAP_USER, "x"))
        assert error[0] == 1045 and "mysql_native_password" in error[1], error

    # b) client variants

    def tls_login(self):
        context = {"check_hostname": False}
        connection = login(self.config, PASSWORD_USER, PASSWORD, ssl=context)
        try:
            assert isinstance(connection._sock, ssl.SSLSocket), "the client connection is not encrypted"
            assert sql(connection, "SELECT 'tls'") == ((b"tls",),)
        finally:
            connection.close()
        error = login_error(lambda: login(self.config, PASSWORD_USER, "wrong", ssl=context))
        assert error[0] == 1045 and not str(error[1]).startswith("ProxySQL"), error
        connection = login(self.config, EMPTY_USER, "", ssl=context)
        connection.close()

    def compression_login(self):
        import mysql.connector
        endpoint = dict(self.config["proxy"])
        endpoint.update(user=PASSWORD_USER, password=PASSWORD)
        for tls in (False, True):
            connection = mysql.connector.connect(**endpoint, use_pure=True, compress=True, ssl_disabled=not tls,
                                                 ssl_verify_cert=False, autocommit=True)
            try:
                assert connection._socket._netbroker.__class__.__name__ == "NetworkBrokerCompressed"
                cursor = connection.cursor()
                cursor.execute("SELECT REPEAT('c', 200000)")
                assert len(cursor.fetchall()[0][0]) == 200000
            finally:
                connection.close()
        endpoint["password"] = "wrong"
        try:
            mysql.connector.connect(**endpoint, use_pure=True, compress=True, ssl_disabled=True)
        except mysql.connector.Error as error:
            assert error.errno == 1045, error
        else:
            raise AssertionError("the login was accepted")

    def run_cli(self, user, password, *options):
        command = [self.cli, "-h", self.config["proxy"]["host"], "-P", str(self.config["proxy"]["port"]),
                   "-u", user, *options, "-e", "SELECT 'cli-ok'"]
        environment = dict(os.environ, MYSQL_PWD=password)
        return subprocess.run(command, env=environment, capture_output=True, text=True, timeout=60)

    def cli_login(self):
        if not shutil.which(self.cli):
            return "no mysql command line client"
        for options in (("--ssl-mode=DISABLED",), ("--ssl-mode=REQUIRED",), ("--ssl-mode=DISABLED", "--compress"),
                        ("--ssl-mode=REQUIRED", "--compress")):
            result = self.run_cli(PASSWORD_USER, PASSWORD, *options)
            assert result.returncode == 0 and "cli-ok" in result.stdout, (options, result.stdout, result.stderr)
        result = self.run_cli(PASSWORD_USER, "wrong", "--ssl-mode=DISABLED")
        assert result.returncode != 0 and "ERROR 1045 (28000): Access denied for user '%s'" % PASSWORD_USER in result.stderr, result.stderr

    def jdbc_login(self):
        if not self.jar:
            return "no Connector/J jar (--connector-jar)"
        source = Path(__file__).resolve().with_name("FeAuthJdbc.java")
        with tempfile.TemporaryDirectory(prefix="proxysql-fe-auth-jdbc-") as output:
            subprocess.run(["javac", "-d", output, str(source)], check=True)

            def run(user, password, variant):
                environment = dict(os.environ, PROXY_HOST=str(self.config["proxy"]["host"]),
                                   PROXY_PORT=str(self.config["proxy"]["port"]), JDBC_USER=user, JDBC_PASSWORD=password)
                return subprocess.run(["java", "-Xmx256m", "-cp", output + os.pathsep + str(Path(self.jar).resolve()),
                                       "FeAuthJdbc", variant], env=environment, capture_output=True, text=True, timeout=120)

            for variant in ("plain", "tls", "compressed", "tls-compressed", "prepared"):
                result = run(PASSWORD_USER, PASSWORD, variant)
                assert result.returncode == 0 and "jdbc-ok" in result.stdout, (variant, result.stdout, result.stderr)
                result = run(EMPTY_USER, "", variant)
                assert result.returncode == 0 and "jdbc-ok" in result.stdout, (variant, "empty password", result.stdout, result.stderr)
            result = run(PASSWORD_USER, "wrong", "plain")
            assert "error 1045" in result.stdout and "Access denied for user '%s'" % PASSWORD_USER in result.stdout, result.stdout

    def cli_auth_switch(self):
        """A client that answers with another plugin switches to mysql_native_password."""
        if not shutil.which(self.cli):
            return "no mysql command line client"
        result = self.run_cli(PASSWORD_USER, PASSWORD, "--ssl-mode=DISABLED", "--default-auth=caching_sha2_password")
        if "unknown option" in result.stderr or "Unknown option" in result.stderr or "plugin" in result.stderr and "not" in result.stderr:
            return "the command line client cannot use caching_sha2_password"
        assert result.returncode == 0 and "cli-ok" in result.stdout, (result.stdout, result.stderr)
        result = self.run_cli(PASSWORD_USER, "wrong", "--ssl-mode=DISABLED", "--default-auth=caching_sha2_password")
        assert result.returncode != 0 and "Access denied for user '%s'" % PASSWORD_USER in result.stderr, result.stderr
        # A cleartext response can never be checked by the FE against its scramble.
        result = self.run_cli(PASSWORD_USER, PASSWORD, "--ssl-mode=REQUIRED", "--default-auth=mysql_clear_password",
                              "--enable-cleartext-plugin")
        assert result.returncode != 0 and "ERROR" in result.stderr, (result.stdout, result.stderr)

    def session_state_stays(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            first = fe_connection_id(connection)
            assert not sql(connection, "SET @px_x = 5")
            assert sql(connection, "SELECT @px_x") == ((5,),)
            sql(connection, "USE information_schema")
            assert sql(connection, "SELECT DATABASE()") == ((b"information_schema",),)
            sql(connection, "BEGIN")
            assert sql(connection, "SELECT 1") == ((1,),)
            sql(connection, "COMMIT")
            assert sql(connection, "SELECT @px_x") == ((5,),)
            assert fe_connection_id(connection) == first
        finally:
            connection.close()

    def command_error(self, function):
        try:
            function()
        except pymysql.err.MySQLError as error:
            return error.args
        raise AssertionError("the command was accepted")

    def use_catalog_and_database(self):
        """USE `catalog`.`database` and COM_INIT_DB catalog.database run on the FE."""
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            first = fe_connection_id(connection)
            sql(connection, "USE `default_catalog`.`information_schema`")
            assert sql(connection, "SELECT DATABASE()") == ((b"information_schema",),)
            connection.select_db("default_catalog.information_schema")
            assert sql(connection, "SELECT DATABASE()") == ((b"information_schema",),)
            assert fe_connection_id(connection) == first
        finally:
            connection.close()

    def init_db_error_keeps_the_session(self):
        """The FE error on COM_INIT_DB reaches the client, and the session goes on in its database."""
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        direct = direct_login(self.config, PASSWORD_USER, PASSWORD)
        try:
            first = fe_connection_id(connection)
            sql(connection, "USE information_schema")
            through = self.command_error(lambda: connection.select_db("px_no_such_catalog`.x"))
            oracle = self.command_error(lambda: direct.select_db("px_no_such_catalog`.x"))
            assert through == oracle, ("the FE error must reach the client unchanged", through, oracle)
            assert sql(connection, "SELECT DATABASE()") == ((b"information_schema",),)
            assert fe_connection_id(connection) == first
        finally:
            connection.close()
            direct.close()

    def set_error_comes_with_the_set(self):
        """A SET that the FE refuses gets the FE error itself, and the next query runs."""
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        direct = direct_login(self.config, PASSWORD_USER, PASSWORD)
        try:
            through = self.command_error(lambda: sql(connection, "SET max_execution_time = 1000"))
            oracle = self.command_error(lambda: sql(direct, "SET max_execution_time = 1000"))
            assert through == oracle, ("the FE error must answer the SET", through, oracle)
            assert sql(connection, "SELECT 1") == ((1,),)
        finally:
            connection.close()
            direct.close()

    def set_catalog_then_use(self):
        """SET CATALOG resets the database on the FE, and USE of the same database sets it again."""
        statements = ("USE information_schema", "SET CATALOG default_catalog", "SELECT DATABASE()",
                      "USE information_schema", "SELECT DATABASE()")
        results = []
        for connect_to in (login, direct_login):
            connection = connect_to(self.config, PASSWORD_USER, PASSWORD)
            try:
                results.append([sql(connection, statement) for statement in statements])
            finally:
                connection.close()
        assert results[0] == results[1], ("the session must follow the FE", results)
        assert results[0][-1] == ((b"information_schema",),), results

    def answers_come_from_the_fe(self):
        """Queries that ProxySQL could answer itself get the answer of the FE."""
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        direct = direct_login(self.config, PASSWORD_USER, PASSWORD)
        try:
            for statement in ("SELECT @@version_comment LIMIT 1", "SELECT DATABASE()"):
                assert sql(connection, statement) == sql(direct, statement), statement
            assert b"@" in sql(connection, "select USER()")[0][0], "USER() of the FE has a host"
        finally:
            connection.close()
            direct.close()

    def ping_reaches_the_fe(self):
        """COM_PING is answered by the FE, so a ping finds out that the FE connection is gone."""
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            connection.ping(reconnect=False)
            identifier = fe_connection_id(connection)
            sql(self.oracle, "KILL %d" % identifier)
            eventually(lambda: identifier not in fe_users(self.oracle), 10)
            try:
                connection.ping(reconnect=False)
            except pymysql.err.Error:
                return None
            raise AssertionError("the ping succeeded after the FE connection ended")
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def field_list_comes_from_the_fe(self):
        """COM_FIELD_LIST lists the columns in the database of the session, as on a direct FE connection."""
        database = "px_fe_auth_fl"
        sql(self.oracle, "CREATE DATABASE IF NOT EXISTS %s" % database)
        try:
            sql(self.oracle, "CREATE TABLE IF NOT EXISTS %s.t (a INT, b VARCHAR(8) DEFAULT 'x') DUPLICATE KEY(a) "
                "DISTRIBUTED BY HASH(a) PROPERTIES('replication_num'='1')" % database)
            results = []
            for connect_to in (login, direct_login):
                connection = connect_to(self.config, self.config["proxy"]["user"], self.config["proxy"]["password"],
                                        database=database)
                try:
                    columns = field_list(connection, b"t")
                    error = self.command_error(lambda: field_list(connection, b"px_no_such_table"))
                    assert sql(connection, "SELECT 1") == ((1,),), "the session goes on after the error"
                    results.append((columns, error))
                finally:
                    connection.close()
            assert results[0] == results[1], ("COM_FIELD_LIST must answer as the FE", results)
            assert [column[0] for column in results[0][0]] == ["a", "b"], results
        finally:
            sql(self.oracle, "DROP DATABASE IF EXISTS %s FORCE" % database)

    def prepared_statements_follow_the_database(self):
        """A statement prepared twice in two databases runs as on a direct FE connection."""
        import mysql.connector
        databases = ("px_fe_auth_ps1", "px_fe_auth_ps2")
        for database in databases:
            sql(self.oracle, "CREATE DATABASE IF NOT EXISTS %s" % database)
        try:
            sql(self.oracle, "CREATE TABLE IF NOT EXISTS %s.t (a INT) DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) "
                "PROPERTIES('replication_num'='1')" % databases[0])
            sql(self.oracle, "CREATE TABLE IF NOT EXISTS %s.t (a INT, b VARCHAR(8)) DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) "
                "PROPERTIES('replication_num'='1')" % databases[1])
            sql(self.oracle, "INSERT INTO %s.t VALUES (1)" % databases[0])
            sql(self.oracle, "INSERT INTO %s.t VALUES (2, 'two')" % databases[1])
            results = []
            for endpoint in (self.config["proxy"], self.config["backend"]):
                options = dict(endpoint)
                options.update(user=self.config["proxy"]["user"], password=self.config["proxy"]["password"])
                connection = mysql.connector.connect(**options, use_pure=True, ssl_disabled=True, autocommit=True)
                try:
                    run = []
                    first = connection.cursor(prepared=True)
                    connection.cmd_query("USE %s" % databases[0])
                    first.execute("SELECT * FROM t")
                    run.append(first.fetchall())
                    connection.cmd_query("USE %s" % databases[1])
                    second = connection.cursor(prepared=True)
                    second.execute("SELECT * FROM t")
                    run.append(second.fetchall())
                    first.execute("SELECT * FROM t")
                    run.append(first.fetchall())
                    first.close()
                    second.close()
                    results.append(run)
                finally:
                    connection.close()
            assert results[0] == results[1], ("prepared statements must run as on the FE", results)
        finally:
            for database in databases:
                sql(self.oracle, "DROP DATABASE IF EXISTS %s FORCE" % database)

    def disconnects_during_login(self):
        """A client that leaves before the FE login ends does not leave an FE connection behind."""
        host, port = self.config["proxy"]["host"], self.config["proxy"]["port"]
        for _ in range(30):
            sock = socket.create_connection((host, port), timeout=10)
            sock.recv(4096)     # the greeting; the FE connection waits for the response
            sock.close()
        for _ in range(30):
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()        # gone before the greeting
        for _ in range(30):
            sock = socket.create_connection((host, port), timeout=10)
            sock.recv(4096)
            sock.sendall(b"\x05\x00\x00\x01garbage"[:6])   # a broken response
            sock.close()
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 20)
        eventually(lambda: len([p for p in fe_users(self.oracle).values() if p == "root"]) <= 1, 20)
        assert login(self.config, PASSWORD_USER, PASSWORD).close() is None

    # e) no reuse of FE connections

    def one_fe_connection_per_client(self):
        clients = [login(self.config, PASSWORD_USER, PASSWORD), login(self.config, PASSWORD_USER, PASSWORD),
                   login(self.config, EMPTY_USER, "")]
        try:
            ids = [fe_connection_id(client) for client in clients]
            assert len(set(ids)) == 3, ids
            users = fe_users(self.oracle)
            assert [users[i] for i in ids] == [PASSWORD_USER, PASSWORD_USER, EMPTY_USER], users
            used, free = pool_counts(self.admin, self.config)
            assert (used, free) == (3, 0), ("every client holds its FE connection and nothing is pooled", used, free)
            self.seen_fe_ids.update(ids)
            for client, expected in zip(clients, ids):
                for _ in range(5):
                    assert fe_connection_id(client) == expected, "a statement ran on another FE connection"
            clients[0].close()
            eventually(lambda: ids[0] not in fe_users(self.oracle), 10)
            assert pool_counts(self.admin, self.config) == (2, 0), "the connection of a closed client went to the pool"
            replacement = login(self.config, PASSWORD_USER, PASSWORD)
            try:
                new_id = fe_connection_id(replacement)
                assert new_id not in self.seen_fe_ids, "a new client got an FE connection that another client had used"
                self.seen_fe_ids.add(new_id)
            finally:
                replacement.close()
        finally:
            for client in clients:
                if client.open:
                    client.close()
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 10)

    def no_reuse_over_many_logins(self):
        seen = set()
        for index in range(40):
            user, password = (PASSWORD_USER, PASSWORD) if index % 2 else (EMPTY_USER, "")
            client = login(self.config, user, password)
            try:
                identifier = fe_connection_id(client)
                assert identifier not in seen, "an FE connection served two clients"
                assert fe_users(self.oracle)[identifier] == user
                seen.add(identifier)
                assert pool_counts(self.admin, self.config)[1] == 0
            finally:
                client.close()
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 10)

    # f) commands that would change the user of the FE connection

    def change_user_error(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            before = fe_connection_id(connection)
            payload = b"root\x00" + b"\x00" + b"\x00" + b"\x2d\x00"
            connection._execute_command(COMMAND.COM_CHANGE_USER, payload)
            try:
                connection._read_packet()
            except pymysql.err.Error as error:
                assert error.args[0] == 1235 and "COM_CHANGE_USER" in error.args[1], error.args
            else:
                raise AssertionError("COM_CHANGE_USER was accepted")
            assert fe_connection_id(connection) == before, "the session changed its FE connection"
            assert fe_users(self.oracle)[before] == PASSWORD_USER, "the FE connection changed its user"
        finally:
            connection.close()

    def reset_connection_error(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            before = fe_connection_id(connection)
            connection._execute_command(COM_RESET_CONNECTION, b"")
            try:
                connection._read_packet()
            except pymysql.err.Error as error:
                assert error.args[0] == 1235 and "COM_RESET_CONNECTION" in error.args[1], error.args
            else:
                raise AssertionError("COM_RESET_CONNECTION was accepted")
            assert fe_connection_id(connection) == before
        finally:
            connection.close()

    # FE connection loss

    def lost_fe_connection_closes_client(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        try:
            identifier = fe_connection_id(connection)
            sql(self.oracle, "KILL %d" % identifier)
            eventually(lambda: identifier not in fe_users(self.oracle), 10)
            # ProxySQL closes the client without waiting for its next command.
            connection._sock.settimeout(15)
            assert connection._sock.recv(1) == b"", "the client connection stayed open after its FE connection ended"
        finally:
            try:
                connection.close()
            except Exception:
                pass
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 10)

    # KILL and cancel of a running query

    def start_heavy_query(self, user=PASSWORD_USER, password=PASSWORD):
        """A client that runs the heavy query in a thread. Returns the client, its FE id and the outcome dict."""
        client = login(self.config, user, password)
        fe_id = fe_connection_id(client)
        outcome = {}

        def run():
            try:
                outcome["rows"] = sql(client, HEAVY_QUERY)
            except Exception as error:
                outcome["error"] = error

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        eventually(lambda: fe_id in heavy_queries(self.oracle), 10)
        outcome["thread"] = thread
        return client, fe_id, outcome

    def kill_query_from_second_connection(self):
        """What the mysql client does on Ctrl+C: KILL QUERY id over a new connection of the same user."""
        victim, fe_id, outcome = self.start_heavy_query()
        try:
            killer = login(self.config, PASSWORD_USER, PASSWORD)
            try:
                assert not sql(killer, "KILL QUERY %d" % victim.thread_id()), "the FE answered with rows"
            finally:
                killer.close()
            outcome["thread"].join(30)
            assert not outcome["thread"].is_alive(), "the query was not interrupted"
            assert "error" in outcome and outcome["error"].args[0] in (1317, 5000, 1105, 1064), outcome
            eventually(lambda: fe_id not in heavy_queries(self.oracle), 10)
            eventually(lambda: fe_id not in cn_queries(self.oracle), 10)
            # KILL QUERY leaves the connection of the victim alive.
            assert sql(victim, "SELECT 5") == ((5,),)
            assert fe_connection_id(victim) == fe_id
        finally:
            victim.close()

    def kill_connection_from_second_connection(self):
        victim, fe_id, outcome = self.start_heavy_query()
        try:
            killer = login(self.config, PASSWORD_USER, PASSWORD)
            try:
                sql(killer, "KILL %d" % victim.thread_id())
            finally:
                killer.close()
            eventually(lambda: fe_id not in fe_users(self.oracle), 15)
            outcome["thread"].join(30)
            assert not outcome["thread"].is_alive() and "error" in outcome, outcome
            eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 15)
        finally:
            try:
                victim.close()
            except Exception:
                pass

    def kill_of_another_user_gets_the_fe_error(self):
        victim, fe_id, outcome = self.start_heavy_query()
        try:
            other = login(self.config, EMPTY_USER, "")
            direct = direct_login(self.config, EMPTY_USER, "")
            try:
                errors = []
                for connection, target in ((other, victim.thread_id()), (direct, fe_id)):
                    try:
                        sql(connection, "KILL QUERY %d" % target)
                    except pymysql.err.Error as error:
                        errors.append(error.args)
                assert len(errors) == 2, ("the FE accepted the KILL of another user", errors)
                assert errors[0] == errors[1], ("the error of the FE must reach the client unchanged", errors)
            finally:
                other.close()
                direct.close()
            assert fe_id in heavy_queries(self.oracle), "the query of the other user was killed"
        finally:
            # The owner stops its own query, so the test needs no more than the feature.
            killer = login(self.config, PASSWORD_USER, PASSWORD)
            try:
                sql(killer, "KILL QUERY %d" % victim.thread_id())
            finally:
                killer.close()
            outcome["thread"].join(30)
            victim.close()

    def kill_of_unknown_id(self):
        connection = login(self.config, PASSWORD_USER, PASSWORD)
        direct = direct_login(self.config, PASSWORD_USER, PASSWORD)
        try:
            errors = []
            for conn in (connection, direct):
                try:
                    sql(conn, "KILL QUERY 4000000000")
                except pymysql.err.Error as error:
                    errors.append(error.args)
            assert len(errors) == 2 and errors[0] == errors[1], errors
        finally:
            connection.close()
            direct.close()

    def fe_of(self, connection):
        """Host and port of the FE that ProxySQL connected for this client."""
        rows = sql(self.admin, "SELECT srv_host, srv_port FROM stats_mysql_processlist WHERE SessionID=%s",
                   (connection.thread_id(),))
        return tuple(rows[0]) if rows else None

    def login_on_other_fe(self, user, password, fe):
        """A client of the user whose FE connection is not on the FE given."""
        for _ in range(40):
            connection = login(self.config, user, password)
            if self.fe_of(connection) != fe:
                return connection
            connection.close()
        raise AssertionError("no login landed on the other FE")

    def kill_across_two_fes(self):
        """KILL by connection id is local to an FE. ProxySQL ends a session of the same user on another FE itself."""
        rows = sql(self.admin, "SELECT hostname,port,weight FROM mysql_servers WHERE hostgroup_id=%s", (self.config["hostgroup"],))
        assert len(rows) == 2, "the hostgroup needs two servers (setup.py adds the second one)"
        sql(self.admin, "UPDATE mysql_servers SET weight=1000000 WHERE hostgroup_id=%s", (self.config["hostgroup"],))
        sql(self.admin, "LOAD MYSQL SERVERS TO RUNTIME")
        victim, fe_id, outcome = self.start_heavy_query()
        killer = other = None
        try:
            victim_fe = self.fe_of(victim)
            other = self.login_on_other_fe(EMPTY_USER, "", victim_fe)
            killer = self.login_on_other_fe(PASSWORD_USER, PASSWORD, victim_fe)
            # ProxySQL cannot check the privilege of the FE to kill the session of another user.
            refused = self.command_error(lambda: sql(other, "KILL QUERY %d" % victim.thread_id()))
            assert refused[0] == 1095 and "ProxySQL: connection" in refused[1], refused
            assert fe_id in heavy_queries(self.oracle), "the query of the victim stopped after a refused KILL"
            # The same user stops its query from the other FE: ProxySQL ends the session of the victim.
            assert not sql(killer, "KILL QUERY %d" % victim.thread_id()), "the KILL answered with rows"
            outcome["thread"].join(30)
            assert not outcome["thread"].is_alive(), "the query was not interrupted"
            assert "error" in outcome, outcome
            eventually(lambda: fe_id not in heavy_queries(self.oracle), 10)
            eventually(lambda: fe_id not in cn_queries(self.oracle), 10)
            assert sql(killer, "SELECT 1") == ((1,),), "the session of the killer goes on"
        finally:
            for connection in (killer, other, victim):
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass
            for host, port, weight in rows:
                sql(self.admin, "UPDATE mysql_servers SET weight=%s WHERE hostgroup_id=%s AND hostname=%s AND port=%s",
                    (int(weight), self.config["hostgroup"], host.decode() if isinstance(host, bytes) else host, int(port)))
            sql(self.admin, "LOAD MYSQL SERVERS TO RUNTIME")

    def jdbc_cancel_and_timeout(self):
        if not self.jar:
            return "no Connector/J jar (--connector-jar)"
        source = Path(__file__).resolve().with_name("FeAuthCancel.java")
        with tempfile.TemporaryDirectory(prefix="proxysql-fe-auth-cancel-") as output:
            subprocess.run(["javac", "-d", output, str(source)], check=True)
            for mode in ("cancel", "timeout"):
                environment = dict(os.environ, PROXY_HOST=str(self.config["proxy"]["host"]),
                                   PROXY_PORT=str(self.config["proxy"]["port"]), JDBC_USER=PASSWORD_USER,
                                   JDBC_PASSWORD=PASSWORD, JDBC_QUERY=HEAVY_QUERY)
                result = subprocess.run(["java", "-Xmx256m", "-cp", output + os.pathsep + str(Path(self.jar).resolve()),
                                         "FeAuthCancel", mode], env=environment, capture_output=True, text=True, timeout=120)
                lines = result.stdout.split()
                assert result.stdout.startswith(("cancelled ", "timeout ")) and "still-usable" in result.stdout, \
                    (mode, result.stdout, result.stderr)
                elapsed_ms = int(lines[1])
                assert elapsed_ms < 20000, (mode, "the query was not stopped soon", elapsed_ms)
                eventually(lambda: not heavy_queries(self.oracle), 10)

    def client_socket_closed_mid_query(self):
        """The FE stops the query when the client leaves, because ProxySQL closes the FE connection."""
        client, fe_id, outcome = self.start_heavy_query()
        # The FE can miss the cancel of a query that has run for less than a moment (the same happens
        # for a client that is not behind ProxySQL), so the client leaves a little later.
        time.sleep(2)
        assert fe_id in cn_queries(self.oracle), "the query does not run on the compute nodes"
        started = time.monotonic()
        client._sock.shutdown(socket.SHUT_RDWR)
        client._sock.close()
        eventually(lambda: fe_id not in heavy_queries(self.oracle), 15)
        eventually(lambda: fe_id not in cn_queries(self.oracle), 15)
        assert time.monotonic() - started < 15
        eventually(lambda: fe_id not in fe_users(self.oracle), 15)
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 15)

    def client_killed_mid_query_stops_cn_work(self):
        """Many clients leave while their queries run: nothing keeps running on the FE or the compute nodes."""
        clients = [self.start_heavy_query() for _ in range(4)]
        time.sleep(2)
        ids = {fe_id for _, fe_id, _ in clients}
        for client, fe_id, outcome in clients:
            client._sock.shutdown(socket.SHUT_RDWR)
            client._sock.close()
        try:
            eventually(lambda: not heavy_queries(self.oracle), 20)
            eventually(lambda: not (ids & cn_queries(self.oracle)), 20)
        except AssertionError:
            rows = [(row[1], row[2], row[5], row[7]) for row in sql(self.oracle, "SHOW PROCESSLIST")
                    if HEAVY_MARKER in (row[9] or b"")]
            raise AssertionError(("queries still run on the FE", rows))

    def unreachable_fe(self):
        # With no ONLINE FE a client gets a clear error instead of a greeting.
        rows = sql(self.admin, "SELECT hostname,port,status FROM mysql_servers WHERE hostgroup_id=%s", (self.config["hostgroup"],))
        sql(self.admin, "UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostgroup_id=%s", (self.config["hostgroup"],))
        sql(self.admin, "LOAD MYSQL SERVERS TO RUNTIME")
        try:
            error = login_error(lambda: login(self.config, PASSWORD_USER, PASSWORD))
            assert error[0] == 9001 and "no StarRocks FE" in error[1], error
        finally:
            for host, port, status in rows:
                sql(self.admin, "UPDATE mysql_servers SET status=%s WHERE hostgroup_id=%s AND hostname=%s AND port=%s",
                    (status.decode() if isinstance(status, bytes) else status, self.config["hostgroup"],
                     host.decode() if isinstance(host, bytes) else host, port))
            sql(self.admin, "LOAD MYSQL SERVERS TO RUNTIME")
        login(self.config, PASSWORD_USER, PASSWORD).close()

    def silent_client_times_out(self):
        # A client that never answers the greeting does not keep an FE connection forever.
        sock = socket.create_connection((self.config["proxy"]["host"], self.config["proxy"]["port"]), timeout=30)
        try:
            greeting = sock.recv(4096)
            assert greeting and greeting[4] == 10, "no greeting"
            used, _ = pool_counts(self.admin, self.config)
            assert used == 1, used
            timeout_ms = int(sql(self.admin, "SELECT variable_value FROM global_variables "
                                 "WHERE variable_name='mysql-connect_timeout_client'")[0][0])
            sock.settimeout(timeout_ms / 1000 + 15)
            assert sock.recv(1) == b"", "ProxySQL did not close the silent client"
        finally:
            sock.close()
        eventually(lambda: pool_counts(self.admin, self.config) == (0, 0), 10)

    def run(self):
        self.create_users()
        try:
            for name, function in (
                ("ProxySQL has no users and authenticates through the FE", self.no_users_in_proxysql),
                ("correct password logs in and queries run", self.password_login),
                ("wrong password gets the error of the FE", self.wrong_password),
                ("unknown user gets the error of the FE", self.unknown_user),
                ("user with an empty password logs in", self.empty_password),
                ("300 logins with random scrambles succeed", self.password_ending_with_zero_byte),
                ("database at login and its FE error", self.database_at_login),
                ("user with a non native plugin gets an error", self.non_native_user),
                ("TLS client login", self.tls_login),
                ("compressed client login, with and without TLS", self.compression_login),
                ("mysql command line client login", self.cli_login),
                ("Connector/J login in every variant", self.jdbc_login),
                ("a client with another authentication plugin switches to mysql_native_password", self.cli_auth_switch),
                ("session state stays on the FE connection of the client", self.session_state_stays),
                ("USE and COM_INIT_DB with a catalog run on the FE", self.use_catalog_and_database),
                ("the FE error on COM_INIT_DB reaches the client and the session goes on", self.init_db_error_keeps_the_session),
                ("a SET that the FE refuses gets the FE error itself", self.set_error_comes_with_the_set),
                ("SET CATALOG and USE follow the FE", self.set_catalog_then_use),
                ("queries that ProxySQL could answer get the answer of the FE", self.answers_come_from_the_fe),
                ("COM_PING is answered by the FE", self.ping_reaches_the_fe),
                ("COM_FIELD_LIST is answered by the FE", self.field_list_comes_from_the_fe),
                ("prepared statements in two databases run as on the FE", self.prepared_statements_follow_the_database),
                ("clients that leave during the login leave no FE connection", self.disconnects_during_login),
                ("every client holds its own FE connection and nothing is pooled", self.one_fe_connection_per_client),
                ("no FE connection serves two clients", self.no_reuse_over_many_logins),
                ("COM_CHANGE_USER gets an error", self.change_user_error),
                ("COM_RESET_CONNECTION gets an error", self.reset_connection_error),
                ("a lost FE connection closes the client", self.lost_fe_connection_closes_client),
                ("KILL QUERY id from a second connection stops the query of the first", self.kill_query_from_second_connection),
                ("KILL id from a second connection ends the first connection", self.kill_connection_from_second_connection),
                ("KILL of a session of another user gets the error of the FE", self.kill_of_another_user_gets_the_fe_error),
                ("KILL of an unknown id gets the error of the FE", self.kill_of_unknown_id),
                ("KILL of a session on another FE ends it for the same user and is refused for another user", self.kill_across_two_fes),
                ("Connector/J Statement.cancel() and setQueryTimeout() stop the query", self.jdbc_cancel_and_timeout),
                ("a client socket closed in the middle of a query stops the query on the FE", self.client_socket_closed_mid_query),
                ("clients that leave in the middle of queries leave no running query", self.client_killed_mid_query_stops_cn_work),
                ("no ONLINE FE gives a clear error", self.unreachable_fe),
                ("a silent client is closed with its FE connection", self.silent_client_times_out),
            ):
                self.case(name, function)
        finally:
            self.drop_users()
            self.oracle.close()
            self.admin.close()
        print("TAP version 13\n1..%d" % len(self.results))
        for number, (status, name) in enumerate(self.results, 1):
            if status.startswith("skip "):
                print("ok %d - %s # SKIP %s" % (number, name, status[5:]))
            else:
                print("%s %d - %s" % (status, number, name))
        return all(status != "not ok" for status, _ in self.results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--connector-jar", type=Path)
    parser.add_argument("--mysql-cli", default="mysql")
    args = parser.parse_args()
    suite = Suite(json.loads(args.config.read_text()), args.connector_jar, args.mysql_cli)
    sys.exit(0 if suite.run() else 1)
