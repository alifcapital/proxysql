#!/usr/bin/env python3
"""Configure an isolated running ProxySQL; never touches its source checkout."""
import argparse
import json
from pathlib import Path
import pymysql

parser = argparse.ArgumentParser()
parser.add_argument("--admin-port", type=int, default=6032)
parser.add_argument("--frontend-port", type=int, default=6033)
parser.add_argument("--pid", type=int, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--spill-directory", type=Path, required=True)
parser.add_argument("--baseline", action="store_true")
parser.add_argument("--multiplexed", action="store_true",
                    help="keep multiplexing in the StarRocks hostgroup (multiplex=1)")
parser.add_argument("--fe-auth", action="store_true",
                    help="the StarRocks FE authenticates the clients: set mysql-starrocks_auth_hostgroup "
                         "and keep no mysql_users rows (clients log in as users of the FE)")
parser.add_argument("--hostgroup", type=int, default=41)
parser.add_argument("--backend-host", required=True, help="first FE (the one that gets the traffic)")
parser.add_argument("--backend-port", type=int, default=9030)
parser.add_argument("--replica-host", required=True, help="second FE, used after the first one is drained")
parser.add_argument("--replica-port", type=int, default=9030)
args = parser.parse_args()
if args.fe_auth and (args.baseline or args.multiplexed):
    parser.error("--fe-auth keeps one FE connection per client: it cannot be combined with --baseline or --multiplexed")
args.spill_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
# With the FE authentication every client holds one FE connection until it disconnects.
max_connections = 200 if args.fe_auth else 10
connection = pymysql.connect(host="127.0.0.1", port=args.admin_port,
                             user="admin", password="admin", autocommit=True)
with connection.cursor() as cursor:
    cursor.execute("DELETE FROM mysql_servers WHERE hostgroup_id=%s", (args.hostgroup,))
    cursor.executemany("INSERT INTO mysql_servers(hostgroup_id,hostname,port,weight,max_connections) VALUES(%s,%s,%s,%s,%s)",
                       [(args.hostgroup, args.backend_host, args.backend_port, 1000000, max_connections),
                        (args.hostgroup, args.replica_host, args.replica_port, 1, max_connections)])
    cursor.execute("DELETE FROM mysql_users")
    if not args.fe_auth:
        cursor.execute("INSERT INTO mysql_users(username,password,default_hostgroup,transaction_persistent) VALUES('root','',%s,0)", (args.hostgroup,))
    for query in [
        "LOAD MYSQL SERVERS TO RUNTIME",
        "LOAD MYSQL USERS TO RUNTIME",
        "UPDATE global_variables SET variable_value='1048576' WHERE variable_name='mysql-threshold_resultset_size'",
        "UPDATE global_variables SET variable_value='false' WHERE variable_name='mysql-monitor_enabled'",
        "UPDATE global_variables SET variable_value='true' WHERE variable_name='mysql-have_ssl'",
    ]:
        cursor.execute(query)
    if not args.baseline:
        cursor.execute("INSERT OR REPLACE INTO mysql_hostgroup_attributes(hostgroup_id,multiplex,hostgroup_settings) "
                       "VALUES(%s,%s,'{\"backend_type\":\"starrocks\"}')", (args.hostgroup, 1 if args.multiplexed else 0))
        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME")
        for name, value in [("mysql-resultset_spill_enabled", "true"),
                            ("mysql-resultset_spill_directory", str(args.spill_directory)),
                            ("mysql-resultset_spill_limit", "0")]:
            cursor.execute("UPDATE global_variables SET variable_value=%s WHERE variable_name=%s", (value, name))
            assert cursor.rowcount == 1, name
    cursor.execute("UPDATE global_variables SET variable_value=%s WHERE variable_name='mysql-starrocks_auth_hostgroup'",
                   (str(args.hostgroup) if args.fe_auth else "-1",))
    assert cursor.rowcount == 1, "mysql-starrocks_auth_hostgroup"
    cursor.execute("LOAD MYSQL VARIABLES TO RUNTIME")
connection.close()
config = json.loads((Path(__file__).parent / "config.example.json").read_text())
config["admin"]["port"] = args.admin_port
config["proxy"]["port"] = args.frontend_port
config["proxysql_pid"] = args.pid
config["hostgroup"] = args.hostgroup
config["backend"].update(host=args.backend_host, port=args.backend_port)
config["new_fe"] = {"host": args.replica_host, "port": args.replica_port}
config["spill_file_marker"] = "proxysql-result-"
config["sticky_connection"] = not (args.baseline or args.multiplexed)
config["fe_auth"] = args.fe_auth
args.output.write_text(json.dumps(config, indent=2) + "\n")
args.output.chmod(0o600)
