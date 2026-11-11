# Real StarRocks 4.1 integration tests

This directory exercises a production ProxySQL executable against real StarRocks
FEs and a compute node. It does not emulate a backend. It is a standalone TAP13
producer; it is not registered as, and must not be reported as, an execution of
the isolated C++ TAP runner in `test/infra`.

Run on the Linux host of an isolated ProxySQL process, as its user or root, so
`/proc/PID/fd` and `/proc/PID/status` are readable. Reserve a dedicated hostgroup
and frontend listener: aggregate `ConnUsed` is only meaningful without unrelated
traffic. Register two distinct real FEs in that hostgroup. ProxySQL runs with
`mysql-starrocks_auth_hostgroup` set to that hostgroup: the FE authenticates the clients,
and every client keeps its own FE connection.

Enable `mysql-resultset_spill_enabled`, set
`mysql-resultset_spill_directory` to a private writable directory,
`mysql-threshold_resultset_size` to 1048576, and allow at least 128 MiB with
`mysql-resultset_spill_limit` (zero means no disk cap). Load variables to runtime.
Use a warmed process to reduce allocator startup noise in the RSS assertion.

```sh
python3 -m venv .venv
.venv/bin/pip install -r test/starrocks/requirements.txt
cp test/starrocks/config.example.json /tmp/starrocks-test.json
# Set actual PID, isolated endpoints, spill filename marker and credentials.
.venv/bin/python test/starrocks/e2e.py /tmp/starrocks-test.json > /tmp/starrocks.tap
```

For actual filesystem faults, set `fault_directory` to a private directory in
the isolated runtime. The harness mounts a fresh 16 MiB tmpfs below it using
sudo, holds the client unread so it fills, then unmounts it in a finally block.
The EACCES case uses a separate directory with mode 0555 and restores/removes it.
Run ProxySQL and the tests as the same non-root user. These checks require sudo
mount privileges; do not substitute disk quota failure as evidence of ENOSPC.
The TLS example verifies that TLS was negotiated against the isolated frontend
with its generated test certificate; configure certificate verification for a
trusted deployment. Frontend TLS/compression are compared with the plaintext FE
oracle, since the golden FE itself advertises neither capability.

`setup.py` configures only a dedicated running test instance and generates the
JSON file. The two FEs must have independent roles (a leader and an observer, for example):
`SHOW FRONTENDS` verifies the actual roles. The addresses in `config.example.json` are
documentation addresses, and `setup.py` takes the real ones from `--backend-host` and
`--replica-host`.

```sh
python test/starrocks/setup.py --pid PROXYSQL_PID --output /tmp/starrocks-test.json --spill-directory /tmp/private-spill \
    --backend-host FE1_ADDRESS --replica-host FE2_ADDRESS
```

The ordered result fingerprint includes a null marker, a length prefix and every
column's bytes, preventing ambiguous concatenations. The oracle query runs
directly on the FE. The slow client uses an unbuffered cursor and requests an
initial 1 MiB TCP receive buffer, stops reading after metadata, and waits for both an actual
nonempty spill FD and backend `ConnUsed=0`. It then checks RSS growth and consumes
the entire response to compare row count, byte count and SHA256. Another case
disconnects with unread bytes and waits for the spill FD inventory to return to
baseline. Files may be unlinked immediately; an empty directory is not proof of
no spill or cleanup.

Implemented acceptance fixtures (execution results must be recorded separately):

* `e2e.py`: paused readers, spill/RSS evidence, disconnect cleanup, disk limits,
  missing directories, ENOSPC/EACCES when configured,
  prepared/TLS/compressed results, multiple results and a later statement error.
* `upgrade_drain.py`: OFFLINE_SOFT during an executing slow query, completion on
  the old FE, the next statement of that client on the old FE, and a new client on
  the other FE.
* `sticky_switch.py`: a client keeps one FE connection while other clients run
  statements; after an FE switch a running query finishes, the running and the idle
  session stay on the old FE with their FE connections, a new client lands on the
  `ONLINE` FE, and the old FE is free when its clients leave; an open transaction
  goes on and commits on the old FE; Ctrl+C of a session on the old FE, sent over a new
  connection on the new FE, stops its query. Each case starts with only `backend` in the
  hostgroup; `new_fe` is the second FE that each case adds when it switches.
* `protocol_edges.py`: compressed raw pipelining and COM_QUIT with unread spill.
* `large_packets.py`: large wire packets across the protocol packet boundary,
  with real spool-file and cleanup evidence. Its Python connector transport
  adapter joins compressed frames and binary row continuations that the pure
  driver cannot assemble; it verifies frame lengths and continuation sequences.
* `large_packets_jdbc.py`: the same 20 MiB row through an unmodified Connector/J
  jar, including negotiated compression, server-side prepare and their combination.
* `concurrent_spill.py`: eight simultaneous clients, bounded output, cleanup and
  concurrent cancellation/disconnect behavior, with a JSON evidence report.

## StarRocks FE authentication

`setup.py` sets `mysql-starrocks_auth_hostgroup` to the test hostgroup, leaves
`mysql_users` empty and gives each server row 200 connections, because every client
holds one FE connection. The clients log in as users of the FE (`root` without a
password in `config.example.json`). Start from a ProxySQL that has no pooled
connections, because `fe_auth.py` checks that nothing goes to the pool.

```sh
python test/starrocks/fe_auth.py /tmp/starrocks-test.json --connector-jar /path/to/mysql-connector-j-8.0.33.jar
```

* `fe_auth.py` creates its own FE users (a user with a password, one with an empty
  password and one with the LDAP plugin) through `config["backend"]`, which must be
  allowed to run `CREATE USER`, and drops them at the end. It checks: login with a
  right and a wrong password, an unknown user and an empty password, where the error
  of the client must equal the error of a direct FE login; a database at login; the
  error for a user that needs another plugin; TLS, zlib compression, the `mysql`
  command line client (plain, TLS, compressed, with an authentication switch),
  Connector/J (plain, TLS, compressed, prepared); that every client holds its own FE
  connection and none is pooled or reused; the error of `COM_CHANGE_USER` and
  `COM_RESET_CONNECTION`; that a killed FE connection closes the client; the error
  when no FE is `ONLINE`; that clients which leave during the login leave no FE
  connection. It also checks `KILL QUERY id` and `KILL id` from a second connection of
  the same user (the way the `mysql` client and Connector/J cancel), `Statement.cancel()`
  and `setQueryTimeout()` of Connector/J, that the FE answers a `KILL` of a session of
  another user and of an unknown id with its own error, and that a query stops on the
  FE when its client closes the socket. These cases run a query over
  `TABLE(generate_series(1,100000000000))` on the compute nodes and look at the FE process
  list; each case fails if a query from an earlier case still runs. The command line client and Connector/J cases are marked SKIP when the
  client or the jar is missing; a SKIP is not a pass. Other cases compare the answers
  through ProxySQL with a direct FE connection: `USE` and `COM_INIT_DB` with a catalog and
  the FE error on `COM_INIT_DB`, a `SET` that the FE refuses, `SET CATALOG` with `USE`,
  queries such as `SELECT @@version_comment` and `USER()`, `COM_PING`, `COM_FIELD_LIST`,
  and prepared statements of one text in two databases.
* `compression_framing.py` reads a million short rows through a compressed connection with
  mysql-connector-python and compares them with a direct FE. The result is spilled, and the
  blocks of the spill have any size, so without the fix many compressed packets ended inside a
  MySQL packet header and the driver failed every time.
* `transactions.py` sends statements around explicit transactions (after a user variable or
  `SET autocommit=0`, `COMMIT`, `ROLLBACK`) and fails if one gets no answer.
* `e2e.py`, `large_packets.py`, `large_packets_jdbc.py`, `protocol_edges.py` and
  `concurrent_spill.py` run unchanged against the same setup. The two last ones wait
  for the FE query to end, as the clients keep their FE connections.

Additional commands for dedicated isolated listeners:

```sh
python test/starrocks/sticky_switch.py /tmp/starrocks-test.json
python test/starrocks/protocol_edges.py /tmp/starrocks-test.json
python test/starrocks/concurrent_spill.py /tmp/starrocks-test.json --clients 8 --report /tmp/concurrent.json
python test/starrocks/large_packets_jdbc.py --config /tmp/starrocks-test.json --connector-jar /path/to/mysql-connector-j-8.0.33.jar --proxy-port 6033
```

`protocol_edges.py` and `concurrent_spill.py` wait until the FE query has ended while
the client has not read the result. The client keeps its FE connection, so they look
at the FE process list.

Run fixtures serially when they share a listener or hostgroup: migration cases
change server status. Presence of a fixture or an example command does not
establish a passing run.

Record executable revision, StarRocks revision, exact configuration, endpoint
roles, TAP output and spill/RSS evidence with each result. Do not mark unexecuted
cases as passing.
