# Real StarRocks 4.1 integration tests

This directory exercises a production ProxySQL executable against real StarRocks
FEs and a compute node. It does not emulate a backend. It is a standalone TAP13
producer; it is not registered as, and must not be reported as, an execution of
the isolated C++ TAP runner in `test/infra`.

Run on the Linux host of an isolated ProxySQL process, as its user or root, so
`/proc/PID/fd` and `/proc/PID/status` are readable. Reserve a dedicated hostgroup
and frontend listener: aggregate `ConnUsed` is only meaningful without unrelated
traffic. Disable query cache for this user/query. Register two distinct real FEs
in that hostgroup and set its `hostgroup_settings` to
`{"backend_type":"starrocks"}` and `multiplex=0`.

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
  missing directories, ENOSPC/EACCES when configured, query cache,
  prepared/TLS/compressed results, multiple results and a later statement error.
* `upgrade_drain.py`: OFFLINE_SOFT during an executing slow query, completion on
  the old FE and subsequent use of the other FE.
* `sticky_switch.py`: a client keeps one FE connection while other clients churn
  the pool, a running query survives an FE switch and idle sessions leave the old
  FE on their next statement, and an open explicit transaction stays on the old
  FE until `COMMIT`. The hostgroup must have `multiplex=0` (set by `setup.py`)
  and contain only `backend`; `new_fe` is the second FE that each case adds when
  it switches.
* `protocol_edges.py`: compressed raw pipelining and COM_QUIT with unread spill.
* `compression_framing.py` reads a million short rows through a compressed connection with
  mysql-connector-python and compares them with a direct FE. The result is spilled, and the
  blocks of the spill have any size, so without the framer many compressed packets ended inside a
  MySQL packet header and the driver failed every time. It works with every setup.
* `large_packets.py`: large wire packets across the protocol packet boundary,
  with real spool-file and cleanup evidence. Its Python connector transport
  adapter joins compressed frames and binary row continuations that the pure
  driver cannot assemble; it verifies frame lengths and continuation sequences.
* `large_packets_jdbc.py`: the same 20 MiB row through an unmodified Connector/J
  jar, including negotiated compression, server-side prepare and their combination.
* `concurrent_spill.py`: eight simultaneous clients, bounded output, cleanup and
  concurrent cancellation/disconnect behavior, with a JSON evidence report.

Additional commands for dedicated isolated listeners:

```sh
python test/starrocks/sticky_switch.py /tmp/starrocks-test.json
python test/starrocks/protocol_edges.py /tmp/starrocks-test.json
python test/starrocks/concurrent_spill.py /tmp/starrocks-test.json --clients 8 --report /tmp/concurrent.json
python test/starrocks/large_packets_jdbc.py --config /tmp/starrocks-test.json --connector-jar /path/to/mysql-connector-j-8.0.33.jar --proxy-port 6033
```

`setup.py` gives the hostgroup `multiplex=0`, as the operator documentation
prescribes, and records `sticky_connection` in the JSON file. `e2e.py`,
`upgrade_drain.py` and `sticky_switch.py` are written for that mode.
`protocol_edges.py` and `concurrent_spill.py` assert that the FE connection goes
back to the pool while the client reads, or that a closed session leaves a
reusable pooled connection, so run them against a hostgroup created with
`setup.py --multiplexed`.

Run fixtures serially when they share a listener or hostgroup: migration cases
change server status. Presence of a fixture or an example command does not
establish a passing run.

Record executable revision, StarRocks revision, exact configuration, endpoint
roles, TAP output and spill/RSS evidence with each result. Do not mark unexecuted
cases as passing.
