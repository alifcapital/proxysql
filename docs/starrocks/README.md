# StarRocks result buffering and hostgroups

## Buffer results for slow clients

Create a private directory writable by the ProxySQL process, then run these
commands on the ProxySQL admin listener:

```sql
SET mysql-resultset_spill_directory='/var/lib/proxysql/result-spill';
SET mysql-threshold_resultset_size=4194304;
SET mysql-resultset_spill_limit=0;
SET mysql-resultset_spill_enabled=true;
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
```

Spill is disabled by default. The directory defaults to `/tmp` and must be an
existing absolute path. The existing result threshold defaults to 4 MiB; sizes
are bytes. Once pending output exceeds the threshold, ProxySQL writes response
bytes to a private temporary file while continuing to serve the client in order.
Two shared filesystem workers perform disk IO. Each response owns its file,
created with mode 0600 and immediately unlinked; inspect the process's open file
descriptors rather than directory listings when diagnosing disk use.

`mysql-resultset_spill_limit` limits unread disk backlog per response, including
pending writes. Zero means unlimited; a nonzero value can be at most 2147483647
bytes. This is not a process-wide disk quota. A finite limit uses a circular
file; unlimited mode reclaims consumed extents on supporting Linux filesystems
and truncates the file when its disk backlog drains. Filesystem exhaustion,
permission failures and configured-limit exhaustion close the response stream
and log an error: a truncated response must not appear successful.

The memory threshold is a buffering trigger, not a strict process RSS limit.
Write staging, readback, protocol/TLS buffers and an individual oversized row
can require additional memory. Query-cache entries also have their own budget;
oversized cache hits bypass copying into the response and fetch from the backend.
After the backend finishes and its session is eligible for multiplexing, it can
return to the pool while the client continues reading the spooled response.
Backend fetching is throttled when the bounded disk-write queue is full.

Spill applies to regular MySQL frontend sessions, including TLS, compressed
connections and prepared results. Admin, mirror and fast-forward sessions do not
use this path. Frontend compression is independent of FE compression; Connector/J
can use `useCompression=true` against ProxySQL with an uncompressed FE connection.

## Enable StarRocks behavior

Enable StarRocks behavior explicitly for each backend hostgroup:

```sql
INSERT OR IGNORE INTO mysql_hostgroup_attributes(hostgroup_id) VALUES(10);
UPDATE mysql_hostgroup_attributes
SET hostgroup_settings=json_set(CASE WHEN hostgroup_settings='' THEN '{}'
                                    ELSE hostgroup_settings END,
                                '$.backend_type', 'starrocks')
WHERE hostgroup_id=10;
LOAD MYSQL SERVERS TO RUNTIME;
```

This capability leaves the default MySQL behavior unchanged.

## Keep a client on one FE connection

With `mysql-multiplexing=true` (the default) every statement of a client may run
on another pooled FE connection. State that lives in the FE connection then
follows the client only by luck: temporary tables, session variables and SQL
`PREPARE` statements are lost. Set the hostgroup attribute `multiplex` to 0 for
every StarRocks hostgroup, so that a client keeps its FE connection until it
disconnects:

```sql
INSERT OR IGNORE INTO mysql_hostgroup_attributes(hostgroup_id) VALUES(10);
UPDATE mysql_hostgroup_attributes SET multiplex=0 WHERE hostgroup_id=10;
LOAD MYSQL SERVERS TO RUNTIME;
```

The attribute applies to the hostgroup only, so other hostgroups keep the
global `mysql-multiplexing`. We do not use `mysql-multiplexing=false` because it
would also stop multiplexing in non-StarRocks hostgroups. Set the attribute
when you create the hostgroup, before clients connect.

Every client session holds one FE connection, so `max_connections` of each
`mysql_servers` row must be at least the number of clients that can be on that
FE at the same time. A client that cannot get a connection waits for
`mysql-connect_timeout_server_max` and then receives error 9001.

A client that reads a buffered response slowly (see the first section) keeps its
FE connection while it reads, so `ConnUsed` stays above 0. The FE query is
complete as soon as ProxySQL has fetched the result.

StarRocks does not report an open explicit transaction in its OK packets, so
ProxySQL follows `BEGIN`, `START TRANSACTION`, `COMMIT` and `ROLLBACK` itself
and keeps the connection pinned between them. A `COMMIT` or `ROLLBACK` that
shares a packet with other statements does not release the connection; the next
single-statement `COMMIT` or `ROLLBACK` does.

## Switch a frontend

Use this procedure to replace an FE (for example, to promote a follower):

1. Add the new FE to the hostgroup as `ONLINE` and run
   `LOAD MYSQL SERVERS TO RUNTIME`.
2. Set the old FE to `OFFLINE_SOFT` and run `LOAD MYSQL SERVERS TO RUNTIME`.
   The two steps may also be one `LOAD`.
3. Wait until `ConnUsed` of the old FE in `stats_mysql_connection_pool` is 0,
   then stop it.

A query that is running on the old FE finishes there and the client receives the
whole result. When a request ends, ProxySQL releases the session's connection to
a server that is not `ONLINE`, and the next statement of the same client
connection runs on an `ONLINE` FE. ProxySQL does not copy the session
variables, catalog or database of the client to the new FE, so the session
starts there as a new login.

A client that is idle when the old FE is set to `OFFLINE_SOFT` keeps its
connection to that FE until it sends its next command, so an idle client that
never sends anything keeps `ConnUsed` above 0. Stop the old FE only when
`ConnUsed` is 0, or accept that such clients lose the connection when it stops.

A session stays on the old FE while its connection holds state that cannot move:
an open transaction (until `COMMIT` or `ROLLBACK`), a temporary table, a user
variable, an SQL `PREPARE` or a lock. Such a session leaves the old FE with the
first request that ends without that state.
