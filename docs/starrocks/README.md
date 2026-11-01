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

## Let the StarRocks FE authenticate the clients

With the default setup ProxySQL holds the user names and passwords in
`mysql_users` and logs in to the FE with them. With the variable
`mysql-starrocks_auth_hostgroup` ProxySQL holds no passwords at all: the FE
authenticates every client, with its own users, passwords and error messages.

```sql
-- hostgroup 10 holds the FEs, with backend_type=starrocks and multiplex=0 as above
SET mysql-starrocks_auth_hostgroup=10;
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
```

The default is -1, which keeps the normal ProxySQL authentication. With 0 or more,
every MySQL frontend login uses the FE hostgroup, and `mysql_users` is not read.
The setting applies to new connections only. Connections that exist keep the
authentication they were made with.

How a login works:

1. A client connects. ProxySQL opens a new connection to an `ONLINE` FE of the
   hostgroup and reads the FE greeting, which carries the scramble (the random
   challenge) of that connection.
2. ProxySQL sends the client its own greeting with that scramble, and the plugin
   `mysql_native_password`.
3. The answer of the client goes to the FE connection as the answer of the
   connection itself, and the FE accepts or refuses it. The client gets the OK or
   the error of the FE as it is, with the error number, the SQL state and the
   message of the FE.
4. After an OK, this FE connection belongs to this client until the client
   disconnects. ProxySQL never gives it to another client and never puts it in the
   pool. When the client disconnects, ProxySQL closes the FE connection.

What to know when you set it up:

* The FE sees the IP address of ProxySQL as the address of every client. Create the
  users of the FE as `'user'@'%'`, or with the address of ProxySQL, because a host
  restriction on the address of the real client cannot work.
* Every client holds one FE connection, also while it logs in. Set `max_connections`
  of each `mysql_servers` row high enough for the clients that can be on that FE at
  once. A client that finds no FE with a free connection gets the error 9001 at
  once.
* A client that connects and does not answer holds an FE connection until
  `mysql-connect_timeout_client` ends (10 seconds by default).
* If no FE is `ONLINE`, or none can be reached, the client gets the error 9001 instead
  of a greeting.
* TLS and compression between the client and ProxySQL work as before. The FE sees
  none of them, because ProxySQL has its own connection to the FE.
* Clients that use another authentication plugin than `mysql_native_password`
  are asked to switch to it, as with normal ProxySQL users. A client library that
  cannot switch cannot log in.
* Query rules must not send these clients to another hostgroup. ProxySQL has no
  password for a connection there, so the client gets an error and its connection
  ends.

Stopping a query works as with a plain MySQL server. A client sends `KILL QUERY id`
or `KILL id` over a new connection (the `mysql` client does it on Ctrl+C, and
Connector/J does it for `Statement.cancel()` and `setQueryTimeout()`). The new
connection logs in through the FE as the same user, and `id` is the connection id
that ProxySQL gave the client in the greeting and in `SELECT CONNECTION_ID()`.
ProxySQL replaces `id` with the FE connection id of that session and runs the
`KILL` on the FE connection of the new session, so ProxySQL needs no password and
the FE answers with its own OK or error. The FE lets a user kill its own sessions
and requires `OPERATE` for the sessions of other users. The connection id is kept
by one ProxySQL, so the `KILL` must reach the ProxySQL that holds the session.
The FE kills only its own connections. If the new connection is on another FE than
the session it wants to kill, the client gets the error 1094 that names both FEs.
`KILL QUERY 'query_id'` with a query id string goes to the FE as it is, and the FE
forwards it to the FE that owns the query.

When a client disconnects while its query runs, ProxySQL closes the FE connection,
and the FE cancels the query (`mysql_service_kill_after_disconnect`, true by default). The
FE can miss the cancel of a query that started less than about a second before, whether the
client is behind ProxySQL or not.

What is not supported in this version:

* Users of the FE that need another authentication plugin than
  `mysql_native_password` (for example LDAP, where the FE sends an authentication
  switch request) get an error that names the plugin.
* `COM_CHANGE_USER` and `COM_RESET_CONNECTION` get an error. Both would change or
  drop the FE connection of the client, and ProxySQL cannot log in again. The client
  has to disconnect and connect again as the other user.
* Per user limits (`max_connections` of `mysql_users`), user attributes, default
  schemas of `mysql_users` and `fast_forward`. A client without a database gets
  `mysql-default_schema`.
* TLS from ProxySQL to the FE (`use_ssl` of the server row) is not tested.

## Switch a frontend

This section describes the setup with ProxySQL users. The next section describes
the FE authentication, where a session does not move.

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

### Switch a frontend when the FE authenticates the clients

A client cannot move to another FE here: ProxySQL has no password to log in again,
and the new FE would not know the session. So an FE is drained by closing its
clients, and the clients log in again on an `ONLINE` FE.

1. Add the new FE to the hostgroup as `ONLINE` and run
   `LOAD MYSQL SERVERS TO RUNTIME`.
2. Set the old FE to `OFFLINE_SOFT` and run `LOAD MYSQL SERVERS TO RUNTIME`. New
   clients log in on an `ONLINE` FE from now on.
3. A client whose request runs on the old FE gets the whole result. Then ProxySQL
   closes its connection. A client that is idle is closed within about a second. An
   open transaction ends with the connection, and the FE rolls it back. A request
   that arrives after the switch is not run.
4. Wait until `ConnUsed` of the old FE in `stats_mysql_connection_pool` is 0, then
   stop it.

The same happens when the FE connection of a client ends for another reason (the FE
restarts, or the connection is killed on the FE): ProxySQL closes the client
connection. It never connects to an FE for a client. The client has to connect
again, so clients and drivers must reconnect, as they must for any lost connection.
