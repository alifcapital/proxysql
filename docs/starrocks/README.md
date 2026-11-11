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
The FE query is complete as soon as ProxySQL has fetched the result, while the
client still reads the spooled response. The client keeps its FE connection while
it reads, so `ConnUsed` stays above 0.
Backend fetching is throttled when the bounded disk-write queue is full.

Spill applies to regular MySQL frontend sessions, including TLS, compressed
connections and prepared results. Admin, mirror and fast-forward sessions do not
use this path. Frontend compression is independent of FE compression; Connector/J
can use `useCompression=true` against ProxySQL with an uncompressed FE connection.

## Let the StarRocks FE authenticate the clients

With the variable `mysql-starrocks_auth_hostgroup` ProxySQL holds no passwords at
all: the FE authenticates every client, with its own users, passwords and error
messages, and every client keeps one FE connection for its whole session.

```sql
-- hostgroup 10 holds the FEs
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
* The FE keeps the whole session state of the client, and ProxySQL keeps none. ProxySQL
  sends every command of the client to the FE and gives the client the answer of the
  FE: `USE`, `COM_INIT_DB` (an error of the FE leaves the session as it was), `SET` of
  any variable, `COMMIT`, `ROLLBACK`, `SHOW WARNINGS`, `COM_PING`, `COM_FIELD_LIST`, `SELECT VERSION()`,
  `LAST_INSERT_ID()` and the other queries that ProxySQL answers itself for its own users.
  It sends nothing of its own before a request, and the query cache is not used. A client
  without a database at login has no database on the FE, as on a direct connection.
* Each prepare of a client is a statement of its own on the FE, and `COM_STMT_CLOSE`
  closes it there. ProxySQL does not know the catalog and the database of the FE, so it
  does not share prepared statements between clients or between prepares of one client.
* ProxySQL still answers a few commands itself: `SELECT CONNECTION_ID()` and the
  greeting carry the connection id of ProxySQL (see `KILL` below), `PROXYSQL INTERNAL`
  commands, `SHOW STATUS LIKE 'Ssl_version'` (about the TLS between the client and
  ProxySQL), the refusal of `LOAD DATA LOCAL INFILE`, and the commands that the FE does
  not support: `COM_SET_OPTION`, `COM_STATISTICS`, `COM_REFRESH` and `COM_PROCESS_KILL`.

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
the session it wants to kill (for example, while the FE of that session is drained),
ProxySQL ends that session itself when it belongs to the same user: it closes the
client connection and the FE connection of that session, and the FE cancels the
query. `KILL QUERY` then ends the whole session, because ProxySQL has no other way to
stop the query on that FE. ProxySQL cannot check the privilege of the FE to kill the
session of another user on another FE, so that `KILL` gets the error 1095.
`KILL QUERY 'query_id'` with a query id string goes to the FE as it is, and the FE
forwards it to the FE that owns the query.

A query that runs longer than its timeout in ProxySQL (`mysql-default_query_timeout`, the
`default_query_timeout` of the hostgroup or the `timeout` of a query rule) ends the whole
session: ProxySQL closes the client connection and the FE connection, and the FE cancels the
query. ProxySQL has no password to cancel the query over another FE connection.

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
  schemas of `mysql_users` and `fast_forward`.
* TLS from ProxySQL to the FE (`use_ssl` of the server row) is not tested.

## Switch a frontend

We want to replace an FE without cutting its clients. A client cannot move to another
FE: ProxySQL has no password to log in again, and the new FE would not know the
session. So the old FE keeps its sessions, and only new logins go to the new FE.

1. Add the new FE to the hostgroup as `ONLINE` and run
   `LOAD MYSQL SERVERS TO RUNTIME`.
2. Set the old FE to `OFFLINE_SOFT` and run `LOAD MYSQL SERVERS TO RUNTIME`. New
   clients log in on an `ONLINE` FE from now on.
3. Every session of the old FE goes on there as before: a running request finishes, an
   open transaction can commit, and the next statements run on the same FE connection.
4. Wait until `ConnUsed` of the old FE in `stats_mysql_connection_pool` is 0, that is,
   until its clients have disconnected by themselves, then stop it. Clients that keep
   their connections open (connection pools) hold the old FE until they close them;
   stopping the FE earlier cuts them.

While a session is on the old FE, a `KILL` that its client sends over a new connection
lands on the new FE. ProxySQL then ends that session itself (see `KILL` above), so
Ctrl+C stops the query, and the session ends with it.

ProxySQL closes the client connection when the FE connection of the client ends (the
FE stops or restarts, or the connection is killed on the FE), or when the FE is set to
`OFFLINE_HARD`. It never connects to an FE for a client that is logged in. The client has
to connect again, so clients and drivers must reconnect, as they must for any lost
connection.
