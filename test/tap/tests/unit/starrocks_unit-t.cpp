#include "StarRocks_Transaction.h"
#include "proxysql.h"
#include "cpp.h"
#include "mysql_connection.h"
#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include <string>

static void test_transaction_commands() {
	ok(starrocks_transaction_command("BEGIN", 5) == 1, "BEGIN starts a transaction");
	ok(starrocks_transaction_command(" /* c */ begin work with label x;", 33) == 1, "BEGIN WORK with label, comment and trailing semicolon");
	ok(starrocks_transaction_command("START TRANSACTION WITH CONSISTENT SNAPSHOT", 41) == 1, "START TRANSACTION starts a transaction");
	ok(starrocks_transaction_command("START SLAVE", 11) == 0, "START without TRANSACTION is not a transaction");
	ok(starrocks_transaction_command("COMMIT", 6) == -1, "COMMIT ends a transaction");
	ok(starrocks_transaction_command("rollback work ;  ", 17) == -1, "ROLLBACK WORK with trailing space ends a transaction");
	ok(starrocks_transaction_command("SELECT 'COMMIT'", 15) == 0, "COMMIT inside a literal is not a command");
	ok(starrocks_transaction_command("COMMITTED", 9) == 0, "keyword needs a word boundary");
	ok(starrocks_transaction_command("SELECT 1; BEGIN", 15) == 1, "BEGIN in a later statement begins a transaction");
	ok(starrocks_transaction_command("BEGIN; COMMIT", 13) == 1, "a multi-statement text never ends a transaction");
	const std::string literal = "INSERT INTO t VALUES ('a; COMMIT')";
	ok(starrocks_transaction_command(literal.data(), literal.size()) == 0, "COMMIT after a semicolon in a literal does not end a transaction");
	ok(starrocks_transaction_command("", 0) == 0, "empty text has no transaction command");
	const std::string comment = "-- BEGIN\nSELECT 1";
	ok(starrocks_transaction_command(comment.data(), comment.size()) == 0, "BEGIN in a line comment is not a command");
}

static void test_connection_flags() {
	MySQL_Connection *connection_ptr = new MySQL_Connection();
	MySQL_Connection& connection = *connection_ptr;
	ok(!connection.MultiplexDisabled(), "a fresh connection can be multiplexed");
	connection.set_status(true, STATUS_MYSQL_CONNECTION_NO_MULTIPLEX_HG);
	ok(connection.MultiplexDisabled(), "hostgroup multiplex=0 disables multiplexing");
	ok(!connection.MultiplexDisabled(true, false), "hostgroup multiplex=0 can be ignored");
	connection.set_status(true, STATUS_MYSQL_CONNECTION_STARROCKS_TRANSACTION);
	ok(connection.IsActiveTransaction(), "an open StarRocks transaction is an active transaction");
	connection.set_status(false, STATUS_MYSQL_CONNECTION_STARROCKS_TRANSACTION);
	ok(!connection.IsActiveTransaction(), "a finished StarRocks transaction is not active");
	connection.set_status(true, STATUS_MYSQL_CONNECTION_TEMPORARY_TABLE);
	ok(connection.MultiplexDisabled(true, false), "connection state pins the connection when the hostgroup setting is ignored");
	delete connection_ptr;
}

// A connection that took the login of a client must not go back to the pool or log in again.
static void test_relay_connection() {
	MySQL_Connection *connection_ptr = new MySQL_Connection();
	MySQL_Connection& connection = *connection_ptr;
	ok(!connection.starrocks_relay, "a fresh connection is not a relay connection");
	connection.relay_enable();
	ok(connection.starrocks_relay && !connection.reusable, "a relay connection is never reusable");
	ok(connection.MultiplexDisabled(true, false), "a relay connection is pinned to its client");
	ok(connection.relay.state == PROXYSQL_RELAY_IDLE, "the relay waits for the scramble of the server");
	ok(connection.userinfo->username != NULL && connection.userinfo->username[0] == '\0',
		"a relay connection has an empty user name until the client is known");
	connection.reusable = true;
	connection.reset();
	ok(!connection.reusable, "reset does not make a relay connection reusable");
	ok(connection.get_status(STATUS_MYSQL_CONNECTION_NO_MULTIPLEX_HG), "reset keeps the hostgroup flag of a relay connection");
	delete connection_ptr;
}

int main() {
	plan(26);
	test_init_minimal();
	test_transaction_commands();
	test_connection_flags();
	test_relay_connection();
	test_cleanup_minimal();
	return exit_status();
}
