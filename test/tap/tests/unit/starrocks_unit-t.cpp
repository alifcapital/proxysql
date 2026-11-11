#include "proxysql.h"
#include "cpp.h"
#include "mysql_connection.h"
#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include <string>

// A connection that took the login of a client must not go back to the pool or log in again.
static void test_relay_connection() {
	MySQL_Connection *connection_ptr = new MySQL_Connection();
	MySQL_Connection& connection = *connection_ptr;
	ok(!connection.starrocks_relay, "a fresh connection is not a relay connection");
	connection.relay_enable();
	ok(connection.starrocks_relay && !connection.reusable, "a relay connection is never reusable");
	ok(connection.MultiplexDisabled(), "a relay connection is pinned to its client");
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
	plan(7);
	test_init_minimal();
	test_relay_connection();
	test_cleanup_minimal();
	return exit_status();
}
