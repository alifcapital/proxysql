#!/usr/bin/env python3
"""Compressed pipelining and COM_QUIT cancellation against production sockets."""
import argparse
import json
from pathlib import Path
import socket
import sys
import traceback
import mysql.connector
from e2e import Suite, connect, eventually, fingerprint

parser = argparse.ArgumentParser()
parser.add_argument("config", type=Path)
args = parser.parse_args()
config = json.loads(args.config.read_text())
suite = Suite(config)


def wait_spill():
    eventually(lambda: any(size > 0 for fd, size in suite.spill_fds().items()
                           if fd not in suite.baseline_fds) and suite.backends_idle(),
               config.get("timeout", 120))


def compressed_pipeline():
    # Repeated strings compress below the socket buffers and cannot maintain
    # paused-reader pressure. Independent hashes retain >60 MiB of wire data.
    columns = ",".join(f"MD5(CONCAT(generate_series, ':{index}'))" for index in range(128))
    query = config.get("compressed_pipeline_query", "SELECT generate_series," + columns +
                       " FROM TABLE(generate_series(1,32768)) ORDER BY generate_series")
    cursor = suite.direct.cursor()
    cursor.execute(query)
    expected = fingerprint(cursor)
    assert expected[1] >= config.get("minimum_result_bytes", 64 << 20), expected
    cursor.close()
    connection = mysql.connector.connect(**config["proxy"], autocommit=True,
                                         use_pure=True, compress=True, ssl_disabled=True,
                                         read_timeout=120, write_timeout=120)
    try:
        assert connection._socket._netbroker.__class__.__name__ == "NetworkBrokerCompressed"
        connection._socket.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        cursor = connection.cursor()
        cursor.execute(query)
        print("# compressed first result metadata received", flush=True)
        # Reset input framing for the next command while old output framing is active.
        connection._socket.send(b"\x03SELECT 999", packet_number=0, compressed_packet_number=0)
        wait_spill()
        print("# compressed first backend released with unread spool", flush=True)
        print("# compressed paused spill " + json.dumps(suite.spill_fds()), flush=True)
        assert fingerprint(cursor) == expected, "compressed first response changed"
        print("# compressed ordered oracle " + json.dumps(expected), flush=True)
        print("# compressed first result hash complete", flush=True)
        result = connection._handle_result(connection._socket.recv())
        cursor._handle_result(result)
        assert cursor.fetchall() == [(999,)], "compressed second response changed or interleaved"
        cursor.close()
    finally:
        connection.close()
        suite.cleanup()


def quit_unread():
    connection = connect(config["proxy"], stream=True)
    cursor = connection.cursor()
    connection._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    try:
        cursor.execute(config["result_query"])
        wait_spill()
        connection._sock.sendall(b"\x01\x00\x00\x00\x01")
        connection._sock.settimeout(15)
        discarded = 0
        try:
            while True:
                data = connection._sock.recv(1 << 16)
                if not data: break
                discarded += len(data)
        except ConnectionResetError:
            pass
        assert discarded < config.get("minimum_result_bytes", 64 << 20), "COM_QUIT waited for entire response"
        # Client is still open here; cleanup must have been driven by COM_QUIT.
        suite.cleanup()
        print(f"# COM_QUIT closed server socket after {discarded} queued bytes", flush=True)
    finally:
        if cursor._result: cursor._result.unbuffered_active = False
        cursor.connection = None
        connection._force_close()


failures = 0
print("TAP version 13\n1..2", flush=True)
for number, (name, test) in enumerate([("compressed raw pipeline preserves both results", compressed_pipeline),
                                      ("COM_QUIT closes socket and removes unread spool", quit_unread)], 1):
    try:
        test()
        print(f"ok {number} - {name}", flush=True)
    except Exception:
        failures += 1
        for line in traceback.format_exc().splitlines(): print("# " + line, flush=True)
        print(f"not ok {number} - {name}", flush=True)
sys.exit(bool(failures))
