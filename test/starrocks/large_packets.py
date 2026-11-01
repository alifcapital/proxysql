#!/usr/bin/env python3
"""Verify a MySQL row crossing the 0xffffff packet boundary through disk spill.

Uses session-local driver setup only; never updates ProxySQL or FE globals.
The direct plain FE is the oracle for plain, compressed, prepared, and TLS
ProxySQL connections. A background sampler records a nonempty, immediately
unlinked production spool file throughout each response.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import time
import threading
import traceback
import types
import zlib

import mysql.connector

COLUMN_BYTES = 1024 * 1024
COLUMN_COUNT = 20
QUERY = "SELECT " + ", ".join(
    f"REPEAT('{chr(65 + column)}', {COLUMN_BYTES}) AS c{column:02d}"
    for column in range(COLUMN_COUNT)
)


def complete_packet_reader(connection, variant):
    """Repair connector pure-reader continuation limits without changing wire bytes.

    Its compressed broker consumes at most one extra compressed frame per classic
    packet; its binary reader consumes only one classic packet per row. Neither
    handles this fixture. Keep the real connector authentication and parsing, but
    assemble complete packets, checking every continuation sequence number.
    """
    transport = connection._socket
    if variant == "compressed":
        broker = transport._netbroker

        def receive_frames(self, sock, compressed_pll, uncompressed_pll):
            pending = bytearray()
            while True:
                frame = self._recv_chunk(sock, size=compressed_pll)
                decoded = zlib.decompress(frame) if uncompressed_pll else frame
                assert not uncompressed_pll or len(decoded) == uncompressed_pll
                pending.extend(decoded)
                while len(pending) >= 4:
                    length = int.from_bytes(pending[:3], "little")
                    if len(pending) < length + 4:
                        break
                    self._queue_read.append(pending[:length + 4])
                    del pending[:length + 4]
                if not pending:
                    return
                header = self._recv_chunk(sock, size=7)
                next_sequence = header[3]
                assert next_sequence == (self._compressed_pktnr + 1) % 256
                self._compressed_pktnr = next_sequence
                compressed_pll = int.from_bytes(header[:3], "little")
                uncompressed_pll = int.from_bytes(header[4:7], "little")

        broker._recv_compressed_pkt = types.MethodType(receive_frames, broker)
    if variant == "prepared":
        original_recv = transport.recv

        def receive_logical(self, *args, **kwargs):
            packet = bytearray(original_recv(*args, **kwargs))
            previous = packet[3]
            length = int.from_bytes(packet[:3], "little")
            while length == 0xFFFFFF:
                continuation = original_recv(*args, **kwargs)
                assert continuation[3] == (previous + 1) % 256
                previous = continuation[3]
                length = int.from_bytes(continuation[:3], "little")
                assert len(continuation) == length + 4
                packet.extend(continuation[4:])
            return packet

        transport.recv = types.MethodType(receive_logical, transport)


def eventually(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("timed out waiting for production spool evidence/cleanup")


def spool_files(pid, marker):
    found = {}
    directory = Path(f"/proc/{pid}/fd")
    for entry in directory.iterdir():
        try:
            target = os.readlink(entry)
            if marker in target:
                found[str(entry)] = {"target": target, "bytes": entry.stat().st_size}
        except (FileNotFoundError, PermissionError):
            continue
    return found


def identity(files):
    return {(descriptor, value["target"]) for descriptor, value in files.items()}


def result_fingerprint(cursor):
    rows = 0
    byte_count = 0
    digest = hashlib.sha256()
    for row in cursor:
        rows += 1
        assert len(row) == COLUMN_COUNT, f"unexpected column count: {len(row)}"
        for column, value in enumerate(row):
            assert value is not None, f"FE returned NULL for column {column}"
            data = value.encode("utf-8") if isinstance(value, str) else bytes(value)
            assert len(data) == COLUMN_BYTES, (column, len(data), COLUMN_BYTES)
            expected = bytes([65 + column]) * COLUMN_BYTES
            assert data == expected, f"column {column} content changed"
            digest.update(struct.pack("<Q", len(data)))
            digest.update(data)
            byte_count += len(data)
    assert rows == 1, f"unexpected row count: {rows}"
    assert byte_count == COLUMN_COUNT * COLUMN_BYTES
    assert byte_count > 0xFFFFFF, "fixture did not cross the MySQL packet boundary"
    return {"rows": rows, "bytes": byte_count, "sha256": digest.hexdigest()}


def connect(endpoint, variant):
    options = dict(endpoint)
    options.update(autocommit=True, connection_timeout=15, read_timeout=120, use_pure=True,
                   ssl_disabled=variant != "tls")
    if variant == "compressed":
        options["compress"] = True
    if variant == "tls":
        options.update(ssl_verify_cert=False, ssl_verify_identity=False)
    connection = mysql.connector.connect(**options)
    if variant == "compressed":
        assert connection._socket._netbroker.__class__.__name__ == "NetworkBrokerCompressed"
    if variant == "tls":
        assert isinstance(connection._socket.sock, ssl.SSLSocket), "TLS was not negotiated"
    complete_packet_reader(connection, variant)
    return connection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--pid", type=int, required=True, help="running ProxySQL PID")
    parser.add_argument("--proxy-port", type=int, default=16043)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    proxy = dict(config["proxy"])
    proxy["port"] = args.proxy_port
    marker = config.get("spill_file_marker", "proxysql-result-")
    baseline = identity(spool_files(args.pid, marker))
    print("1..5", flush=True)
    source = Path(__file__).resolve()
    repository = source.parents[2]
    tracked_path = str(source.relative_to(repository))
    subprocess.run(["git", "-C", str(repository), "ls-files", "--error-unmatch", tracked_path],
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    source_commit = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "HEAD"], text=True).strip()
    print("# source " + json.dumps({"commit": source_commit, "path": str(source),
                                   "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                                   "runtime_pid": args.pid,
                                   "runtime_executable": os.readlink(f"/proc/{args.pid}/exe")}, sort_keys=True), flush=True)
    failures = 0
    oracle = None
    for test_number, variant in enumerate(("oracle", "plain", "compressed", "prepared", "tls"), 1):
        connection = None
        cursor = None
        sampler = None
        stop_sampling = threading.Event()
        observations = {}
        sampling_errors = []
        try:
            connection = connect(config["backend"] if variant == "oracle" else proxy, variant)
            if variant != "oracle":
                connection._socket.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
                def sample():
                    try:
                        while not stop_sampling.is_set():
                            for fd, value in spool_files(args.pid, marker).items():
                                if (fd, value["target"]) not in baseline and value["bytes"] > 0:
                                    observations[(fd, value["target"])] = value
                            stop_sampling.wait(0.001)
                    except Exception as error:
                        sampling_errors.append(str(error))
                sampler = threading.Thread(target=sample, daemon=True)
                sampler.start()
            cursor = connection.cursor(prepared=variant == "prepared")
            cursor.execute(QUERY)
            actual = result_fingerprint(cursor)
            stop_sampling.set()
            if sampler is not None:
                sampler.join()
                assert not sampling_errors, sampling_errors
                assert observations, f"{variant} response did not exercise the production disk spool"
                print("# " + variant + " disk evidence " + json.dumps(list(observations.values()), sort_keys=True), flush=True)
            if variant == "oracle":
                oracle = actual
            else:
                assert actual == oracle, (variant, actual, oracle)
            cursor.close()
            cursor = None
            connection.close()
            connection = None
            eventually(lambda: identity(spool_files(args.pid, marker)) == baseline)
            print(f"ok {test_number} - {variant} exact 20 MiB row across MySQL packet boundary", flush=True)
            print("# " + json.dumps(actual, sort_keys=True), flush=True)
        except Exception:
            failures += 1
            print(f"not ok {test_number} - {variant} exact 20 MiB row across MySQL packet boundary", flush=True)
            for line in traceback.format_exc().splitlines():
                print("# " + line, flush=True)
        finally:
            stop_sampling.set()
            if sampler is not None:
                sampler.join()
            if connection is not None:
                connection.close()
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
