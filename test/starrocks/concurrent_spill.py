#!/usr/bin/env python3
"""Eight real clients: independent spools, hashes, cancellation and FD reuse."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time
import traceback
from e2e import Suite, connect, eventually, fingerprint


def inventory(suite):
    files = {}
    for filename in suite.spill_fds():
        try:
            target = os.readlink(filename)
            stat = Path(filename).stat()
            # ext4 can recycle an unlinked inode immediately after cancellation.
            # The unique mkstemp name distinguishes different spool lifetimes.
            if os.readlink(filename) != target:
                continue
            inode = f"{stat.st_dev}:{stat.st_ino}"
            files[f"{inode}:{target}"] = {"fd": filename, "bytes": stat.st_size,
                                         "inode": inode, "target": target}
        except FileNotFoundError:
            pass
    return files


def descriptor_targets(descriptors):
    """Observe reuse for any resource occupying a retired spool descriptor."""
    targets = {}
    for descriptor, previous_target in descriptors.items():
        try:
            target = os.readlink(descriptor)
            if target != previous_target:
                targets[descriptor] = target
        except FileNotFoundError:
            pass
    return targets


def close_unread(connection, cursor):
    if connection._sock:
        try: connection._sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
    connection._force_close()
    if cursor._result: cursor._result.unbuffered_active = False
    cursor.connection = None


def query_for(config, marker):
    query = config["result_query"]
    assert query.startswith("SELECT ") and "CONCAT(LPAD" in query, "provide the generated fixture query"
    query = query.replace("SELECT ", f"SELECT '{marker}', ", 1)
    return query.replace("CONCAT(LPAD", f"CONCAT('{marker}:', LPAD", 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    assert args.clients >= 8, "acceptance requires at least eight concurrent clients"
    config = json.loads(args.config.read_text())
    suite = Suite(config)
    baseline = inventory(suite)
    report = {"clients": args.clients, "phases": []}
    failures = 0
    print("TAP version 13\n1..2", flush=True)
    with ThreadPoolExecutor(max_workers=args.clients * 2) as workers:
        for phase in range(2):
            record = {"name": "healthy" if phase == 0 else "cancel-and-replace"}
            report["phases"].append(record)
            clients = []
            sampler = None
            stop_sampling = threading.Event()
            try:
                expected = {}
                # Oracle fingerprints contain each seed in every payload, not only a header.
                for index in range(args.clients + (3 if phase else 0)):
                    marker = f"pxsr-{phase}-{index:02d}"
                    cursor = suite.direct.cursor()
                    cursor.execute(query_for(config, marker))
                    expected[marker] = fingerprint(cursor)
                    cursor.close()
                before = suite.rss()
                peak = [before]
                def sample_rss():
                    while not stop_sampling.wait(0.02):
                        peak[0] = max(peak[0], suite.rss())
                sampler = threading.Thread(target=sample_rss, daemon=True)
                sampler.start()
                def start(index):
                    marker = f"pxsr-{phase}-{index:02d}"
                    connection = connect(config["proxy"], stream=True)
                    connection._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
                    cursor = connection.cursor()
                    cursor.execute(query_for(config, marker))
                    return connection, cursor, marker
                clients = list(workers.map(start, range(args.clients)))
                evidence = {}
                def paused_complete():
                    nonlocal evidence
                    files = {inode: data for inode, data in inventory(suite).items()
                             if inode not in baseline and data["bytes"] > 0}
                    if len(files) >= args.clients:
                        evidence = files
                    return len(files) >= args.clients and suite.used() == 0
                eventually(paused_complete, config.get("timeout", 120))
                assert len(evidence) >= args.clients, "clients shared a spool inode"
                assert len({data["inode"] for data in evidence.values()}) == len(evidence), "simultaneous clients shared a spool inode"
                assert peak[0] - before <= config.get("maximum_aggregate_rss_growth_bytes", 128 << 20), (before, peak[0])
                record.update(rss_before=before, rss_peak=peak[0], paused_spill_inodes=evidence)
                print("# concurrent paused evidence " + json.dumps(record), flush=True)
                if phase:
                    # Cancel three unread responses, then create new responses while others
                    # are draining. Sockets, wake pipes or new spool files can reuse
                    # the canceled descriptor numbers; all exercise lifetime safety.
                    for connection, cursor, marker in clients[:3]:
                        close_unread(connection, cursor)
                    clients = clients[3:]
                    eventually(lambda: len(inventory(suite)) <= args.clients - 3)
                    remaining = inventory(suite)
                    retired = {data["fd"]: data["target"] for inode, data in evidence.items() if inode not in remaining}
                    record["retired_spill_fds"] = sorted(retired)
                def consume(client):
                    connection, cursor, marker = client
                    connection._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
                    def rows():
                        for count, row in enumerate(cursor):
                            assert row[0] == marker.encode(), (marker, "foreign row marker", row[0])
                            if int(marker[-2:]) % 2 and count % 128 == 0:
                                time.sleep(0.002)
                            yield row
                    try:
                        actual = fingerprint(rows())
                        assert actual == expected[marker], (marker, actual, expected[marker])
                        return marker, actual
                    finally:
                        close_unread(connection, cursor)
                pending = [workers.submit(consume, client) for client in clients]
                if phase:
                    replacement = list(workers.map(start, range(args.clients, args.clients + 3)))
                    clients.extend(replacement)
                    eventually(lambda: sum(inode not in evidence and data["bytes"] > 0
                                           for inode, data in inventory(suite).items()) >= 3,
                               config.get("timeout", 120))
                    replacement_files = inventory(suite)
                    new_files = {inode: data for inode, data in replacement_files.items() if inode not in evidence}
                    reused = {}
                    def observe_reuse():
                        reused.update(descriptor_targets(retired))
                        return bool(reused)
                    eventually(observe_reuse, config.get("timeout", 120))
                    record["replacement_spill_inodes"] = new_files
                    record["reused_spill_fds"] = sorted(reused)
                    record["retired_descriptor_replacements"] = reused
                    assert reused, "replacement phase did not reuse any retired descriptor"
                    pending.extend(workers.submit(consume, client) for client in replacement)
                record["results"] = dict(future.result() for future in pending)
                suite.cleanup()
                eventually(lambda: suite.used() == 0)
                assert inventory(suite) == baseline
                record["rss_peak"] = peak[0]
                assert peak[0] - before <= config.get("maximum_aggregate_rss_growth_bytes", 128 << 20), (before, peak[0])
                print(f"ok {phase + 1} - {record['name']} exact per-client isolation and cleanup", flush=True)
            except Exception:
                failures += 1
                for line in traceback.format_exc().splitlines(): print("# " + line, flush=True)
                print(f"not ok {phase + 1} - {record['name']} concurrent isolation", flush=True)
            finally:
                stop_sampling.set()
                if sampler is not None: sampler.join()
                for connection, cursor, marker in clients:
                    close_unread(connection, cursor)
                suite.cleanup()
    report["failures"] = failures
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
