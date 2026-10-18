#!/usr/bin/env python3
"""Create/start/stop only this benchmark's Docker resources in a dedicated VM.

Prepare three 5 GiB ext4 mounts in the VM first; see docs/local-30-cell-benchmark.md.
The load generator and object store are outside each database node's limits.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.request

from control import Control

CONTEXT = "colima-bdbperf30"
RUNTIME = "rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e"
STORE = "ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858"
VM_ROOT = "/mnt/lima-colima-bdbperf30/beyonddb-perf30"


def docker(*args):
    return subprocess.check_output(["docker", "--context", CONTEXT, *args], text=True).strip()


def write_private(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "store", "node", "stop"])
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--node", type=int, choices=[1, 2, 3])
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    root = args.run_dir.resolve()
    if args.action == "init":
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        credentials = {"access_key": "AKIAPERF30LOCAL0001", "secret": secrets.token_urlsafe(40),
                       "store_secret": secrets.token_urlsafe(40)}
        write_private(root / "credentials.json", json.dumps(credentials).encode())
        write_private(root / "bootstrap.secret", credentials["secret"].encode())
        write_private(root / "peer.key", secrets.token_bytes(32))
        write_private(root / "encryption.key", secrets.token_bytes(32))
        write_private(root / "store.env", ("RUSTFS_ACCESS_KEY=perf30store\nRUSTFS_SECRET_KEY="
                      + credentials["store_secret"] + "\nRUSTFS_CONSOLE_ENABLE=false\n"
                      "RUSTFS_OBS_LOG_DIRECTORY=/data/logs\n").encode())
        write_private(root / "node.env", ("AWS_ACCESS_KEY_ID=perf30store\nAWS_SECRET_ACCESS_KEY="
                      + credentials["store_secret"] + "\nAWS_REGION=us-east-1\n"
                      "AWS_ALLOW_HTTP=true\nAWS_VIRTUAL_HOSTED_STYLE_REQUEST=false\n").encode())
        write_private(root / "policy.json", json.dumps({"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]}).encode())
        for i in range(1, 4):
            config = {"node_name": f"perf30-node{i}", "data_dir": "/data", "region": "us-east-1",
                      "public_bind": f"127.0.0.1:{18000+i}",
                      "public_endpoint": f"http://127.0.0.1:{18000+i}",
                      "peer_bind": f"127.0.0.1:{19000+i}", "peer_advertise": f"127.0.0.1:{19000+i}",
                      "peer_key_file": "/config/peer.key", "encryption_key_file": "/config/encryption.key",
                      "accounts": ["123456789012"], "initial_partitions": 30,
                      "max_active_cells": 2048, "split_threshold_bytes": 1073741824,
                      "auth_cache_enabled": False, "shutdown_mode": "preserve",
                      "fleet": {"bucket": "beyonddb-perf30", "endpoint": "http://127.0.0.1:19100",
                                "prefix": "ordered-v1", "follower_durability": False},
                      "bootstrap": {"account_id": "123456789012", "access_key_id": credentials["access_key"],
                                    "principal_name": "perf", "policy_name": "benchmark",
                                    "policy_file": "/config/policy.json"}}
            write_private(root / f"node{i}.json", json.dumps(config, indent=2).encode())
        print("Created private configuration and credentials.")
    elif args.action == "store":
        print(docker("run", "--detach", "--name", "beyonddb-perf30-store", "--label", "beyonddb.perf=30cells",
                     "--network", "host", "--cpus", "1", "--memory", "4g", "--memory-swap", "4g",
                     "--env-file", str(root / "store.env"), "--volume", "beyonddb-perf30-store:/data",
                     STORE, "/data", "--address", "127.0.0.1:19100"))
        control = Control(root)
        deadline = time.monotonic() + 45
        while True:
            try:
                try:
                    control.s3.head_bucket(Bucket="beyonddb-perf30")
                except Exception:
                    control.s3.create_bucket(Bucket="beyonddb-perf30")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        print("Local object store and fixture bucket ready.")
    elif args.action == "node":
        if not args.node or not args.binary:
            parser.error("node requires --node and --binary")
        i = args.node
        command = f"exec /opt/beyonddb /config/node{i}.json"
        if args.bootstrap:
            command += " --bootstrap < /config/bootstrap.secret"
        print(docker("run", "--detach", "--name", f"beyonddb-perf30-node{i}", "--label", "beyonddb.perf=30cells",
                     "--network", "host", "--cpus", "1", "--memory", "3g", "--memory-swap", "3g",
                     "--ulimit", "nofile=65536:65536", "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m", "--cap-drop", "ALL",
                     "--security-opt", "no-new-privileges", "--pids-limit", "512",
                     "--env-file", str(root / "node.env"),
                     "--mount", f"type=bind,src={root},dst=/config,readonly",
                     "--mount", f"type=bind,src={args.binary.resolve()},dst=/opt/beyonddb,readonly",
                     "--mount", f"type=bind,src={VM_ROOT}/node{i},dst=/data",
                     RUNTIME, "sh", "-c", command))
        deadline = time.monotonic() + 120
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{18000+i}/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
            state = json.loads(docker("inspect", "--format", "{{json .State}}", f"beyonddb-perf30-node{i}"))
            if not state["Running"]:
                raise RuntimeError(f"node {i} exited; inspect its retained Docker log")
            time.sleep(1)
        print(f"Node {i} ready.")
    else:
        print(docker("stop", "--timeout", "100", *[f"beyonddb-perf30-node{i}" for i in range(1, 4)]))
        print(docker("stop", "--timeout", "30", "beyonddb-perf30-store"))
        stopped = json.loads(docker("inspect", *[f"beyonddb-perf30-node{i}" for i in range(1, 4)], "beyonddb-perf30-store"))
        if any(p["State"]["Running"] or p["State"]["ExitCode"] != 0 or p["State"]["OOMKilled"] for p in stopped):
            raise RuntimeError("benchmark stop was incomplete; inspect retained process logs")


if __name__ == "__main__":
    main()
